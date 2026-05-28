# build-stg-msisdn-imei

**Витрина:** `stg_msisdn_imei` · **Команда:** `build-stg-msisdn-imei` · **Режим:** интервалы актуальности MSISDN↔IMEI за отчётный день (один Parquet на дату).

Референс: [`pipelines/stg/msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py). Схема витрины: [`msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_geo_all` за `report_date` | DataFrame событий |
| 2 | Нормализовать MSISDN и IMEI, отфильтровать валидные пары | События с `msisdn`, `imei`, `event_ts` |
| 3 | Построить интервалы по смене IMEI на MSISDN | `valid_from` / `valid_to` |
| 4 | Записать витрину в Parquet | Файл `output_path` |

**Бизнес-назначение:** для любого момента внутри суток знать, какое устройство (IMEI) наблюдалось у MSISDN по фактическим событиям.

**В scope задач:** чтение `stg_geo_all`, нормализация идентификаторов, сегментация событий в интервалы, запись Parquet и публикация таймингов.

---

## TODO

1. DQ-витрина `dq-stg-msisdn-imei`.
2. Сквозная цепочка `run-all`.

---

## Параметры запуска

Переменные, передаваемые в job (аргументы `run_build()`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да* | — | Отчётный день |
| `stg_geo_all_path` | string (path) | Нет | `data/stg/geo_all/{report_date}.parquet` | Входной `stg_geo_all` parquet или каталог `data/stg/geo_all` |
| `output_path` | string (path) | Нет | `data/stg/msisdn_imei/{report_date}.parquet` | Выходной Parquet (перезапись) |

\* Без `--report-date` — цикл дней из [`cli_defaults.py`](../../src/mobile/cli_defaults.py).

**Предусловие:** `build-stg-geo-all` за ту же `report_date`.

```bash
uv run mobile build-stg-msisdn-imei --report-date 2025-01-01
uv run mobile build-stg-msisdn-imei --report-date 2025-01-01 \
  --stg-geo-all-path data/stg/geo_all \
  --output-path data/stg/msisdn_imei/2025-01-01.parquet
```

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_msisdn_imei` — [`msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json) |
| Формат | Parquet, один файл на `report_date` |
| Сжатие | `snappy` |

### Поля витрины

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | MSISDN, E.164 |
| 2 | `imei` | string | IMEI, 14–16 цифр |
| 3 | `valid_from` | timestamp | Начало интервала |
| 4 | `valid_to` | timestamp | Конец интервала |

### Нормализация

| Поле | Правила ([`subscriber_ids.py`](../../src/mobile/pipelines/stg/subscriber_ids.py)) |
|------|--------|
| `msisdn` | Только цифры; RU 10→`7…`, `8XXXXXXXXXX`→`7…`; иностранные 7–15 цифр |
| `imei` | 14–16 цифр |

Время интервалов — UTC из `stg_geo_all.start_time_utc`.

---

## Источники витрины

| Атрибут | Значение |
|---------|----------|
| Слой | `stg_geo_all` после [`build-stg-geo-all`](./build_stg_geo_all.md) |
| Путь | `data/stg/geo_all/{YYYY-MM-DD}.parquet` (или каталог через `stg_geo_all_path`) |
| Чтение | `pd.read_parquet(...)` c колонками `msisdn`, `imei`, `start_time_utc` |
| Временная ось | `start_time_utc` (UTC) |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Определить `output_path`:
   - явный `--output-path`, либо
   - `stg_msisdn_imei_output_path(report_date)`.
2. Определить вход:
   - если `stg_geo_all_path` не задан — `stg_geo_all_output_path(report_date)`,
   - если передан каталог — файл `{report_date}.parquet`,
   - если передан файл — использовать как есть.
3. Загрузить схему контракта (`STG_MSISDN_IMEI_FIELDS`) из JSON при импорте модуля.

### Шаг 1. Чтение источника

1. Прочитать `stg_geo_all` с минимальным набором полей (`msisdn`, `imei`, `start_time_utc`).
2. При отсутствии файла или ошибке чтения:
   - записать warning/error в лог,
   - продолжить с пустым DataFrame (pipeline не падает на этапе read).

### Шаг 2. Подготовка событий

1. `event_ts` ← `start_time_utc` (`pd.to_datetime(..., errors=\"coerce\")`).
2. Нормализовать `msisdn` через `normalize_msisdn`.
3. Нормализовать `imei` через `normalize_imei`.
4. Отфильтровать строки, где любой из обязательных атрибутов пустой:
   - `msisdn`,
   - `imei`,
   - `event_ts`.
5. Выход шага: канонический набор событий `msisdn + imei + event_ts`.

### Шаг 3. Построение интервалов

1. Отсортировать события по `msisdn`, `event_ts`.
2. Для каждого `msisdn` пройти последовательность:
   - первое значение `imei` открывает сегмент,
   - при смене `imei` закрыть предыдущий сегмент (`valid_from`, `valid_to`) и открыть новый,
   - при неизменном `imei` обновить `valid_to`.
3. После конца группы закрыть последний открытый сегмент.
4. Ограничить интервалы границами суток `report_date`:
   - `valid_from >= day_start`,
   - `valid_to <= day_end`.
5. Удалить интервалы с нарушенной временной логикой (`valid_from > valid_to`).

### Шаг 4. Приведение к контракту и запись

1. Повторно привести поля к контрактным типам (`msisdn`, `imei`, `valid_from`, `valid_to`).
2. Удалить строки с null в контрактных колонках.
3. Записать parquet со сжатием `snappy`.
4. Сохранить метрики в `command_timing.jsonl`:
   - `geo_rows_read`,
   - `event_rows_with_pair`,
   - `interval_rows`,
   - `distinct_msisdn`,
   - тайминги шагов.

### Типовые ошибки

| Ошибка/ситуация | Поведение |
|-----------------|-----------|
| Нет `stg_geo_all` за дату | Пустой результат (warning в логах) |
| Все события отброшены нормализацией | Пустая витрина за день |
| `--report-date` не задан в worker-режиме | CLI завершится с `SystemExit` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`src/mobile/schema/stg/msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json) |
| ETL | [`src/mobile/pipelines/stg/msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py) |
| MSISDN–IMSI | [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md) |
| stg_geo_all | [`build_stg_geo_all.md`](./build_stg_geo_all.md) |
