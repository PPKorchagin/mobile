# build-stg-msisdn-imsi

**Витрина:** `stg_msisdn_imsi` · **Команда:** `build-stg-msisdn-imsi` · **Режим:** интервалы актуальности MSISDN↔IMSI за отчётный день (один Parquet на дату).

Референс: [`pipelines/stg/msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py). Схема витрины: [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `event_dds` за `report_date` (все ЦОД) | DataFrame событий |
| 2 | Нормализовать MSISDN и IMSI, отфильтровать валидные пары | События с `msisdn`, `imsi`, `event_ts` |
| 3 | Построить интервалы по смене IMSI на MSISDN | `valid_from` / `valid_to` |
| 4 | Записать витрину в Parquet | Файл `output_path` |

**Бизнес-назначение:** для любого момента внутри суток знать, какой IMSI был привязан к MSISDN по фактическим событиям (без синтетических разрывов).

**В scope задач:** чтение DDS, нормализация идентификаторов, построение интервалов, запись Parquet. Объединение с историей за другие дни **не входит** — один файл на `report_date`.

---

## TODO

1. DQ-витрина `dq-stg-msisdn-imsi` (схема, покрытие, пересечения интервалов).
2. Включить в сквозную цепочку `run-all`, если появится оркестратор STG.

---

## Параметры запуска

Переменные, передаваемые в job (аргументы `run_build()`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да* | — | Отчётный день (сутки по локальному `event_timestamp`) |
| `event_dds_path` | string (path) | Нет | `data/stg/event_dds` | Корень DDS, каталог `YYYY-MM-DD` или один `.parquet` |
| `output_path` | string (path) | Нет | `data/stg/msisdn_imsi/{report_date}.parquet` | Выходной Parquet (перезапись) |

\* Без `--report-date` в CLI — цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)); на каждый день отдельный timed-run и свой `output_path` по шаблону.

Пути **относительные к корню репозитория** `mobile`, если не заданы абсолютные (`resolve_project_path` в [`project_paths.py`](../../src/mobile/project_paths.py)).

**Константы ETL в коде** ([`msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `STG_MSISDN_IMSI_TABLE` | `stg_msisdn_imsi` (из [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json)) |
| `STG_MSISDN_IMSI_FIELDS` | порядок и типы колонок |
| `DEFAULT_PARQUET_COMPRESSION` | `snappy` ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)) |

**Предусловие:** `build-move-event` за ту же `report_date` (файлы в `data/stg/event_dds/{YYYY-MM-DD}/`).

Локальный запуск:

```bash
uv run mobile build-stg-msisdn-imsi --report-date 2025-01-01
uv run mobile build-stg-msisdn-imsi --report-date 2025-01-01 \
  --event-dds-path data/stg/event_dds \
  --output-path data/stg/msisdn_imsi/2025-01-01.parquet
```

Логи: `data/logs/mobile.log`. Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-msisdn-imsi` или `build-stg-msisdn-imsi-{date}`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_msisdn_imsi` — [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json) → `table` |
| Описание | Интервалы MSISDN–IMSI — `description` в JSON |
| Формат хранения | Parquet |
| Партиционирование | Один файл на `report_date` |
| Календарный срез | `report_date` (`YYYY-MM-DD` в пути по умолчанию) |
| Сжатие | `snappy` (`DEFAULT_PARQUET_COMPRESSION`) |

### Поля витрины

Контракт полей — [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json) → `fields`; в ETL — `STG_MSISDN_IMSI_FIELDS`.

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | MSISDN, E.164 (RU и иностранные 7–15 цифр) |
| 2 | `imsi` | string | IMSI, 14–15 цифр |
| 3 | `valid_from` | timestamp | Первое событие интервала (локальное время) |
| 4 | `valid_to` | timestamp | Последнее событие интервала (локальное время) |

---

## Источники витрины

| Атрибут | Значение |
|---------|----------|
| Слой | `event_dds` после [`build-move-event`](./build_move_event.md) |
| Путь | `data/stg/event_dds/{YYYY-MM-DD}/{central\|far-east}.parquet` (параметр `event_dds_path`) |
| Чтение | [`event_dds_reader.read_event_dds_for_report_date`](../../src/mobile/pipelines/stg/event_dds_reader.py) — все файлы дня |
| Колонки | `event_timestamp`, `msisdn`, `imsi` (остальные не используются) |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `output_path` — аргумент или `stg_msisdn_imsi_output_path(report_date)`.
2. Схема из JSON при импорте модуля (`_load_schema_contract`).

### Шаг 1. Чтение источника

`read_event_dds_for_report_date(report_date, event_dds_path)` → объединение Parquet всех ЦОД за день.

### Шаг 2. Подготовка событий

1. `event_ts` ← парсинг `event_timestamp` ([`parse_event_timestamps`](../../src/mobile/pipelines/stg/event_dds_reader.py)).
2. `msisdn` ← [`normalize_msisdn`](../../src/mobile/pipelines/stg/subscriber_ids.py).
3. `imsi` ← [`normalize_imsi`](../../src/mobile/pipelines/stg/subscriber_ids.py).
4. Оставить строки, где все три поля не null.

### Шаг 3. Интервалы

Для каждого `msisdn` (сортировка по `event_ts`):

1. Идти по событиям; при смене `imsi` закрыть интервал `[valid_from, valid_to]` предыдущего значения.
2. `valid_from` / `valid_to` — min/max `event_ts` в сегменте.
3. Обрезка: `valid_from` ≥ начало суток, `valid_to` ≤ конец суток `report_date`.
4. Отбросить интервалы с `valid_from > valid_to`.

### Шаг 4. Запись

`to_parquet(output_path, compression=snappy, index=False)` — перезапись; каталог создаётся при необходимости.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет файлов `event_dds` за день |
| Пустой выход | Нет валидных пар MSISDN–IMSI за день |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/stg/msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json) |
| ETL | [`src/mobile/pipelines/stg/msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py) |
| Нормализация ID | [`src/mobile/pipelines/stg/subscriber_ids.py`](../../src/mobile/pipelines/stg/subscriber_ids.py) |
| Чтение DDS | [`src/mobile/pipelines/stg/event_dds_reader.py`](../../src/mobile/pipelines/stg/event_dds_reader.py) |
| Пути по умолчанию | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| MSISDN–IMEI | [`build_stg_msisdn_imei.md`](./build_stg_msisdn_imei.md) |
| event_dds | [`build_move_event.md`](./build_move_event.md) |
