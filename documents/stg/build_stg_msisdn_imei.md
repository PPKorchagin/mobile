# build-stg-msisdn-imei

**Витрина:** `stg_msisdn_imei` · **Команда:** `build-stg-msisdn-imei` · **Режим:** месячный parquet с **ежедневным** инкрементом из `stg_geo_all`.

Референс: [`msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py), [`binding_intervals.py`](../../src/mobile/pipelines/stg/binding_intervals.py). Схема: [`msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_geo_all` за отчётный день | События MSISDN + IMEI |
| 2 | Построить суточные интервалы по смене устройства | Сегменты в границах суток |
| 3 | Обновить месячный файл | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` |

**Бизнес-назначение:** месячная привязка номера к IMEI (смена телефона при том же MSISDN) для person и geo-intervals.

Алгоритм **идентичен** [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md), колонка связи — `imei`, нормализация — [`normalize_imei`](../../src/mobile/pipelines/stg/subscriber_ids.py).

---

## Параметры запуска

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `report_date` | — | Любой день месяца |
| `stg_geo_all_path` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | Вход за день |
| `output_path` | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Месячный выход |

```bash
uv run mobile build-stg-msisdn-imei --report-date 2025-01-15
```

---

## Структура витрины

| Свойство | Значение |
|----------|----------|
| Поля | `msisdn`, `imei`, `valid_from`, `valid_to` |
| Файл | один parquet на календарный месяц (`YYYY-MM-01` в пути) |
| Сжатие | `snappy` |

---

## Алгоритм обработки данных

Точка входа: `run_build` → `_run_build` в [`msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py), `value_col="imei"`.

### Шаг 0. Инициализация

1. Границы суток `report_date`.
2. `output_path` → `stg_msisdn_imei_output_path` (месяц = 1-е число месяца `report_date`).
3. Чтение geo: колонки `msisdn`, `imei`, `start_time_utc`.

### Шаг 1. Чтение и подготовка событий

1. `read_parquet(stg_geo_all)`; при отсутствии — warning и пустой вход.
2. `event_ts` из `start_time_utc`; нормализация `msisdn` / `imei`.
3. Фильтр: все ключевые поля not null.

### Шаг 2. Суточные интервалы

Для каждого `msisdn` по возрастанию `event_ts`:

1. Сегменты по непрерывному одинаковому `imei`.
2. При смене IMEI — закрыть предыдущий интервал `[min_ts, max_ts]`.
3. Clip в `[day_start, day_end]`.
4. `_coerce_output` → `day_interval_rows`.

### Шаг 3. Инкремент в месячный файл

Через [`upsert_daily_into_month_parquet`](../../src/mobile/pipelines/stg/binding_intervals.py):

1. Удалить из month-файла строки, пересекающие календарный день `report_date`.
2. Добавить суточные интервалы.
3. `merge_binding_intervals` по `(msisdn, imei)` (склейка смежных сегментов, gap ≤ 1s).
4. Записать `data/stg/msisdn_imei/{YYYY-MM-01}.parquet`.

### Шаг 4. Потребители

- **geo-intervals:** fill пустого `imei` по MSISDN и времени события.
- **person:** рёбра `msisdn↔imei` в union-find за месяц.

### Типовые ситуации

| Ситуация | Поведение |
|----------|-----------|
| Нет geo | warning, день не меняет month-файл |
| Повтор за день | идемпотентно |
| IMEI меняется внутри дня | несколько суточных интервалов |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py) |
| IMSI (аналог) | [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md) |
| Person | [`build_stg_person.md`](./build_stg_person.md) |
