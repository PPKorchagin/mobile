# dq-stg-geo-intervals

**Витрина:** `stg_geo_intervals` · **Команда:** `dq-stg-geo-intervals` · **Режим:** read-only DQ (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/geo_intervals.py`](../../../src/mobile/pipelines/dq/stg/geo_intervals.py). Сборка витрины: [`build_stg_geo_intervals.md`](../../stg/build_stg_geo_intervals.md). Схема: [`geo_intervals.json`](../../../src/mobile/schema/stg/geo_intervals.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти parquet `stg_geo_intervals` за `report_date` | Путь к дневному срезу |
| 2 | Проверить контракт колонок и профили null/cardinality | Логи `DQ_STG_GEO_INTERVALS` |
| 3 | Проверить временные, географические и ключевые ограничения | Gate-статусы |
| 4 | Выдать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества интервального геослоя перед downstream-анализом треков и построением поведенческих признаков.

**В scope:** проверка наличия набора, контракта, временных интервалов, координат, словаря `bs_type`, `cgi_list` и ключевых дубликатов.

---

## TODO

1. Добавить baseline-пороги (warning/failed) для долей `nulls.timezone` и кардинальности `cgi_list`.
2. Расширить профиль распределениями длительности интервала (`end_time_utc - start_time_utc`) и числа CGI.

---

## Параметры запуска

Вызов: `run_dq(report_date, stg_geo_intervals_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-stg-geo-intervals`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | Отчётный день |
| `stg_geo_intervals_path` | path | Нет | `data/stg/geo_intervals/{report_date}.parquet` | Входной parquet или каталог `data/stg/geo_intervals` |

CLI:

```bash
uv run mobile dq-stg-geo-intervals --report-date 2025-01-01
uv run mobile dq-stg-geo-intervals --report-date 2025-01-01 --stg-geo-intervals-path data/stg/geo_intervals
uv run mobile dq-stg-geo-intervals --report-date 2025-01-01 --stg-geo-intervals-path data/stg/geo_intervals/2025-01-01.parquet
```

Логи: `data/logs/mobile.log` (тег `DQ_STG_GEO_INTERVALS`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-stg-geo-intervals`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Путь по умолчанию | `data/stg/geo_intervals/{YYYY-MM-DD}.parquet` |
| Формат | Parquet |
| Контракт полей | [`geo_intervals.json`](../../../src/mobile/schema/stg/geo_intervals.json) |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `stg_geo_intervals` parquet | `data/stg/geo_intervals/{YYYY-MM-DD}.parquet` | Вход для DQ профиля |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Определить путь к parquet (`stg_geo_intervals_output_path(report_date)` или `--stg-geo-intervals-path`).
2. Если передан каталог, взять файл `{report_date}.parquet`.
3. Инициализировать счётчики `total/warning/failed` и правила gate-статусов.

### Шаг 1. Наличие набора

1. Проверить существование файла.
2. Если файл отсутствует:
   - записать `dataset_presence=failed`,
   - сформировать `summary`,
   - вернуть статус без дальнейших шагов.

### Шаг 2. Базовый профиль

1. `dataset_basic`: число строк/колонок, путь.
2. `schema_columns`: проверка контрактных полей.
3. Для каждого поля: `nulls.<field>` и `cardinality.<field>`.
4. На этом шаге формируется базовый профиль заполненности витрины.

### Шаг 3. Gate-проверки

1. **`required_fields_presence`:** `msisdn`, `start_time_utc`, `end_time_utc`, `sub_lat`, `sub_lon`, `bs_type` — null_ratio = 0 для ключевых; иначе **failed**.
2. **`temporal_order`:** `end_time_utc >= start_time_utc`; `inverted_count` → **failed**.
3. **`coords_range`:** широта/долгота подписчика в физических пределах; NaN только для пустых интервалов без веса.
4. **`bs_type_vocab`:** `bs_type ∈ {i,o,…}` по контракту [`geo_intervals.json`](../../../src/mobile/schema/stg/geo_intervals.json).
5. **`timezone_range`:** `timezone` (UTC offset hours) ∈ [-12, 14]; выбросы → **warning**.
6. **`cgi_list_non_empty`:** `cgi_list` не пустая строка / список; пустые → **failed** по доле.
7. **`distribution.cgi_list_len`:** гистограмма числа CGI в интервале (info); аномально длинные списки → **warning**.
8. **`duplicate_interval_key`:** дубликаты по `(imsi, imei, msisdn, start_time_utc, end_time_utc, bs_type)` → **warning**/**failed**.
9. **`oktmo_dominance` (если есть):** согласованность `oktmo_code_1/2` с весами событий.

### Шаг 4. Итог

1. Сформировать `summary` с `total_checks`, `warning_checks`, `failed_checks`.
2. Вернуть общий статус:
   - `failed` при наличии failed-checks,
   - `warning` при отсутствии failed и наличии warning,
   - `ok` при полном прохождении checks.

### Типовые ошибки

| Ошибка/ситуация | Поведение |
|-----------------|-----------|
| Нет parquet за `report_date` | `dataset_presence=failed` |
| Неполный контракт колонок | `schema_columns=failed` |
| Пустые `cgi_list` | `cgi_list_non_empty=failed` |

---

## Проверки

| Check | Уровень | Смысл |
|-------|---------|-------|
| `dataset_presence` | failed | parquet не найден |
| `dataset_basic` | info | базовые размеры |
| `schema_columns` | gate | полнота контракта колонок |
| `nulls.*` | info | доли null по полям |
| `cardinality.*` | info | кардинальность полей |
| `required_fields_presence` | gate | обязательность ключевых полей |
| `temporal_order` | gate | корректность интервалов времени |
| `coords_range` | gate | диапазоны координат |
| `bs_type_vocab` | gate | словарь `bs_type` |
| `timezone_range` | gate | контроль timezone |
| `cgi_list_non_empty` | gate | непустой список CGI |
| `distribution.cgi_list_len` | info | распределение длины `cgi_list` |
| `duplicate_interval_key` | gate | дубликаты ключа интервала |
| `summary` | info | итоговый статус и счетчики |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ ETL | [`src/mobile/pipelines/dq/stg/geo_intervals.py`](../../../src/mobile/pipelines/dq/stg/geo_intervals.py) |
| Сборка `stg_geo_intervals` | [`documents/stg/build_stg_geo_intervals.md`](../../stg/build_stg_geo_intervals.md) |
| CLI | [`src/mobile/cli.py`](../../../src/mobile/cli.py) |
| Схема | [`src/mobile/schema/stg/geo_intervals.json`](../../../src/mobile/schema/stg/geo_intervals.json) |
