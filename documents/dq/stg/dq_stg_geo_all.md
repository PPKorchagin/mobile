# dq-stg-geo-all

**Витрина:** `stg_geo_all` · **Команда:** `dq-stg-geo-all` · **Режим:** read-only DQ (не изменяет данные, не падает при failed checks).

Референс: [`pipelines/dq/stg/geo_all.py`](../../../src/mobile/pipelines/dq/stg/geo_all.py). Сборка витрины: [`build_stg_geo_all.md`](../../stg/build_stg_geo_all.md). Схема: [`geo_all.json`](../../../src/mobile/schema/stg/geo_all.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти parquet `stg_geo_all` за `report_date` | Путь к дневному срезу |
| 2 | Проверить контракт колонок и профили null/cardinality | Логи `DQ_STG_GEO_ALL` |
| 3 | Проверить доменные и временные правила | Gate-статусы `ok/warning/failed` |
| 4 | Выдать `summary` | Счетчики checks |

**Бизнес-назначение:** контроль качества дневной гео-витрины перед downstream-использованием и построением связок MSISDN↔IMSI/IMEI.

**В scope:** проверка наличия набора, контракта, координат, времени, словарей и ключевых дубликатов.

---

## TODO

1. Добавить динамические пороги для `warning/failed` по историческим baseline.
2. Расширить распределения по `utc_offset` и `event_count` bucket-профилем.

---

## Параметры запуска

Вызов: `run_dq(report_date, stg_geo_all_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-stg-geo-all`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | Отчётный день |
| `stg_geo_all_path` | path | Нет | `data/stg/geo_all/{report_date}.parquet` | Входной parquet или каталог `data/stg/geo_all` |

CLI:

```bash
uv run mobile dq-stg-geo-all --report-date 2025-01-01
uv run mobile dq-stg-geo-all --report-date 2025-01-01 --stg-geo-all-path data/stg/geo_all
uv run mobile dq-stg-geo-all --report-date 2025-01-01 --stg-geo-all-path data/stg/geo_all/2025-01-01.parquet
```

Логи: `data/logs/mobile.log` (тег `DQ_STG_GEO_ALL`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-stg-geo-all`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Путь по умолчанию | `data/stg/geo_all/{YYYY-MM-DD}.parquet` |
| Формат | Parquet |
| Контракт полей | [`geo_all.json`](../../../src/mobile/schema/stg/geo_all.json) |
| Проверяемый слой | Дневной STG geo-срез |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `stg_geo_all` parquet | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | Вход для DQ-профиля и gate-checks |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Определить путь к parquet (`stg_geo_all_output_path(report_date)` или `--stg-geo-all-path`).
2. Если передан каталог, взять файл `{report_date}.parquet`.
3. Инициализировать счетчики `total/warning/failed` и правила gate-статусов.

### Шаг 1. Наличие набора

1. Проверить существование файла.
2. Если файл отсутствует:
   - записать `dataset_presence=failed`,
   - сформировать `summary`,
   - вернуть статус без дальнейших шагов.

### Шаг 2. Базовый профиль

1. `dataset_basic`: число строк/колонок, путь.
2. `schema_columns`: обязательные поля витрины.
3. Для каждого поля контракта: `nulls.<field>` и `cardinality.<field>`.
4. На этом шаге формируется базовый профиль распределения заполненности набора.

### Шаг 3. Gate-проверки

1. `required_fields_presence`: обязательность `msisdn`, `cgi`, `start_time_utc`.
2. `coords_range`: диапазоны `lat/lon`.
3. `temporal_order`: `end_time_utc >= start_time_utc` (если `end_time_utc` не null).
4. `event_count_valid`: `event_count >= 1`.
5. `source_event_type_vocab`: допустимые значения `cdr/sms/gprs/location`.
6. `distribution.source_event_type`: операционный профиль типов событий.
7. `utc_offset_range`: разумный диапазон `[-12, 14]`.
8. `duplicate_event_key`: дубликаты по ключу `msisdn + start_time_utc + source_event_type + cgi`.

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
| Неожиданные типы `source_event_type` | `source_event_type_vocab=failed` |
| Координаты вне диапазонов | `coords_range=warning` |

---

## Проверки

| Check | Уровень | Смысл |
|-------|---------|-------|
| `dataset_presence` | failed | parquet не найден |
| `dataset_basic` | info | базовые размеры |
| `schema_columns` | gate | полнота контракта колонок |
| `nulls.*` | info | доли null по полям |
| `cardinality.*` | info | кардинальность полей |
| `required_fields_presence` | gate | `msisdn/cgi/start_time_utc` обязательны |
| `coords_range` | gate | диапазоны координат |
| `temporal_order` | gate | корректность интервала времени |
| `event_count_valid` | gate | `event_count >= 1` |
| `source_event_type_vocab` | gate | значения типа события из справочника |
| `distribution.source_event_type` | info | распределение `source_event_type` |
| `utc_offset_range` | gate | контроль смещения часового пояса |
| `duplicate_event_key` | gate | дубликаты ключа события |
| `summary` | info | итоговый статус и счетчики |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ ETL | [`src/mobile/pipelines/dq/stg/geo_all.py`](../../../src/mobile/pipelines/dq/stg/geo_all.py) |
| CLI | [`src/mobile/cli.py`](../../../src/mobile/cli.py) |
| Сборка `stg_geo_all` | [`documents/stg/build_stg_geo_all.md`](../../stg/build_stg_geo_all.md) |
| Схема | [`src/mobile/schema/stg/geo_all.json`](../../../src/mobile/schema/stg/geo_all.json) |
