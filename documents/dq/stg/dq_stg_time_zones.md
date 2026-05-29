# dq-stg-time-zones

**Витрина:** `stg_time_zones` · **Команда:** `dq-stg-time-zones` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/time_zones.py`](../../../src/mobile/pipelines/dq/stg/time_zones.py). Контракт полей: [`time_zones.json`](../../../src/mobile/schema/stg/time_zones.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать конфиг и путь parquet | Целевой файл DQ |
| 2 | Проверить схему, `code`, `timezone`, WKT в `geometry` | Логи `DQ_STG_TIME_ZONES` |
| 3 | Итог `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества справочника тайм-зон после `build-stg-time-zones`.

**В scope задач:** наличие файла, колонки, null/cardinality, диапазон timezone, геометрия.

---

## TODO

1. При необходимости добавить перекрёстную проверку с `stg_oktmo` (сейчас только parquet time_zones).

---

## Параметры запуска

Вызов: `run_dq(parquet_path)` → `dq-stg-time-zones`.

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `parquet_path` | string (path) | Да | `data/stg/time_zones.parquet` | `DEFAULT_STG_TIME_ZONES_OUTPUT_PATH` |

Флагов CLI **нет**. Поля — `STG_TIME_ZONES_FIELDS` в ETL.

**Предусловие:** `uv run mobile build-stg-time-zones`.

```bash
uv run mobile dq-stg-time-zones
```

Логи: `data/logs/mobile.log`. Метрики: `command=dq-stg-time-zones` в `command_timing.jsonl`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_time_zones` |
| Поля | `code`, `name`, `timezone`, `geometry` — JSON → `fields` |

---

## Источники

| # | Источник | Путь |
|---|----------|------|
| 1 | Parquet | `data/stg/time_zones.parquet` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

`_resolve_parquet_path(parquet_path)`; `expected_columns` из `STG_TIME_ZONES_FIELDS`.

### Шаг 1. Наличие данных

Нет parquet → `dataset_presence` (**failed**), summary, return.

### Шаг 2. Базовые проверки

`dataset_basic`, `schema_columns` (**failed** при missing), `nulls.*`, `cardinality.*`.

### Шаг 3. Предметные проверки

1. **`timezone_quality`:** пустые / дубликаты `timezone` → **warning** с `empty_timezone_count`, `duplicate_timezone_count`.
2. **`utc_offset_range`:** `utc_offset` в [-12, 14]; выход за диапазон → **failed** или **warning** по порогу доли.
3. **`wkt_geometry`:** аналогично ОКТМО — парсинг WKT, тип POLYGON/MULTIPOLYGON, топология; метрики `parse_error_count`, `geom_type_counts`.
4. **`point_in_polygon_sample` (если реализовано):** spot-check нескольких тестовых точек против полигонов.

Детали checks — [Проверки](#проверки).

### Шаг 4. Итог

`summary`; тег логов `DQ_STG_TIME_ZONES`. Формат строки: `{"tag":"DQ_STG_TIME_ZONES","check":"...","status":"...","metrics":{...}}`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| Нет parquet | `dataset_presence` failed |
| pandas / pyarrow | Битый parquet |

---

## Проверки

Статусы: **ok** / **warning** / **failed** (`nulls.*`, `cardinality.*` — всегда **ok**).

### Наличие и схема

| Check | Статус при сбое | Смысл |
|-------|-----------------|--------|
| `dataset_presence` | **failed** | Нет parquet |
| `dataset_basic` | **ok** | `row_count`, `column_count`, `parquet_path` |
| `schema_columns` | **failed** | Нет колонок из `STG_TIME_ZONES_FIELDS` (`code`, `name`, `timezone`, `geometry`) |

### По каждому полю схемы

| Check | Статус | Метрики |
|-------|--------|---------|
| `nulls.{field}` | **ok** | `null_count`, `null_ratio` |
| `cardinality.{field}` | **ok** | `nunique` |

### Предметные checks

| Check | Статус при сбое | Смысл / метрики |
|-------|-----------------|-----------------|
| `code_quality` | **warning** | Дубли `code` или NaN после `to_numeric`; `duplicate_code_count`, `invalid_code_count` |
| `timezone_range` | **warning** | `timezone` вне [-12, 14]; `invalid_timezone_count`, `timezone_min`, `timezone_max`, **`distribution`** — доли по значениям timezone (%) |
| `geometry_quality` | **warning** | WKT в `geometry`: `parse_error_count`, `invalid_topology_count`, `empty_geometry_count`, `unsupported_geom_type_count`, `geom_type_counts`, `valid_geometry_count` (допустимы `POLYGON`, `MULTIPOLYGON`) |

### Итог

| Check | Смысл |
|-------|--------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Обзор DQ | [`../README.md`](../README.md) |
| Схема | [`time_zones.json`](../../../src/mobile/schema/stg/time_zones.json) |
| ETL build | [`pipelines/stg/time_zones.py`](../../../src/mobile/pipelines/stg/time_zones.py) |
| DQ | [`pipelines/dq/stg/time_zones.py`](../../../src/mobile/pipelines/dq/stg/time_zones.py) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |
