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

| Check | Условие |
|-------|---------|
| `code_quality` | **warning** — дубли `code` или NaN после `to_numeric` |
| `timezone_range` | **warning** — значения вне [-12, 14]; метрики min/max и `distribution` |
| `geometry_quality` | **warning** — ошибки WKT (`_collect_wkt_metrics` для колонки `geometry`) |

### Шаг 4. Итог

`summary`; тег логов `DQ_STG_TIME_ZONES`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| Нет parquet | `dataset_presence` failed |
| pandas / pyarrow | Битый parquet |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`time_zones.json`](../../../src/mobile/schema/stg/time_zones.json) |
| ETL build | [`pipelines/stg/time_zones.py`](../../../src/mobile/pipelines/stg/time_zones.py) |
| DQ | [`pipelines/dq/stg/time_zones.py`](../../../src/mobile/pipelines/dq/stg/time_zones.py) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |
