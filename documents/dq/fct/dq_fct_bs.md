# dq-fct-bs

**Витрина:** `fct_bs` · **Команда:** `dq-fct-bs` · **Режим:** read-only DQ (процесс не падает при failed checks).

Референс: [`pipelines/dq/fct/bs.py`](../../../src/mobile/pipelines/dq/fct/bs.py). Сборка: [`build_fct_bs.md`](../../fct/build_fct_bs.md). Схема: [`bs.json`](../../../src/mobile/schema/fct/bs.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать parquet `fct_bs` | DataFrame витрины |
| 2 | Проверить схему, ключи CGI, SCD-интервалы, координаты, словари, WKT | JSON-метрики в лог `DQ_FCT_BS` |
| 3 | Сформировать `summary` | Счётчики checks и итоговый статус |

**Бизнес-назначение:** контроль качества исторической витрины БС после [`build-fct-bs`](../../fct/build_fct_bs.md) перед [`build-stg-geo-all`](../../stg/build_stg_geo_all.md) и интервалами.

**В scope задач:** наличие файла, колонки `FCT_BS_FIELDS`, профиль `nulls.*` / `cardinality.*`, ключ `(mcc,mnc,lac,cell_id)`, уникальность среза по `date_on`, порядок `date_on`/`date_off`, координаты, словари `bs_type` / `telecomstandard`, WKT `sector_wkt` и `mapinfo_wkt`.

---

## TODO

1. Добавить исторические тренды quality-метрик (по дням/версиям витрины).

---

## Параметры запуска

Вызов pipeline: `run_dq(parquet_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-fct-bs`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `parquet_path` | path | Нет | `data/fct/bs.parquet` | Историческая витрина `fct_bs` (`fct_bs_output_path()`) |

**CLI:** `--fct-bs-path` — явный parquet; без флага — `fct_bs_output_path()` ([`project_paths.py`](../../../src/mobile/project_paths.py)).

**Константы DQ в коде** ([`bs.py`](../../../src/mobile/pipelines/dq/fct/bs.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_FCT_BS` |
| `_OPEN_END_TS` | `2262-04-11 00:00:00` (открытый конец SCD) |
| `_BS_TYPES` | `m`, `f`, `i`, `x`, `o` |
| `_TELECOMSTANDARD` | `2G`, `3G`, `4G` |
| `_ALLOWED_GEOM_TYPES` | `POLYGON`, `MULTIPOLYGON` |

**Предусловие:** `uv run mobile build-fct-bs` (нужны `build-src-bs`, `build-dim-oktmo`, `build-dim-time-zones`).

Локальный запуск:

```bash
uv run mobile build-fct-bs
uv run mobile dq-fct-bs
uv run mobile dq-fct-bs --fct-bs-path data/fct/bs.parquet
uv run mobile nb-fct-bs
```

Логи: `data/logs/mobile.log` (тег `DQ_FCT_BS`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-fct-bs`. Визуализация: `nb-fct-bs` → `data/notebooks/10_fct_bs.executed.ipynb`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `fct_bs` — [`bs.json`](../../../src/mobile/schema/fct/bs.json) → `table` |
| Путь по умолчанию | `data/fct/bs.parquet` |
| Формат | Parquet |
| Модель данных | SCD Type 2: интервалы `date_on` / `date_off` |
| Контракт полей | `FCT_BS_FIELDS` из [`pipelines/fct/bs.py`](../../../src/mobile/pipelines/fct/bs.py) |

### Поля (контракт)

28 колонок — [`bs.json`](../../../src/mobile/schema/fct/bs.json) → `fields`. Ключевые для DQ:

| Поле | Смысл |
|------|-------|
| `mcc`, `mnc`, `lac`, `cell_id` | CGI-ключ БС |
| `date_on`, `date_off` | Интервал актуальности в STG |
| `lon`, `lat` | Координаты для join и карт |
| `bs_type`, `telecomstandard` | Тип и поколение RAN |
| `sector_wkt`, `mapinfo_wkt` | Геометрии покрытия (WKT) |
| `oktmo_code_1`, `oktmo_code_2` | ОКТМО (профиль через `nulls.*`) |

---

## Источники витрины

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `fct_bs` | `data/fct/bs.parquet` | Единый исторический parquet после `build-fct-bs` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `_resolve_parquet_path(parquet_path)` относительно `PROJECT_ROOT`.
2. Список ожидаемых колонок — `FCT_BS_FIELDS`.
3. Счётчики `total_checks`, `warning_checks`, `failed_checks`.

### Шаг 1. Наличие и чтение

Нет файла → `dataset_presence` (**failed**), `summary`, return.  
Иначе `pd.read_parquet` → `dataset_basic` (**ok**: `row_count`, `column_count`, `parquet_path`).

### Шаг 2. Схема и профиль полей

1. `schema_columns` — все поля контракта присутствуют (**failed** при пропусках).
2. Для каждой доступной колонки контракта: `nulls.{field}` (**ok**), `cardinality.{field}` (**ok**).

### Шаг 3. Ключи и SCD-время

1. `key_presence` — нет null в `(mcc, mnc, lac, cell_id)` (**failed** при `null_key_rows > 0`).
2. `key_uniqueness_per_snapshot` — дубли по `(mcc, mnc, lac, cell_id, date_on)` (**warning**).
3. `temporal_consistency` — `date_off >= date_on`; метрики `open_rows`, `open_ratio` для `date_off = _OPEN_END_TS` (**failed** при `invalid_date_order_count > 0`).

### Шаг 4. Домен и геометрия

1. `coords_range` — `lon ∈ [-180,180]`, `lat ∈ [-90,90]` (**warning**).
2. `bs_type_vocab` — `bs_type ∈ {m,f,i,x,o}` (**warning**).
3. `telecomstandard_vocab` — `{2G,3G,4G}` (**warning**).
4. `geometry.sector_wkt`, `geometry.mapinfo_wkt` — парсинг WKT, тип, топология (**warning** при ошибках).

### Шаг 5. Итог

`summary` — `total_checks`, `warning_checks`, `failed_checks`; return dict со статусом прогона.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `dataset_presence` **failed** | Нет `data/fct/bs.parquet` |
| `schema_columns` **failed** | Нет колонок из `FCT_BS_FIELDS` |
| `key_presence` **failed** | Пустые компоненты CGI |
| `temporal_consistency` **failed** | `date_off < date_on` |
| Битый parquet | исключение pandas/pyarrow при чтении |
| Warning в `geometry.*` | невалидный/пустой WKT, неподдерживаемый тип |

---

## Проверки

Статусы: **ok** — метрика/успешный gate; **warning** / **failed** — отклонение. Формат лога: `{"tag":"DQ_FCT_BS","check":"...","status":"...","metrics":{...}}`.

### Наличие и схема

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet по `parquet_path` не найден | Без файла [`build-stg-geo-all`](../../stg/build_stg_geo_all.md) не может enrich по CGI |
| `dataset_basic` | **ok** | `row_count`, `column_count`, `parquet_path` | Фиксация объёма среза для сравнения прогонов |
| `schema_columns` | **failed** | `missing_columns` vs `FCT_BS_FIELDS` | Контракт совпадает с ETL [`build-fct-bs`](../../fct/build_fct_bs.md) и [`bs.json`](../../../src/mobile/schema/fct/bs.json) |

### Профиль по полям контракта

| Check | Статус | Метрики | Обоснование |
|-------|--------|---------|-------------|
| `nulls.{field}` | **ok** | `null_count`, `null_ratio` | Полнота каждого поля канона (в т.ч. `oktmo_code_*`, геометрии) |
| `cardinality.{field}` | **ok** | `nunique` | Профиль кардинальности без выгрузки значений |

### Ключи и интервалы SCD

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `key_presence` | **failed** | `null_key_rows` по `(mcc,mnc,lac,cell_id)` | CGI обязателен для join с `event_dds` |
| `key_uniqueness_per_snapshot` | **warning** | `duplicate_rows` по ключу + `date_on` | Два состояния одной БС на один момент `date_on` ломают lookup |
| `temporal_consistency` | **failed** | `invalid_date_order_count`, `open_rows`, `open_ratio`, `open_sentinel` | SCD-инвариант; открытые строки (`2262-04-11`) — ожидаемая доля активных БС |

### Координаты и словари

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `coords_range` | **warning** | `invalid_lon_count`, `invalid_lat_count` | Координаты используются в geo-all и картах |
| `bs_type_vocab` | **warning** | `invalid_bs_type_count`, `allowed_values` | Тип БС из генератора `src_bs` |
| `telecomstandard_vocab` | **warning** | `invalid_telecomstandard_count` | Поколение RAN для группировки MAPINFO/секторов |

### Геометрия WKT

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `geometry.sector_wkt` | **warning** | `parse_error_count`, `valid_geometry_count`, `geom_type_counts`, … | Секторное покрытие из ETL; допустимы `POLYGON`, `MULTIPOLYGON` |
| `geometry.mapinfo_wkt` | **warning** | те же метрики | Ячейки Вороного для MAPINFO-слоя |

### Итог

| Check | Смысл | Обоснование |
|-------|-------|-------------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks` | Сводка прогона для мониторинга |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/fct/bs.py`](../../../src/mobile/pipelines/dq/fct/bs.py) |
| DQ notebook | [`pipelines/nb/10_fct_bs.ipynb`](../../../src/mobile/pipelines/nb/10_fct_bs.ipynb) |
| ETL build | [`pipelines/fct/bs.py`](../../../src/mobile/pipelines/fct/bs.py) |
| Пути layout | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`bs.json`](../../../src/mobile/schema/fct/bs.json) |
| DQ src (вход ETL) | [`dq_src_bs.md`](../src/dq_src_bs.md) |

Сквозная цепочка: `build-src-bs` → `build-fct-bs` → `dq-fct-bs` → `nb-fct-bs` → `build-stg-geo-all` → downstream.
