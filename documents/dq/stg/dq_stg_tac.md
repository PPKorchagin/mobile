# dq-stg-tac

**Витрина:** `stg_tac` · **Команда:** `dq-stg-tac` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/tac.py`](../../../src/mobile/pipelines/dq/stg/tac.py). Контракт: [`tac.json`](../../../src/mobile/schema/stg/tac.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать parquet по пути CLI | DataFrame витрины |
| 2 | Проверить TAC, M2M, даты, manufacturer | Логи `DQ_STG_TAC` |
| 3 | Итог `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества справочника TAC после `build-stg-tac`.

**В scope:** схема, целостность TAC (8 цифр, без дублей), согласованность `is_m2m` с `equipment_type`, покрытие M2M.

---

## TODO

1. При смене таксономии M2M обновлять `M2M_EQUIPMENT_TYPES` в [`pipelines/stg/tac.py`](../../../src/mobile/pipelines/stg/tac.py) (DQ читает ту же константу).

---

## Параметры запуска

Вызов: `run_dq(parquet_path)` → `dq-stg-tac`.

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `parquet_path` | string (path) | Да | `data/stg/tac.parquet` | `DEFAULT_STG_TAC_OUTPUT_PATH` |

Флагов CLI **нет**. Поля — `STG_TAC_FIELDS`; M2M-типы — `M2M_EQUIPMENT_TYPES` (оба в ETL [`stg/tac.py`](../../../src/mobile/pipelines/stg/tac.py)).

**Константа DQ:** `_MIN_M2M_RATIO = 0.05`.

**Предусловие:** `uv run mobile build-stg-tac`.

```bash
uv run mobile dq-stg-tac
```

---

## Структура проверяемой витрины

12 полей — [`tac.json`](../../../src/mobile/schema/stg/tac.json) → `fields`. Ключевые для DQ: `tac`, `equipment_type`, `is_m2m`, `allocation_date`, `manufacturer`.

---

## Источники

| # | Источник | Путь |
|---|----------|------|
| 1 | Parquet | `data/stg/tac.parquet` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

`_resolve_parquet_path(parquet_path)`; `STG_TAC_FIELDS`; `m2m_types = M2M_EQUIPMENT_TYPES`.

### Шаг 1. Наличие данных

Нет parquet → `dataset_presence` (**failed**), return.

### Шаг 2. Базовые проверки

`dataset_basic`, `schema_columns`, `nulls.*`; `cardinality.*` для всех полей **кроме** `is_m2m`.

### Шаг 3. Предметные проверки

См. раздел [Проверки](#проверки).

### Шаг 4. Итог

`summary`; тег `DQ_STG_TAC`. Формат: `{"tag":"DQ_STG_TAC","check":"...","status":"...","metrics":{...}}`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| Нет parquet | `dataset_presence` failed |
| pandas / pyarrow | Битый parquet |

---

## Проверки

Статусы: **ok** / **warning** / **failed**. Порог M2M: `_MIN_M2M_RATIO = 0.05` в [`pipelines/dq/stg/tac.py`](../../../src/mobile/pipelines/dq/stg/tac.py).

### Наличие и схема

| Check | Статус при сбое | Смысл |
|-------|-----------------|--------|
| `dataset_presence` | **failed** | Нет parquet |
| `dataset_basic` | **ok** | `row_count`, `column_count`, `parquet_path` |
| `schema_columns` | **failed** | Нет колонок из `STG_TAC_FIELDS` (12 полей, см. [`tac.json`](../../../src/mobile/schema/stg/tac.json)) |

### По каждому полю схемы

| Check | Статус | Метрики |
|-------|--------|---------|
| `nulls.{field}` | **ok** | `null_count`, `null_ratio` |
| `cardinality.{field}` | **ok** | `nunique` (для всех полей **кроме** `is_m2m`) |

### Предметные checks

| Check | Статус при сбое | Смысл / метрики |
|-------|-----------------|-----------------|
| `tac_integrity` | **failed** | TAC не `^\d{8}$` или дубликаты; `invalid_tac_count`, `duplicate_tac_count` |
| `m2m_coverage` | **warning** | 0 строк M2M или доля M2M ниже 5%; `m2m_row_count`, `m2m_ratio`, `non_m2m_row_count` |
| `m2m_equipment_type_consistency` | **failed** | Флаг `is_m2m` не согласован с `equipment_type ∈ M2M_EQUIPMENT_TYPES`; `mismatch_count`, **`equipment_type_counts`** (top-20), `configured_m2m_types` |
| `allocation_date_format` | **warning** | Дата не `%Y-%m-%d`; `invalid_date_count`, `min_date`, `max_date` |
| `manufacturer_quality` | **warning** | Пустой или `-` в `manufacturer`; `empty_manufacturer_count` |

### Итог

| Check | Смысл |
|-------|--------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Обзор DQ | [`../README.md`](../README.md) |
| Схема | [`tac.json`](../../../src/mobile/schema/stg/tac.json) |
| ETL build | [`pipelines/stg/tac.py`](../../../src/mobile/pipelines/stg/tac.py) |
| DQ | [`pipelines/dq/stg/tac.py`](../../../src/mobile/pipelines/dq/stg/tac.py) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |
