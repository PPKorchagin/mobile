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

| Check | Условие |
|-------|---------|
| `tac_integrity` | **failed** — TAC не `^\d{8}$` или дубликаты |
| `m2m_coverage` | **warning** — 0 строк M2M или доля &lt; 5% |
| `m2m_equipment_type_consistency` | **failed** — `is_m2m` ≠ `(equipment_type ∈ m2m_types)` |
| `allocation_date_format` | **warning** — не парсится как `%Y-%m-%d` |
| `manufacturer_quality` | **warning** — пустой или `-` |

### Шаг 4. Итог

`summary`; тег `DQ_STG_TAC`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| Нет parquet | `dataset_presence` failed |
| pandas / pyarrow | Битый parquet |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`tac.json`](../../../src/mobile/schema/stg/tac.json) |
| ETL build | [`pipelines/stg/tac.py`](../../../src/mobile/pipelines/stg/tac.py) |
| DQ | [`pipelines/dq/stg/tac.py`](../../../src/mobile/pipelines/dq/stg/tac.py) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |
