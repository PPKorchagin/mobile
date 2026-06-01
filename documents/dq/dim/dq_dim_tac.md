# dq-dim-tac

**Витрина:** `dim_tac` · **Команда:** `dq-dim-tac` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: `[pipelines/dq/dim/tac.py](../../../src/mobile/pipelines/dq/dim/tac.py)`. Контракт: `[tac.json](../../../src/mobile/schema/dim/tac.json)`.

---

## Задачи pipeline


| #   | Задача                                 | Результат         |
| --- | -------------------------------------- | ----------------- |
| 1   | Прочитать parquet по пути CLI          | DataFrame витрины |
| 2   | Проверить TAC, M2M, даты, manufacturer | Логи `DQ_DIM_TAC` |
| 3   | Итог `summary`                         | Счётчики checks   |


**Бизнес-назначение:** контроль качества справочника TAC после `build-dim-tac`.

**В scope:** схема, целостность TAC (8 цифр, без дублей), согласованность `is_m2m` с `equipment_type`, покрытие M2M.

---

## TODO

1. При смене таксономии M2M обновлять `M2M_EQUIPMENT_TYPES` в `[pipelines/dim/tac.py](../../../src/mobile/pipelines/dim/tac.py)` (DQ читает ту же константу).

---

## Параметры запуска

Вызов: `run_dq(tac_path)` (`[cli.py](../../../src/mobile/cli.py)` → `dq-dim-tac`).


| Переменная | Тип           | Обязательность | Значение по умолчанию  | Описание              |
| ---------- | ------------- | -------------- | ---------------------- | --------------------- |
| `tac_path` | string (path) | Да             | `data/dim/tac.parquet` | CLI: `**--tac-path`** |


```bash
uv run mobile dq-dim-tac
uv run mobile dq-dim-tac --tac-path data/dim/tac.parquet
```

Флаги CLI: `**--tac-path**`. Поля — `DIM_TAC_FIELDS`; M2M-типы — `M2M_EQUIPMENT_TYPES` (оба в ETL `[stg/tac.py](../../../src/mobile/pipelines/dim/tac.py)`).

**Константа DQ:** `_MIN_M2M_RATIO = 0.05`.

**Предусловие:** `uv run mobile build-dim-tac`.

```bash
uv run mobile dq-dim-tac
```

---

## Структура проверяемой витрины

12 полей — `[tac.json](../../../src/mobile/schema/dim/tac.json)` → `fields`. Ключевые для DQ: `tac`, `equipment_type`, `is_m2m`, `allocation_date`, `manufacturer`.

---

## Источники


| #   | Источник | Путь                   |
| --- | -------- | ---------------------- |
| 1   | Parquet  | `data/dim/tac.parquet` |


---

## Алгоритм обработки данных

### Шаг 0. Инициализация

`_resolve_tac_path(tac_path)`; `DIM_TAC_FIELDS`; `m2m_types = M2M_EQUIPMENT_TYPES`.

### Шаг 1. Наличие данных

Нет parquet → `dataset_presence` (**failed**), return.

### Шаг 2. Базовые проверки

`dataset_basic`, `schema_columns`, `nulls.*`; `cardinality.*` для всех полей **кроме** `is_m2m`.

### Шаг 3. Предметные проверки

Для каждой проверки — отдельная запись в лог с `tag=DQ_DIM_TAC`. Порог M2M: `_MIN_M2M_RATIO = 0.05`.

1. `**tac_integrity`:** все `tac` match `^\d{8}$`; `duplicate_tac_count` по полному дубликату ключа → **failed** при нарушении.
2. `**m2m_coverage`:** `m2m_row_count`, `m2m_ratio`, `non_m2m_row_count`; **warning**, если M2M=0 или `m2m_ratio < 5%` (ожидается доля IoT в справочнике).
3. `**m2m_equipment_type_consistency`:** для каждой строки `is_m2m == (equipment_type in M2M_EQUIPMENT_TYPES)`; при `mismatch_count > 0` → **failed** + top-20 `equipment_type_counts`.
4. `**allocation_date_format`:** парсинг даты; **warning** при `invalid_date_count > 0`.
5. `**manufacturer_quality`:** пустой или `-` в `manufacturer` → **warning** с `empty_manufacturer_count`.

### Шаг 4. Итог

`summary`; тег `DQ_DIM_TAC`. Формат: `{"tag":"DQ_DIM_TAC","check":"...","status":"...","metrics":{...}}`.

### Типовые ошибки


| Ошибка           | Причина                   |
| ---------------- | ------------------------- |
| Нет parquet      | `dataset_presence` failed |
| pandas / pyarrow | Битый parquet             |


---

## Проверки

Статусы: **ok** / **warning** / **failed**. Порог M2M: `_MIN_M2M_RATIO = 0.05` в `[pipelines/dq/dim/tac.py](../../../src/mobile/pipelines/dq/dim/tac.py)`.

### Наличие и схема


| Check              | Статус при сбое | Смысл                                      | Обоснование                                                           |
| ------------------ | --------------- | ------------------------------------------ | --------------------------------------------------------------------- |
| `dataset_presence` | **failed**      | Нет parquet                                | Без файла витрины DQ и downstream (`build-fct-person`) не имеют входа |
| `dataset_basic`    | **ok**          | `row_count`, `column_count`, `tac_path`    | Фиксация объёма среза для сравнения прогонов                          |
| `schema_columns`   | **failed**      | Нет колонок из `DIM_TAC_FIELDS` (12 полей) | Контракт колонок совпадает с ETL и ожиданиями person/M2M-фильтра      |


### По каждому полю схемы


| Check                 | Статус | Метрики                                       | Обоснование                      |
| --------------------- | ------ | --------------------------------------------- | -------------------------------- |
| `nulls.{field}`       | **ok** | `null_count`, `null_ratio`                    | Доля пропусков по полю контракта |
| `cardinality.{field}` | **ok** | `nunique` (для всех полей **кроме** `is_m2m`) | Профиль полноты и выбросов       |


### Предметные checks


| Check                            | Статус при сбое | Смысл / метрики                                                                                                              | Обоснование                                                                   |
| -------------------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `tac_integrity`                  | **failed**      | TAC не `^\d{8}$` или дубликаты; `invalid_tac_count`, `duplicate_tac_count`                                                   | `tac` — ключ справочника и первые 8 цифр IMEI                                 |
| `m2m_coverage`                   | **warning**     | 0 строк M2M или доля M2M ниже 5%; `m2m_row_count`, `m2m_ratio`, `non_m2m_row_count`                                          | Ожидается доля IoT в TACDB; используется при отсечении M2M в person           |
| `m2m_equipment_type_consistency` | **failed**      | Флаг `is_m2m` не согласован с `equipment_type ∈ M2M_EQUIPMENT_TYPES`; `mismatch_count`, `**equipment_type_counts`** (top-20) | ETL вычисляет `is_m2m` из `equipment_type`; рассогласование ломает M2M-фильтр |
| `allocation_date_format`         | **warning**     | Дата не `%Y-%m-%d`; `invalid_date_count`, `min_date`, `max_date`                                                             | Даты аллокации должны быть нормализованы на STG                               |
| `manufacturer_quality`           | **warning**     | Пустой или `-` в `manufacturer`; `empty_manufacturer_count`                                                                  | Пустой производитель снижает качество классификации терминалов                |


### Итог


| Check     | Смысл                                             |
| --------- | ------------------------------------------------- |
| `summary` | `total_checks`, `warning_checks`, `failed_checks` |


---

## Ссылки


| Артефакт  | Путь                                                                     |
| --------- | ------------------------------------------------------------------------ |
| Схема     | `[tac.json](../../../src/mobile/schema/dim/tac.json)`                    |
| ETL build | `[pipelines/dim/tac.py](../../../src/mobile/pipelines/dim/tac.py)`       |
| DQ        | `[pipelines/dq/dim/tac.py](../../../src/mobile/pipelines/dq/dim/tac.py)` |
| Пути      | `[project_paths.py](../../../src/mobile/project_paths.py)`               |


