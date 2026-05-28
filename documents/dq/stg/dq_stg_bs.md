# dq-stg-bs

**Витрина:** `stg_bs` · **Команда:** `dq-stg-bs` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/bs.py`](../../../src/mobile/pipelines/dq/stg/bs.py). Контракт: [`bs.json`](../../../src/mobile/schema/stg/bs.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_bs` parquet | DataFrame витрины |
| 2 | Проверить схему, ключи, интервалы, координаты, геометрию | Логи `DQ_STG_BS` |
| 3 | Сформировать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества исторической витрины БС после `build-stg-bs`.

---

## Параметры запуска

Вызов: `run_dq(parquet_path)` → `dq-stg-bs`.

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `parquet_path` | string (path) | Да | `data/stg/bs.parquet` | Историческая витрина `stg_bs` |

```bash
uv run mobile dq-stg-bs
```

---

## Проверки

Статусы: **ok** / **warning** / **failed**.

| Check | Статус при сбое | Смысл |
|-------|------------------|-------|
| `dataset_presence` | **failed** | Нет parquet |
| `schema_columns` | **failed** | Нет колонок из контракта `STG_BS_FIELDS` |
| `nulls.*`, `cardinality.*` | **ok** | Профили полей |
| `key_presence` | **failed** | Есть строки с null в `(mcc,mnc,lac,cell_id)` |
| `key_uniqueness_per_snapshot` | **warning** | Дубли по `(mcc,mnc,lac,cell_id,date_on)` |
| `temporal_consistency` | **failed** | `date_off < date_on`; плюс метрика открытых строк (`date_off=2262-04-11`) |
| `coords_range` | **warning** | Координаты вне диапазона |
| `bs_type_vocab` | **warning** | `bs_type` не из `{m,f,i,x,o}` |
| `telecomstandard_vocab` | **warning** | `telecomstandard` не из `{2G,3G,4G}` |
| `geometry.sector_wkt` | **warning** | Невалидная/пустая/неподдерживаемая геометрия |
| `geometry.mapinfo_wkt` | **warning** | Невалидная/пустая/неподдерживаемая геометрия |
| `summary` | **ok** | `total_checks`, `warning_checks`, `failed_checks` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`bs.json`](../../../src/mobile/schema/stg/bs.json) |
| ETL build | [`pipelines/stg/bs.py`](../../../src/mobile/pipelines/stg/bs.py) |
| DQ | [`pipelines/dq/stg/bs.py`](../../../src/mobile/pipelines/dq/stg/bs.py) |
