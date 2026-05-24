# build-stg-tac

Команда читает [`tac.json`](../../src/mobile/schema/stg/tac.json), нормализует CSV TAC (8 цифр, даты `YYYY-MM-DD`, признак `is_m2m`) и перезаписывает [`data/stg/tac.parquet`](../../data/stg/tac.parquet).

**Запуск** (из корня репозитория):

```bash
uv run mobile build-stg-tac
```

Флагов CLI нет. Entry point: `mobile = "mobile.cli:main"` в [`pyproject.toml`](../../pyproject.toml).

---

## На вход

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | [`tac.json`](../../src/mobile/schema/stg/tac.json) | JSON | `src/mobile/schema/stg/tac.json` | `fields`, `csv_path`, `m2m_equipment_types`, `readiness` |

CSV из `csv_path` в JSON. Фактический файл: `src/mobile/raw_data/tacdb_v001.csv` (`;`, `utf-8-sig`).

---

## На выходе

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | `stg_tac` | Parquet (snappy) | `data/stg/tac.parquet` | Справочник TAC |

В метриках build: `m2m_row_count`.

---

## Конфиг → код

[`tac.json`](../../src/mobile/schema/stg/tac.json). JSON Schema не проверяется.

| Ключ | Использование |
|------|----------------|
| `csv_path` | Входной CSV |
| `readiness.s3_layout` | Выходной parquet |
| `source_mapping_columns` | CSV → витрина |
| `m2m_equipment_types` | `equipment_type` → `is_m2m = true` |
| `fields` | Порядок колонок; `is_m2m` вычисляется |

Код: [`pipelines/stg/tac.py`](../../src/mobile/pipelines/stg/tac.py) — `run_from_config(config_path)`.

---

## Логика сборки

1. `read_csv` → `_prepare_dataset`.
2. TAC: strip, только цифры, `zfill(8)`, regex `^\d{8}$`.
3. `allocation_date`: `%d.%m.%Y`, fallback `dayfirst=True` → `YYYY-MM-DD` string.
4. `is_m2m` по `equipment_type in m2m_equipment_types`.
5. Дубликаты TAC после нормализации → `ValueError`.
6. `to_parquet` → `append_command_metrics(command="build-stg-tac", ...)`.

---

## Результат (текущий CSV)

- **22553** строк, **12** колонок (`is_m2m` — `boolean`, остальное `string`).
- JSONL: `read_csv_sec`, `write_parquet_sec`, `elapsed_total_sec`, `run_id`, `m2m_row_count`.

---

## Ошибки

| Исключение | Когда |
|------------|-------|
| `FileNotFoundError` | Нет конфига или CSV |
| `ValueError` | Невалидный TAC; непарсимая дата; дубликат TAC |
| pandas / pyarrow | Битый CSV, запись на диск |

---

## TODO

1. Сверить список `m2m_equipment_types` с актуальной таксономией GSMA.
2. Периодически обновлять `tacdb_v001.csv` (Osmocom TACDB) или перейти на источник от поставщика.
