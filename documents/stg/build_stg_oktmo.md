# build-stg-oktmo

Команда читает `[oktmo.json](../../src/mobile/schema/stg/oktmo.json)`, нормализует CSV ОКТМО и перезаписывает `[data/stg/oktmo.parquet](../../data/stg/oktmo.parquet)`. Справочник без календарного среза; `WKT` хранится как строка, геометрию код не парсит.

**Запуск** (из корня репозитория):

```bash
uv run mobile build-stg-oktmo
```

Флагов CLI нет. Entry point: `mobile = "mobile.cli:main"` в `[pyproject.toml](../../pyproject.toml)`.

---

## На вход


| №   | Артефакт                                               | Формат | Путь (по умолчанию)                | Назначение                                                  |
| --- | ------------------------------------------------------ | ------ | ---------------------------------- | ----------------------------------------------------------- |
| 1   | `[oktmo.json](../../src/mobile/schema/stg/oktmo.json)` | JSON   | `src/mobile/schema/stg/oktmo.json` | `fields`, `csv_path`, `source_mapping_columns`, `readiness` |


CSV из `csv_path` в JSON. Фактический файл: `src/mobile/raw_data/oktmo_v001.csv`.

---

## На выходе


| №   | Артефакт    | Формат           | Путь (по умолчанию)      | Назначение    |
| --- | ----------- | ---------------- | ------------------------ | ------------- |
| 1   | `stg_oktmo` | Parquet (snappy) | `data/stg/oktmo.parquet` | Витрина ОКТМО |


---

## Конфиг → код

`[oktmo.json](../../src/mobile/schema/stg/oktmo.json)`. JSON Schema не проверяется.


| Ключ                            | Использование             |
| ------------------------------- | ------------------------- |
| `csv_path`                      | Входной CSV               |
| `readiness.s3_layout`           | Выходной parquet          |
| `readiness.parquet_compression` | `snappy`                  |
| `source_csv.sep` / `encoding`   | `read_csv` (`;`, `utf-8`) |
| `source_csv.chunk_size`         | Чанки (200000)            |
| `source_mapping_columns`        | CSV → витрина             |
| `fields`                        | Порядок и типы колонок    |


Код: `[pipelines/stg/oktmo.py](../../src/mobile/pipelines/stg/oktmo.py)` — `run_from_config(config_path)`.

---

## Логика сборки

1. `read_csv` (опционально по чанкам) → `_prepare_chunk`: rename, отбор `fields`, cast типов.
2. `concat` → `to_parquet` (перезапись).
3. Метрики — `append_command_metrics(command="build-stg-oktmo", ...)`.

Типы в `_prepare_chunk`: `string`, `int32`/`int64`, `float64`, `bool`; битые int → `<NA>`.

---

## Результат (текущий CSV)

- **2678** строк, **5** колонок: `WKT`, `level`, `parent_code`, `code`, `name`.

---

## Ошибки


| Исключение          | Когда                                     |
| ------------------- | ----------------------------------------- |
| `FileNotFoundError` | Нет конфига или CSV                       |
| `ValueError`        | Нет колонок CSV / неподдерживаемый `type` |
| `KeyError`          | Нет обязательного ключа в JSON            |
| pandas / pyarrow    | Битый CSV, запись на диск                 |


---

## TODO

1. Проверить корректность кодов на соответствие с реальным классификатором.
2. Автоматизировать получение данных от внешнего поставщика с сохранением историчности и обновлением границ.

