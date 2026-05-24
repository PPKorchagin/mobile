# build-stg-time-zones

Команда читает [`time_zones.json`](../../src/mobile/schema/stg/time_zones.json), нормализует CSV тайм-зон и перезаписывает [`data/stg/time_zones.parquet`](../../data/stg/time_zones.parquet). Колонка `geometry` — WKT-строка, парсинг в build не выполняется.

**Запуск** (из корня репозитория):

```bash
uv run mobile build-stg-time-zones
```

Флагов CLI нет. Entry point: `mobile = "mobile.cli:main"` в [`pyproject.toml`](../../pyproject.toml).

---

## На вход

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | [`time_zones.json`](../../src/mobile/schema/stg/time_zones.json) | JSON | `src/mobile/schema/stg/time_zones.json` | `fields`, `csv_path`, `source_mapping_columns`, `readiness` |

CSV из `csv_path` в JSON. Фактический файл: `src/mobile/raw_data/time_zones.csv`.

---

## На выходе

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | `stg_time_zones` | Parquet (snappy) | `data/stg/time_zones.parquet` | Справочник тайм-зон |

---

## Конфиг → код

[`time_zones.json`](../../src/mobile/schema/stg/time_zones.json). JSON Schema не проверяется.

| Ключ | Использование |
|------|----------------|
| `csv_path` | Входной CSV |
| `readiness.s3_layout` | Выходной parquet |
| `source_csv.sep` / `encoding` | `;`, `utf-8` |
| `source_csv.chunk_size` | Чанки (200000) |
| `source_mapping_columns` | CSV → витрина |
| `fields` | Порядок и типы |

Код: [`pipelines/stg/time_zones.py`](../../src/mobile/pipelines/stg/time_zones.py) — `run_from_config(config_path)`.

---

## Логика сборки

`read_csv` (опционально по чанкам) → `_prepare_chunk` → `concat` → `to_parquet` → `append_command_metrics(command="build-stg-time-zones", ...)`.

---

## Результат (текущий CSV)

- **86** строк, **4** колонки: `code`, `name`, `timezone`, `geometry`.
- JSONL: `read_csv_sec`, `write_parquet_sec`, `elapsed_total_sec`, `run_id`.

---

## Ошибки

| Исключение | Когда |
|------------|-------|
| `FileNotFoundError` | Нет конфига или CSV |
| `ValueError` | Нет колонок CSV / неподдерживаемый `type` |
| `KeyError` | Нет обязательного ключа в JSON |
| pandas / pyarrow | Битый CSV, запись на диск |

---

## TODO

1. Проверить актуальность справочника тайм-зон относительно ОКТМО/регионов.
2. Автоматизировать загрузку от внешнего поставщика, если такого удастся найти.
