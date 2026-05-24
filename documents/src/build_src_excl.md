# build-src-excl

Формирует списки исключений **`src_imsi`**, **`src_imei`**, **`src_msisdn`** — случайная выборка связанных троек (imsi, imei, msisdn) из последнего полного среза **`src_person`** (каталог с маркером `_SUCCESS`).

**Запуск** (из корня репозитория):

```bash
uv run mobile build-src-excl
uv run mobile build-src-excl --excl-pct-of-ab 0.5
```

Параметры — константы и флаг CLI в [`cli_defaults.py`](../../src/mobile/cli_defaults.py). Даты CLI **не используются**. Entry point: `mobile = "mobile.cli:main"` в [`pyproject.toml`](../../pyproject.toml).

**Предварительно:** `mobile build-src-person` (нужен каталог person с `_SUCCESS`).

---

## На вход

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | [`person.json`](../../src/mobile/schema/src/person.json) | JSON | `src/mobile/schema/src/person.json` | Layout и `success_flag` источника |
| 2 | [`imsi.json`](../../src/mobile/schema/src/imsi.json) | JSON | `src/mobile/schema/src/imsi.json` | Схема выхода IMSI |
| 3 | [`imei.json`](../../src/mobile/schema/src/imei.json) | JSON | `src/mobile/schema/src/imei.json` | Схема выхода IMEI |
| 4 | [`msisdn.json`](../../src/mobile/schema/src/msisdn.json) | JSON | `src/mobile/schema/src/msisdn.json` | Схема выхода MSISDN |
| 5 | Срез person | Parquet | `data/src/person/.../person.parquet` | Последний день с `_SUCCESS` |

---

## На выходе

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | `src_imsi` | Parquet (snappy) | `data/src/excl/src_imsi.parquet` | Колонка `value` (IMSI) |
| 2 | `src_imei` | Parquet (snappy) | `data/src/excl/src_imei.parquet` | Колонка `value` (IMEI) |
| 3 | `src_msisdn` | Parquet (snappy) | `data/src/excl/src_msisdn.parquet` | Колонка `value` (MSISDN) |

---

## Параметры CLI → `BuildSrcExclParams`

| Параметр | CLI | По умолчанию | Смысл |
|----------|-----|--------------|-------|
| `pct_of_ab` | `--excl-pct-of-ab` | `DEFAULT_SRC_EXCL_PCT_OF_AB` (**0.7**) | % строк **АБ** в исключениях |
| `seed` | константа | `DEFAULT_BS_SEED` (`20250407`) | Детерминированная выборка |

`sample_size = min(eligible_triples, max(1, round(ab_row_count × pct / 100)))`, где `eligible_triples` — уникальные строки с непустыми imsi, imei, isdn (`isdn` → msisdn).

---

## Конфиг → код

Код: [`pipelines/src/excl.py`](../../src/mobile/pipelines/src/excl.py) — `run_from_config(...)`.

1. Последний каталог дня с `_SUCCESS` по layout из `person.json`.
2. Отбор уникальных троек imsi/imei/msisdn.
3. Случайная выборка `sample_size` строк.
4. Запись трёх parquet; метрики `append_command_metrics(command="build-src-excl", ...)`.

---

## Ошибки

| Исключение | Когда |
|------------|-------|
| `FileNotFoundError` | Нет конфига или нет `_SUCCESS` / parquet person |
| `ValueError` | Нет строк с полной тройкой imsi/imei/msisdn |
