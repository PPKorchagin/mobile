# dq-stg-event

**Витрина:** `stg_event` (DDS-слой `event_dds`) · **Команда:** `dq-stg-event` · **Режим:** read-only DQ (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/event.py`](../../../src/mobile/pipelines/dq/stg/event.py). Сборка DDS: [`build_move_event.md`](../../stg/build_move_event.md). Схема полей: [`event.json`](../../../src/mobile/schema/stg/event.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Обойти каталог `event_dds_root`, найти все `*.parquet` за `report_date` | Список файлов по ЦОД |
| 2 | Отфильтровать строки по `event_timestamp[:8]` | Срез отчётного дня |
| 3 | Профиль полей, микс событий, gate `stg_contract.*` | JSON-метрики в лог `DQ_STG_EVENT` |
| 4 | Сформировать `summary` | Счётчики checks и итоговый статус |

**Бизнес-назначение:** контроль качества дневного DDS-среза событий после [`build-move-event`](../../stg/build_move_event.md) перед потребителями geo-слоя.

**В scope задач:** рекурсивный обход каталога, чтение Parquet, фильтр по локальным суткам, проверки контракта `STG_EVENT_FIELDS`, gate по идентификаторам и `location` (без PII в distribution).

---

## TODO

1. Notebook DQ (по аналогии с geo).

---

## Параметры запуска

Вызов pipeline: `run_dq(report_date, event_dds_root)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-stg-event`). **Оба параметра обязательны.**

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | **Да** | `DEFAULT_SRC_*` (оркестратор CLI) | Отчётный день (`--report-date`) |
| `event_dds_root` | path (dir) | **Да** | `data/stg/event_dds` | Корень каталога DDS (`--event-dds-path`) |

Pipeline принимает **только каталог**. Обход: `event_dds_root/{YYYY-MM-DD}/*.parquet` или `rglob` с фильтром по дате в пути ([`discover_event_dds_parquet_paths`](../../../src/mobile/pipelines/stg/event_dds_reader.py)).

**CLI:** оркестратор перебирает календарные дни `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../../src/mobile/cli_defaults.py)); на каждый день — один timed-run с тем же `event_dds_root` (все ЦОД за день в одном прогоне).

**Без флагов** — **43** прогона (по одному на календарный день SRC).

**С `--report-date`** — один прогон за указанный день; опционально `--event-dds-path` переопределяет корень каталога.

**Константы DQ в коде** ([`event.py`](../../../src/mobile/pipelines/dq/stg/event.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_STG_EVENT` |
| `STG_EVENT_CRITICAL_COLUMNS` | поля из `STG_EVENT_FIELDS` |
| `EVENT_CODES` | коды/имена событий (из ETL [`stg/event.py`](../../../src/mobile/pipelines/stg/event.py)) |

**Предусловие:** `uv run mobile build-move-event` (или `build-stg-event` + `build-move-event`) за ту же `report_date`.

Локальный запуск:

```bash
uv run mobile build-move-event
uv run mobile dq-stg-event
uv run mobile dq-stg-event --report-date 2025-01-01
uv run mobile dq-stg-event --report-date 2025-01-01 --event-dds-path data/stg/event_dds
uv run mobile nb-stg-event
```

Логи: `data/logs/mobile.log` (тег `DQ_STG_EVENT`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-stg-event-{date}`. Визуализация: `nb-stg-event` → `data/notebooks/9_stg_event.executed.ipynb`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_event` — [`event.json`](../../../src/mobile/schema/stg/event.json) → `table` |
| Слой хранения | `data/stg/event_dds/{YYYY-MM-DD}/{source_id}.parquet` |
| Формат | Parquet |
| Партиционирование | Отчётный день × ЦОД (`source_id`) |
| Календарный срез | `report_date` (`event_timestamp[:8]`, локальное время) |

### Поля (контракт)

Контракт — [`event.json`](../../../src/mobile/schema/stg/event.json) → `fields`; см. [`build_stg_event.md`](../../stg/build_stg_event.md) → «Поля витрины».

---

## Источники витрины

| # | Источник | Путь (фрагмент) |
|---|----------|-----------------|
| 1 | DDS Parquet | `…/event_dds/{YYYY-MM-DD}/central.parquet` |
| 2 | DDS Parquet | `…/event_dds/{YYYY-MM-DD}/far-east.parquet` |

Файлы отбираются из `event_dds_root` функцией `discover_event_dds_parquet_paths`; `source_id` — из имени файла ([`project_paths.py`](../../../src/mobile/project_paths.py)).

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Проверить, что `event_dds_root` — каталог; иначе `ValueError`.
2. `discover_event_dds_parquet_paths(root, report_date)` — все `*.parquet` дня.
3. Группировка по `source_id` (`central` / `far-east`).

### Шаг 1. Наличие и покрытие

Нет файлов → `dataset_presence` (**failed**). Иначе `coverage`, `source.coverage` по ЦОД.

### Шаг 2. Чтение и фильтр

1. `read_parquet` по каждому файлу сегмента ЦОД.
2. `_filter_df_by_local_report_date` — `event_timestamp[:8] == report_date`.
3. Агрегат по всем ЦОД — общие метрики без префикса `source.`.

### Шаг 3. Профиль и gates

Для каждого `source_id` и для объединённого среза:

1. **Парсинг времени:** `event_timestamp_parseable`.
2. **Счётчики:** `event_count_valid`.
3. **Распределения:** `distribution.*` (типы событий, часы, длины ID, location).
4. **Дубликаты:** ключ `imsi + event_timestamp + event`.
5. **STG-контракт:** `event.stg_contract.*`, `stg_contract.columns`.

Полный перечень — раздел [Проверки](#проверки).

### Шаг 4. Итог

`summary` — `total_checks`, `warning_checks`, `failed_checks`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `ValueError` | `event_dds_root` не каталог |
| `dataset_presence` failed | Нет parquet за `report_date` в каталоге |
| Warning `sample_read` | Пустой срез после фильтра для сегмента |
| `SystemExit` | CLI: явный прогон без `--report-date` |

---

## Проверки

Статусы: **info** — метрика (`emit_metric`); **ok** / **warning** / **failed** — gate (`emit_gate`).  
Префикс `source.{dc}`: `central` | `far-east` — те же checks на срезе одного ЦОД. Checks без префикса — объединённый срез за день (оба ЦОД).

### Покрытие и наличие данных

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Нет `*.parquet` за `report_date` в `event_dds_root` | Без файлов DDS downstream (`build-stg-geo-all`, binding) не может стартовать |
| `coverage` | info | `parquet_files`, `row_count_total`, список путей | Сводное покрытие дня перед профилем и gate |
| `source.coverage` | info | Файлов и строк по `source_id` | Контроль, что оба ЦОД (или ожидаемый сегмент) присутствуют в выгрузке |

### Профиль среза (info)

| Check | Срез | Смысл | Обоснование |
|-------|------|-------|-------------|
| `source.{dc}.sample_read` | ЦОД | Пустой DataFrame после фильтра по дню | Ранний выход из глубокого профиля сегмента; не смешивать с отсутствием файла |
| `source.{dc}.sample_basic` / `sample_basic` | ЦОД / день | `row_count`, `parquet_files` | Базовый объём среза для калибровки и сравнения с SRC-DQ |
| `source.{dc}.event_distribution` / `event_distribution` | ЦОД / день | Счётчики по коду `event` (10001…10004) | Sanity микса типов событий после `build-stg-event` |
| `source.{dc}.event_name_distribution` / `event_name_distribution` | ЦОД / день | Счётчики `cdr` / `sms` / `gprs` / `location` | Согласованность имён с кодами OCC |
| `source.{dc}.distribution.{col}` / `distribution.{col}` | ЦОД / день | Профиль колонки (numeric / categorical top-N) | Калибровка генератора; без значений PII |
| `source.{dc}.distribution.event_count_bucket` / `distribution.event_count_bucket` | ЦОД / день | Корзины `1` / `2-5` / `6-20` / `21+` | Контроль доли 5m-схлопывания из [`build-stg-event`](../../stg/build_stg_event.md) |
| `source.{dc}.distribution.event_timestamp_hour` / `distribution.event_timestamp_hour` | ЦОД / день | Час суток (`event_timestamp[8:10]`) | Профиль активности абонента в локальных сутках |
| `source.{dc}.distribution.{imsi,imei,msisdn}_length` | ЦОД / день | Длина строки идентификатора | Формат ID без логирования самих номеров |
| `source.{dc}.distribution.imei_tac` | ЦОД / день | Top-N TAC (первые 8 цифр IMEI) | Разнообразие терминалов без полного IMEI |
| `source.{dc}.distribution.location_{mcc,mnc,lac,cell}` | ЦОД / день | Top-N по полям struct `location` | Sanity CGI без координат абонента |
| `source.{dc}.distribution.location_compressible` | ЦОД / день | Доля строк с валидным lac/cell | Оценка пригодности к сжатию в ETL |
| `source.{dc}.null_rates` / `null_rates` | ЦОД / день | `null_rate_by_column` по `STG_EVENT_FIELDS` | Полнота полей канонической витрины |
| `source.{dc}.imsi_event_timestamp_event_duplicates` / `imsi_event_timestamp_event_duplicates` | ЦОД / день | Строки с дублем `(imsi, event_timestamp, event)` | Диагностика повторов; не gate (схлопывание даёт `event_count>1`) |

Колонки для `distribution.{col}` (скаляры): `event`, `event_name`, `event_count`.

### Gate temporal и агрегация

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `source.{dc}.event_timestamp_parseable` / `event_timestamp_parseable` | **failed** / **warning** | Доля `event_timestamp` = 14 цифр `YYYYMMDDhhmmss` (<99% / <99.5%) | Локальный календарный срез в `build-stg-geo-all` и binding завязаны на parseable timestamp |
| `source.{dc}.event_count_valid` / `event_count_valid` | **failed** | Строки с `event_count` < 1 или не число; в метриках — `aggregated_rows`, `aggregated_share` (`event_count>1`) | Инвариант ETL: каждая строка — ≥1 событие; доля схлопнутых групп — контроль 5m-bucket из [`build-stg-event`](../../stg/build_stg_event.md) |

### Gate STG-контракт (`event.stg_contract.*` / `source.{dc}.event.stg_contract.*`)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `.stg_contract.sample` | **warning** | Пустой DataFrame на gate | Нет данных для проверки контракта |
| `.stg_contract.event` | **failed** / **warning** | `event` ∈ {10001, 10002, 10003, 10004} (<99.9% / <100%) | Код OCC согласован с layout SRC и [`EVENT_CODES`](../../../src/mobile/pipelines/stg/event.py) |
| `.stg_contract.event_name` | **failed** / **warning** | `event_name` ∈ {cdr, sms, gprs, location} | Человекочитаемый тип для отчётов и join |
| `.stg_contract.event_code_name_alignment` | **failed** / **warning** | Согласованность `event` и `event_name` (<99.9%) | Исключение рассинхрона кода и имени после concat витрин |
| `.stg_contract.location` | **failed** / **warning** | Struct `location`: mcc/mnc непустые при наличии (<90% / <98%) | Минимальная геопривязка для geo-all и карт |
| `.stg_contract.location_compressible` | **warning** | `compressible_location_rate` < 50% | Низкая доля строк, пригодных к 5m-схлопыванию; не блокирует прогон |
| `stg_contract.columns` | **failed** | Отсутствует колонка из `STG_EVENT_FIELDS` | Минимальный контракт [`event.json`](../../../src/mobile/schema/stg/event.json) для потребителей DDS |

Обязательные поля (`STG_EVENT_FIELDS`): `event_timestamp`, `imsi`, `imei`, `msisdn`, `location`, `event`, `event_name`, `event_count`.

### Итог

| Check | Смысл | Обоснование |
|-------|-------|-------------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks` | Сводка прогона для мониторинга и сравнения периодов |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/stg/event.py`](../../../src/mobile/pipelines/dq/stg/event.py) |
| Обход каталога | [`pipelines/stg/event_dds_reader.py`](../../../src/mobile/pipelines/stg/event_dds_reader.py) |
| Перенос DDS | [`build_move_event.md`](../../stg/build_move_event.md) |
| Сборка событий | [`build_stg_event.md`](../../stg/build_stg_event.md) |
| Пути layout | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`event.json`](../../../src/mobile/schema/stg/event.json) |
| DQ mobile (вход) | [`dq_src_mobile.md`](../src/dq_src_mobile.md) |

Сквозная цепочка: `build-src-mobile` → `build-stg-event` → `build-move-event` → `dq-stg-event` → geo/downstream.
