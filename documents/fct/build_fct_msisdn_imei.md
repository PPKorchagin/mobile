# build-fct-msisdn-imei

**Витрина:** `fct_msisdn_imei` · **Команда:** `build-fct-msisdn-imei` · **Режим:** месячный Parquet с **ежедневным** инкрементом из `stg_geo_all`.

Референс: [`pipelines/stg/msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py). Схема витрины: [`msisdn_imei.json`](../../src/mobile/schema/fct/msisdn_imei.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_geo_all` за отчётный день | События MSISDN + IMEI + `start_time_utc` |
| 2 | Построить суточные интервалы по смене IMEI | Сегменты `(msisdn, imei, valid_from, valid_to)` в границах суток |
| 3 | Инкремент в месячный parquet | `data/fct/msisdn_imei/{YYYY-MM-01}.parquet` |

**Бизнес-назначение:** месячная привязка номера к устройству (смена IMEI при том же MSISDN) для дозаполнения в [`build-fct-geo-intervals`](./build_fct_geo_intervals.md) и рёбер графа в [`build-fct-person`](./build_fct_person.md).

**В scope задач:** чтение geo за день, нормализация MSISDN/IMEI, построение интервалов по временной оси, идемпотентный upsert в месячный файл (снятие вклада дня + merge смежных сегментов по `(msisdn, imei)`).

---

## Параметры запуска

Вызов: `run_build(report_date, stg_geo_all_path, output_path)` ([`cli.py`](../../src/mobile/cli.py) → `build-fct-msisdn-imei`). **Все три обязательны** — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Отчётный день (любой день месяца → файл `{YYYY-MM-01}.parquet`) |
| `stg_geo_all_path` | path | **Да** | Входной `stg_geo_all` за день (файл или каталог) |
| `output_path` | path | **Да** | Месячный parquet `fct_msisdn_imei` |

Пути **относительные к корню репозитория** `mobile` (`resolve_project_path`). Parquet пишется со сжатием **`snappy`** (`DEFAULT_PARQUET_COMPRESSION`).

**Константы ETL в коде** (на вход job **не передаются**): логика merge с gap ≤ 1 с — см. [`msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py) (`_merge_imei_intervals`).

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)); дни с существующим `stg_geo_all`; timed-run `build-fct-msisdn-imei-{YYYY-MM-DD}` с путями `data/stg/geo_all/{date}.parquet` → `data/fct/msisdn_imei/{YYYY-MM-01}.parquet` |
| Все 3 явно | `--report-date`, `--stg-geo-all-path`, `--output-path` (один прогон) |

**Предусловие:** `build-stg-geo-all` за тот же `report_date`.

Локальный запуск:

```bash
uv run mobile build-stg-geo-all
uv run mobile build-fct-msisdn-imei
uv run mobile build-fct-msisdn-imei \
  --report-date 2025-01-15 \
  --stg-geo-all-path data/stg/geo_all/2025-01-15.parquet \
  --output-path data/fct/msisdn_imei/2025-01-01.parquet
```

Логи: `data/logs/mobile.log` (строка `build-fct-msisdn-imei completed`). Метрики: `data/qa/command_timing.jsonl`, `command=build-fct-msisdn-imei` или `build-fct-msisdn-imei-{date}`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `fct_msisdn_imei` — [`msisdn_imei.json`](../../src/mobile/schema/fct/msisdn_imei.json) |
| Формат хранения | Parquet |
| Календарный срез | один файл на **месяц** (`report_date` в пути = `YYYY-MM-01`) |
| Сжатие | `snappy` |

### Путь выхода

Шаблон: `FCT_MSISDN_IMEI_LAYOUT_TEMPLATE` в [`project_paths.py`](../../src/mobile/project_paths.py):

`data/fct/msisdn_imei/{YYYY-MM-01}.parquet`

### Поля витрины

Контракт — [`msisdn_imei.json`](../../src/mobile/schema/fct/msisdn_imei.json) → `fields`.

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | Нормализованный MSISDN (E.164) |
| 2 | `imei` | string | IMEI, 14–16 цифр |
| 3 | `valid_from` | timestamp | Начало интервала (первое событие сегмента) |
| 4 | `valid_to` | timestamp | Конец интервала (последнее событие сегмента) |

---

## Источники витрины

| # | Источник | Путь (по умолчанию) | Назначение |
|---|----------|---------------------|------------|
| 1 | `stg_geo_all` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | События с `msisdn`, `imei`, `start_time_utc` |

---

## Алгоритм обработки данных

Точка входа: `run_build` → `_run_build` в [`msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py).

### Шаг 0. Инициализация

1. `report_date` → границы суток `[day_start, day_end]`.
2. Разрешить `stg_geo_all_path` (файл дня или каталог `…/{date}.parquet`).
3. `output_path` — месячный файл (`report_month_start(report_date)` в CLI по умолчанию).

### Шаг 1. Чтение `stg_geo_all`

1. `read_parquet` колонок `msisdn`, `imei`, `start_time_utc`.
2. При отсутствии файла — `WARNING`, пустой вход (день не меняет month-файл).
3. Метрика: `geo_rows_read`.

### Шаг 2. Подготовка событий

1. `event_ts` ← `start_time_utc`.
2. [`normalize_msisdn`](../../src/mobile/pipelines/stg/subscriber_ids.py), [`normalize_imei`](../../src/mobile/pipelines/stg/subscriber_ids.py).
3. Отбор: `msisdn`, `imei`, `event_ts` not null.
4. Метрика: `event_rows_with_pair`.

### Шаг 3. Суточные интервалы

Для каждого `msisdn` по возрастанию `event_ts`:

1. Сегменты с постоянным `imei`; при смене IMEI — закрыть предыдущий интервал.
2. Clip `valid_from` / `valid_to` в границы суток.
3. Метрика: `day_interval_rows`.

### Шаг 4. Инкремент месячного файла

Через [`_upsert_daily_into_month_parquet`](../../src/mobile/pipelines/stg/msisdn_imei.py):

1. Удалить из month-файла строки, **пересекающие** календарный день `report_date`.
2. Добавить суточные интервалы.
3. `_merge_imei_intervals` по `(msisdn, imei)` (склейка смежных сегментов, gap ≤ 1 с).
4. Записать `output_path`.
5. Метрики: `month_interval_rows`, `distinct_msisdn`.

### Шаг 5. Запись метрик

`append_command_metrics` → `data/qa/command_timing.jsonl`.

### Типовые ситуации

| Ситуация | Поведение |
|----------|-----------|
| Нет `stg_geo_all` за день | warning, день не меняет month-файл |
| Повторный прогон за тот же день | идемпотентно (пересчёт вклада дня) |
| Несколько IMEI у MSISDN в сутки | несколько интервалов в day_rows |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/fct/msisdn_imei.json`](../../src/mobile/schema/fct/msisdn_imei.json) |
| ETL | [`src/mobile/pipelines/stg/msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py) |
| Пути/лейауты | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
| Вход geo | [`build_stg_geo_all.md`](../stg/build_stg_geo_all.md) |
| IMSI + operator | [`build_fct_msisdn_imsi_operator.md`](./build_fct_msisdn_imsi_operator.md) |
| Person | [`build_fct_person.md`](./build_fct_person.md) |
| Geo-intervals | [`build_fct_geo_intervals.md`](./build_fct_geo_intervals.md) |
| DQ | [`dq_fct_msisdn_imei.md`](../dq/fct/dq_fct_msisdn_imei.md) |

Сквозная цепочка: `build-stg-geo-all` → **`build-fct-msisdn-imei`** → **`dq-fct-msisdn-imei`** → **`nb-fct-msisdn-imei`** → downstream.
