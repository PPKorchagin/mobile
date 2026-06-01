# build-stg-msisdn-imsi-operator

**Витрина:** `stg_msisdn_imsi` · **Команда:** `build-stg-msisdn-imsi-operator` · **Режим:** месячный Parquet MSISDN + IMSI + `operator_id` с **ежедневным** инкрементом из `stg_geo_all`.

Референс: [`pipelines/stg/msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py). Схема витрины: [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Построить суточные интервалы MSISDN–IMSI из `stg_geo_all` | Сегменты за день |
| 2 | Вычислить `operator_id` из IMSI (MNC при MCC=250; иначе null) | Поле operator по наблюдениям |
| 3 | Инкремент в месячный parquet | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` |

**Бизнес-назначение:** месячные наблюдения связки номер–SIM и обслуживающего оператора (по MNC в IMSI) для [`build-stg-geo-intervals`](./build_stg_geo_intervals.md) и графа персон в [`build-stg-person`](./build_stg_person.md).

**В scope задач:** только наблюдения из `stg_geo_all`; `operator_id` **не** берётся из `src_person`, а выводится из IMSI (`250` + MNC, цифры 4–5). Идемпотентный upsert по дню с merge по `(msisdn, operator_id, imsi)`.

---

## Параметры запуска

Вызов: `run_build(report_date, stg_geo_all_path, output_path)` ([`cli.py`](../../src/mobile/cli.py) → `build-stg-msisdn-imsi-operator`). **Все три обязательны** — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Отчётный день (любой день месяца → файл `{YYYY-MM-01}.parquet`) |
| `stg_geo_all_path` | path | **Да** | Входной `stg_geo_all` за день (файл или каталог) |
| `output_path` | path | **Да** | Месячный parquet `stg_msisdn_imsi` |

Пути **относительные к корню репозитория** `mobile` (`resolve_project_path`). Parquet пишется со сжатием **`snappy`** (`DEFAULT_PARQUET_COMPRESSION`).

**Константы ETL в коде** (на вход job **не передаются**): правило MNC `imsi[3:5]` при префиксе `250`, merge с gap ≤ 1 с — см. [`msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py).

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)); дни с существующим `stg_geo_all`; timed-run `build-stg-msisdn-imsi-operator-{YYYY-MM-DD}` с путями `data/stg/geo_all/{date}.parquet` → `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` |
| Все 3 явно | `--report-date`, `--stg-geo-all-path`, `--output-path` (один прогон) |

**Предусловие:** `build-stg-geo-all` за тот же `report_date`.

Локальный запуск:

```bash
uv run mobile build-stg-geo-all
uv run mobile build-stg-msisdn-imsi-operator
uv run mobile build-stg-msisdn-imsi-operator \
  --report-date 2025-01-15 \
  --stg-geo-all-path data/stg/geo_all/2025-01-15.parquet \
  --output-path data/stg/msisdn_imsi/2025-01-01.parquet
```

Логи: `data/logs/mobile.log` (строка `build-stg-msisdn-imsi-operator completed`). Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-msisdn-imsi-operator` или `build-stg-msisdn-imsi-operator-{date}`.

Дополнительно пишутся счётчики: `day_imsi_interval_rows`, `day_rows_with_operator`, `month_interval_rows`, `distinct_msisdn`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_msisdn_imsi` — [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json) |
| Формат хранения | Parquet |
| Календарный срез | один файл на **месяц** (`report_date` в пути = `YYYY-MM-01`) |
| Сжатие | `snappy` |

### Путь выхода

Шаблон: `STG_MSISDN_IMSI_LAYOUT_TEMPLATE` в [`project_paths.py`](../../src/mobile/project_paths.py):

`data/stg/msisdn_imsi/{YYYY-MM-01}.parquet`

### Поля витрины

Контракт — [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json) → `fields`.

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | Нормализованный MSISDN (E.164) |
| 2 | `imsi` | string | IMSI, 14–15 цифр (MCC+MNC+MSIN) |
| 3 | `operator_id` | long | MNC при MCC=250 (цифры 4–5); для иностранных IMSI — null |
| 4 | `valid_from` | timestamp | Начало интервала наблюдения |
| 5 | `valid_to` | timestamp | Конец интервала наблюдения |

---

## Источники витрины

| # | Источник | Путь (по умолчанию) | Назначение |
|---|----------|---------------------|------------|
| 1 | `stg_geo_all` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | События с `msisdn`, `imsi`, `start_time_utc` |

---

## Алгоритм обработки данных

Точка входа: `run_build` в [`msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py).

### Шаг 0. Инициализация

1. `report_date` → границы суток `[day_start, day_end]`.
2. Разрешить `stg_geo_all_path` и `output_path`.

### Шаг 1. Суточные интервалы MSISDN–IMSI

`build_imsi_day_intervals`:

1. Чтение `stg_geo_all` (`msisdn`, `imsi`, `start_time_utc`).
2. Нормализация, сегменты по смене IMSI на временной оси (аналогично IMEI-пайплайну).
3. Clip в границы суток → колонки `msisdn`, `imsi`, `valid_from`, `valid_to`.
4. Метрика: `day_imsi_interval_rows`.

### Шаг 2. Расчёт `operator_id`

`build_imsi_intervals_with_operator`:

1. `operator_id_from_imsi_series`: для IMSI `250…` → `operator_id = int(imsi[3:5])` (MNC); иначе `null`.
2. Все валидные пары `(msisdn, imsi)` сохраняются; отбрасываются только строки без MSISDN/IMSI/интервала.
3. Метрики: `day_binding_rows`, `day_rows_with_operator`, `day_rows_without_operator_id`.

### Шаг 3. Инкремент месячного файла

`upsert_imsi_daily_into_month_parquet`:

1. Снять из month-файла строки, пересекающие календарный день.
2. Добавить суточные строки.
3. `_merge_imsi_intervals` по `(msisdn, operator_id, imsi)` (склейка смежных сегментов, gap ≤ 1 с).
4. Записать `output_path`.
5. Метрики: `month_interval_rows`, `distinct_msisdn`.

### Шаг 4. Запись метрик

`append_command_metrics` → `data/qa/command_timing.jsonl`.

### Типовые ситуации

| Ситуация | Поведение |
|----------|-----------|
| Нет `stg_geo_all` за день | warning, день не меняет month-файл |
| IMSI не `250…` | строка в витрине, `operator_id` = null |
| Повторный прогон за день | идемпотентно |
| Смена IMSI в сутки | несколько интервалов / operator_id |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/stg/msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json) |
| ETL | [`src/mobile/pipelines/stg/msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py) |
| Пути/лейауты | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
| Вход geo | [`build_stg_geo_all.md`](./build_stg_geo_all.md) |
| IMEI binding | [`build_stg_msisdn_imei.md`](./build_stg_msisdn_imei.md) |
| Person | [`build_stg_person.md`](./build_stg_person.md) |
| Geo-intervals | [`build_stg_geo_intervals.md`](./build_stg_geo_intervals.md) |
| DQ | [`dq_stg_msisdn_imsi_operator.md`](../dq/stg/dq_stg_msisdn_imsi_operator.md) |

Сквозная цепочка: `build-stg-geo-all` → `build-stg-msisdn-imei` → `dq-stg-msisdn-imei` → **`build-stg-msisdn-imsi-operator`** → **`dq-stg-msisdn-imsi-operator`** → **`nb-stg-msisdn-imsi-operator`** → downstream.
