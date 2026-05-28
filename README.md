# mobile

CLI и пайплайны для mobile OSS-витрин. Схемы — `src/mobile/schema/`, документация — `documents/`.

## Запуск

```bash
uv sync
uv run mobile build-src
```

`build-src` выполняет build STG/SRC-справочников и в конце — `nb-perf-metrics`.

Или по отдельности:

```bash
uv run mobile build-stg-day
uv run mobile build-stg-oktmo
uv run mobile build-stg-time-zones
uv run mobile build-stg-tac
uv run mobile dq-stg-oktmo
uv run mobile dq-stg-time-zones
uv run mobile dq-stg-tac
uv run mobile dq-stg-bs
uv run mobile build-src-bs
uv run mobile build-src-person
uv run mobile build-src-excl
uv run mobile build-src-mobile
uv run mobile build-stg-event
uv run mobile build-stg-event --dc central --report-date 2025-01-01
uv run mobile build-stg-geo-all --report-date 2025-01-01
uv run mobile build-move-event --report-date 2025-01-01
uv run mobile dq-src-mobile
uv run mobile dq-src-bs
uv run mobile dq-src-mobile --dc central --report-date 2025-01-01
uv run mobile dq-stg-event --dc central --report-date 2025-01-01
uv run mobile build-stg-msisdn-imsi --report-date 2025-01-01
uv run mobile build-stg-msisdn-imei --report-date 2025-01-01
uv run mobile build-stg-bs
uv run mobile nb-perf-metrics
```

Логи: `data/logs/mobile.log` (console + rotating file).  
Метрики времени: `data/qa/command_timing.jsonl`.  
Дашборд метрик: `data/notebooks/perf_metrics.executed.ipynb`.

## CLI

| Команда | Описание |
|---------|----------|
| `build-src` | `build-stg-oktmo` → `build-stg-time-zones` → `build-stg-tac` → `build-src-bs` → `build-src-person` → `build-src-excl` → `build-src-mobile` → `nb-perf-metrics` |
| `build-stg-day` | STG build + DQ за `--day` (по умолчанию `2025-01-01`) → `data/stg/load_day=…/` |
| `build-stg-oktmo` | CSV → `data/stg/oktmo.parquet` |
| `build-stg-time-zones` | CSV → `data/stg/time_zones.parquet` |
| `build-stg-tac` | CSV → `data/stg/tac.parquet` |
| `dq-stg-oktmo` | DQ `data/stg/oktmo.parquet` — схема, иерархия, WKT ([checks](documents/dq/stg/dq_stg_oktmo.md#проверки)) |
| `dq-stg-time-zones` | DQ `data/stg/time_zones.parquet` — timezone, geometry ([checks](documents/dq/stg/dq_stg_time_zones.md#проверки)) |
| `dq-stg-tac` | DQ `data/stg/tac.parquet` — TAC, M2M ([checks](documents/dq/stg/dq_stg_tac.md#проверки)) |
| `dq-stg-bs` | DQ `data/stg/bs.parquet` — ключи, интервалы, координаты, геометрия ([checks](documents/dq/stg/dq_stg_bs.md#проверки)) |
| `build-src-bs` | ОКТМО + профиль → `data/src/bs.parquet` |
| `build-src-person` | Суточные срезы → `data/src/person/...` |
| `build-src-excl` | Списки IMSI/IMEI/MSISDN из последнего full snapshot person |
| `build-src-mobile` | CDR / SMS / GPRS / location по дням и операторам |
| `build-stg-event` | CDR/SMS/GPRS/location → `data/stg/event/.../events.parquet` ([doc](documents/stg/build_stg_event.md)); фильтр по `Started`, сортировка по абоненту, сжатие 5m |
| `build-stg-geo-all` | `event_dds` + `stg_bs` → `data/stg/geo_all/{YYYY-MM-DD}.parquet` (без fill через `msisdn-imsi`/`msisdn-imei`) ([doc](documents/stg/build_stg_geo_all.md)) |
| `build-move-event` | `stg/event/{dc}` → `stg/event_dds/{date}/{dc}.parquet` ([doc](documents/stg/build_move_event.md)) |
| `dq-src-mobile` | DQ mobile за отчётную дату; без `--dc` — все дни × оба ЦОД ([checks](documents/dq/src/dq_src_mobile.md#проверки), логи `DQ_SRC_MOBILE`) |
| `dq-src-bs` | DQ всей витрины `src_bs`: распределения, кросс-распределения, контрактные проверки ([checks](documents/dq/src/dq_src_bs.md#проверки), логи `DQ_SRC_BS`) |
| `dq-stg-event` | DQ `event_dds` за `--report-date` и `--event-dds-path` ([checks](documents/dq/stg/dq_stg_event.md#проверки), логи `DQ_STG_EVENT`) |
| `build-stg-msisdn-imsi` | Интервалы MSISDN↔IMSI из `event_dds` ([doc](documents/stg/build_stg_msisdn_imsi.md)) |
| `build-stg-msisdn-imei` | Интервалы MSISDN↔IMEI из `event_dds` ([doc](documents/stg/build_stg_msisdn_imei.md)) |
| `build-stg-bs` | Полный `src_bs` + SCD-merge → `stg_bs` ([doc](documents/stg/build_stg_bs.md)) |
| `nb-perf-metrics` | Notebook-дашборд по `command_timing.jsonl` |

Флаг **`--day YYYY-MM-DD`** — для `build-stg-day` (по умолчанию `2025-01-01`).

Флаг **`--target-per-operator N`** — для `build-src-person` и `build-src` (по умолчанию `50000`).

Флаг **`--excl-pct-of-ab PCT`** — для `build-src-excl` и `build-src` (по умолчанию `0.7` — доля строк АБ в исключениях).

Флаг **`--report-date YYYY-MM-DD`** — для `dq-src-mobile`, `build-stg-event`, `dq-stg-event`, `build-stg-msisdn-*`, `build-move-event`.

Флаг **`--src-bs-path PATH`** — для `build-stg-bs` / `dq-src-bs`: входной `src_bs` (по умолчанию `data/src/bs.parquet`).

Флаг **`--oktmo-path PATH`** — для `build-stg-bs`: справочник ОКТМО (по умолчанию `data/stg/oktmo.parquet`).

Флаг **`--time-zones-path PATH`** — для `build-stg-bs`: справочник часовых поясов (по умолчанию `data/stg/time_zones.parquet`).

Флаг **`--event-dds-path PATH`** — для `dq-stg-event` / `build-stg-msisdn-*`: корень `data/stg/event_dds`, каталог `YYYY-MM-DD` или файл `{dc}.parquet`.

Флаг **`--output-path PATH`** — для `build-stg-msisdn-*` / `build-stg-bs` / `build-stg-geo-all`: выходной Parquet.

Флаг **`--dc`** — для `dq-src-mobile` / `build-stg-event` / `dq-stg-event`: `central` или `far-east`. Без `--dc` — оркестратор по дням и ЦОД. Опционально **`--mobile-root`**.

| Команда | Конфиг / источник | Вход | Выход |
|---------|-------------------|------|-------|
| `build-stg-day` | — | raw CSV (см. ниже) | `data/stg/load_day={day}/*.parquet` |
| `build-stg-oktmo` | — | `src/mobile/raw_data/oktmo_v001.csv` | `data/stg/oktmo.parquet` (snappy) |
| `build-stg-time-zones` | — | `src/mobile/raw_data/time_zones.csv` | `data/stg/time_zones.parquet` (snappy) |
| `build-stg-tac` | — | `src/mobile/raw_data/tacdb_v001.csv` | `data/stg/tac.parquet` (snappy) |
| `dq-stg-oktmo` | — | `data/stg/oktmo.parquet` | логи + `command_timing.jsonl` |
| `dq-stg-time-zones` | — | `data/stg/time_zones.parquet` | логи + timing |
| `dq-stg-tac` | — | `data/stg/tac.parquet` | логи + timing |
| `dq-stg-bs` | — | `data/stg/bs.parquet` | логи + timing |
| `build-stg-event` | `--dc`, `--report-date` | `data/src/mobile/{dc}/operator/...` | `data/stg/event/{YYYY}/{MM}/{DD}/{dc}/events.parquet` |
| `build-stg-geo-all` | `--report-date`, `--event-dds-path`, `--stg-bs-path`, `--output-path` | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet`, `data/stg/bs.parquet` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` |
| `build-move-event` | `--report-date` | `data/stg/event/.../events.parquet` | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet` |
| `dq-src-mobile` | `--dc`, `--report-date` | `data/src/mobile/{dc}/operator/...` | логи `DQ_SRC_MOBILE` + timing |
| `dq-src-bs` | `--src-bs-path` | `data/src/bs.parquet` | логи `DQ_SRC_BS` + timing |
| `dq-stg-event` | `--report-date`, `--event-dds-path`, `--dc` | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet` | логи `DQ_STG_EVENT` + timing |
| `build-stg-msisdn-imsi` | `--report-date`, `--event-dds-path`, `--output-path` | `data/stg/event_dds/{YYYY-MM-DD}/` | `data/stg/msisdn_imsi/{YYYY-MM-DD}.parquet` |
| `build-stg-msisdn-imei` | `--report-date`, `--event-dds-path`, `--output-path` | `data/stg/event_dds/{YYYY-MM-DD}/` | `data/stg/msisdn_imei/{YYYY-MM-DD}.parquet` |
| `build-stg-bs` | `--src-bs-path`, `--oktmo-path`, `--time-zones-path`, `--output-path` | `data/src/bs.parquet`, `data/stg/oktmo.parquet`, `data/stg/time_zones.parquet` | `data/stg/bs.parquet` |
| `build-src-bs` | `data/stg/oktmo.parquet`, профиль OpenCellID | — | `data/src/bs.parquet` |
| `build-src-person` | — | — | `data/src/person/load_year=…/person.parquet`, `_SUCCESS` |
| `build-src-excl` | — | последний `person.parquet` с `_SUCCESS` | `data/src/excl/src_*.parquet` |
| `build-src-mobile` | — | `data/src/bs.parquet`, person с `_SUCCESS` | `data/src/mobile/{central\|far-east}/operator/...` |
| `nb-perf-metrics` | `src/mobile/nb/perf_metrics.ipynb` | `data/qa/command_timing.jsonl` | `data/notebooks/perf_metrics.executed.ipynb` |

Документация:

- [`documents/stg/build_stg_day.md`](documents/stg/build_stg_day.md)
- [`documents/stg/build_stg_oktmo.md`](documents/stg/build_stg_oktmo.md)
- [`documents/stg/build_stg_time_zones.md`](documents/stg/build_stg_time_zones.md)
- [`documents/stg/build_stg_tac.md`](documents/stg/build_stg_tac.md)
- [`documents/stg/build_stg_event.md`](documents/stg/build_stg_event.md)
- [`documents/stg/build_stg_geo_all.md`](documents/stg/build_stg_geo_all.md)
- [`documents/stg/build_move_event.md`](documents/stg/build_move_event.md)
- [`documents/stg/build_stg_msisdn_imsi.md`](documents/stg/build_stg_msisdn_imsi.md)
- [`documents/stg/build_stg_msisdn_imei.md`](documents/stg/build_stg_msisdn_imei.md)
- [`documents/stg/build_stg_bs.md`](documents/stg/build_stg_bs.md)
- [`documents/dq/stg/dq_stg_oktmo.md`](documents/dq/stg/dq_stg_oktmo.md)
- [`documents/dq/stg/dq_stg_time_zones.md`](documents/dq/stg/dq_stg_time_zones.md)
- [`documents/dq/stg/dq_stg_tac.md`](documents/dq/stg/dq_stg_tac.md)
- [`documents/dq/stg/dq_stg_bs.md`](documents/dq/stg/dq_stg_bs.md)
- [`documents/dq/src/dq_src_mobile.md`](documents/dq/src/dq_src_mobile.md)
- [`documents/dq/src/dq_src_bs.md`](documents/dq/src/dq_src_bs.md)
- [`documents/dq/stg/dq_stg_event.md`](documents/dq/stg/dq_stg_event.md)
- [`documents/src/build_src_bs.md`](documents/src/build_src_bs.md)
- [`documents/src/build_src_person.md`](documents/src/build_src_person.md)
- [`documents/src/build_src_excl.md`](documents/src/build_src_excl.md)
- [`documents/src/build_src_mobile.md`](documents/src/build_src_mobile.md)

## Пайплайны (код)

- `src/mobile/pipelines/stg/day.py` — `run()` / `BuildStgDayParams`
- `src/mobile/pipelines/stg/oktmo.py` — `run()`
- `src/mobile/pipelines/stg/time_zones.py` — `run()`
- `src/mobile/pipelines/stg/tac.py` — `run()`
- `src/mobile/pipelines/stg/event.py` — `run_build()`
- `src/mobile/pipelines/stg/geo_all.py` — `run_build()`
- `src/mobile/pipelines/stg/move_event.py` — `run_move()`
- `src/mobile/pipelines/dq/stg/event.py` — `run_dq()`
- `src/mobile/pipelines/stg/msisdn_imsi.py` — `run_build()`
- `src/mobile/pipelines/stg/msisdn_imei.py` — `run_build()`
- `src/mobile/pipelines/stg/bs.py` — `run_build()`
- `src/mobile/pipelines/dq/stg/oktmo.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/time_zones.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/tac.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/bs.py` — `run_dq()`
- `src/mobile/pipelines/dq/src/mobile.py` — `run_dq(dc, report_date, cdr_path, …)`
- `src/mobile/pipelines/dq/src/bs.py` — `run_dq(parquet_path)`
- `src/mobile/pipelines/src/bs.py` — `run()`
- `src/mobile/pipelines/src/person.py` — `run()`
- `src/mobile/pipelines/src/excl.py` — `run()`
- `src/mobile/pipelines/src/mobile.py` — `run_mobile_all()`
- `src/mobile/pipelines/nb/perf_metrics.py` — `run()`
