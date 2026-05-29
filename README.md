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
uv run mobile build-stg-oksm
uv run mobile dq-stg-oktmo
uv run mobile dq-stg-time-zones
uv run mobile dq-stg-tac
uv run mobile dq-stg-oksm
uv run mobile dq-stg-bs
uv run mobile build-src-bs
uv run mobile build-src-person
uv run mobile build-src-excl
uv run mobile build-src-mobile
uv run mobile build-stg-event
uv run mobile build-stg-event --dc central --report-date 2025-01-01
uv run mobile build-stg-geo-all --report-date 2025-01-01
uv run mobile build-stg-geo-intervals --report-date 2025-01-01
uv run mobile build-stg-person --report-date 2025-01-01
uv run mobile build-move-event --report-date 2025-01-01
uv run mobile dq-src-mobile
uv run mobile dq-src-bs
uv run mobile dq-src-mobile --dc central --report-date 2025-01-01
uv run mobile dq-stg-event --dc central --report-date 2025-01-01
uv run mobile dq-stg-geo-all --report-date 2025-01-01
uv run mobile dq-stg-geo-intervals --report-date 2025-01-01
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
| `build-src` | `build-stg-oktmo` → `build-stg-time-zones` → `build-stg-tac` → `build-stg-oksm` → `build-src-bs` → … → `nb-perf-metrics` |
| `build-stg-day` | STG build + DQ за `--day` (по умолчанию `2025-01-01`) → `data/stg/load_day=…/` |
| `build-stg-oktmo` | CSV → `data/stg/oktmo.parquet` |
| `build-stg-time-zones` | CSV → `data/stg/time_zones.parquet` |
| `build-stg-tac` | CSV → `data/stg/tac.parquet` |
| `build-stg-oksm` | CSV → `data/stg/oksm.parquet` ([doc](documents/stg/build_stg_oksm.md)) |
| `dq-stg-oktmo` | DQ `data/stg/oktmo.parquet` — схема, иерархия, WKT ([checks](documents/dq/stg/dq_stg_oktmo.md#проверки)) |
| `dq-stg-time-zones` | DQ `data/stg/time_zones.parquet` — timezone, geometry ([checks](documents/dq/stg/dq_stg_time_zones.md#проверки)) |
| `dq-stg-tac` | DQ `data/stg/tac.parquet` — TAC, M2M ([checks](documents/dq/stg/dq_stg_tac.md#проверки)) |
| `dq-stg-oksm` | DQ `data/stg/oksm.parquet` — коды стран, имена ([checks](documents/dq/stg/dq_stg_oksm.md#проверки)) |
| `dq-stg-bs` | DQ `data/stg/bs.parquet` — ключи, интервалы, координаты, геометрия ([checks](documents/dq/stg/dq_stg_bs.md#проверки)) |
| `build-src-bs` | ОКТМО + профиль → `data/src/bs.parquet` |
| `build-src-person` | Суточные срезы → `data/src/person/...` |
| `build-src-excl` | Списки IMSI/IMEI/MSISDN из последнего full snapshot person |
| `build-src-mobile` | CDR / SMS / GPRS / location по дням и операторам |
| `build-stg-event` | CDR/SMS/GPRS/location → `data/stg/event/.../events.parquet` ([doc](documents/stg/build_stg_event.md)); фильтр по `Started`, сортировка по абоненту, сжатие 5m |
| `build-stg-geo-all` | `event_dds` + `stg_bs` → `data/stg/geo_all/{YYYY-MM-DD}.parquet` (без fill через `msisdn-imsi`/`msisdn-imei`) ([doc](documents/stg/build_stg_geo_all.md)) |
| `build-stg-geo-intervals` | `stg_geo_all` + `stg_bs` + `stg_time_zones` + fill из `stg_msisdn_*` → `data/stg/geo_intervals/{YYYY-MM-DD}.parquet` ([doc](documents/stg/build_stg_geo_intervals.md)) |
| `build-stg-person` | `stg_person` + `stg_person_sim` + ledger ([doc](documents/stg/build_stg_person.md)) |
| `build-stg-msisdn-operator` | MNP-наблюдения MSISDN↔operator ([doc](documents/stg/build_stg_msisdn_operator.md)) |
| `build-stg-msisdn-imsi-month` | *(alias)* пересборка месяца из всех `stg_geo_all` ([doc](documents/stg/build_stg_msisdn_imsi.md)) |
| `build-stg-msisdn-imei-month` | *(alias)* то же для IMSI+IMEI |
| `build-move-event` | `stg/event/{dc}` → `stg/event_dds/{date}/{dc}.parquet` ([doc](documents/stg/build_move_event.md)) |
| `dq-src-mobile` | DQ mobile за отчётную дату; без `--dc` — все дни × оба ЦОД ([checks](documents/dq/src/dq_src_mobile.md#проверки), логи `DQ_SRC_MOBILE`) |
| `dq-src-bs` | DQ всей витрины `src_bs`: распределения, кросс-распределения, контрактные проверки ([checks](documents/dq/src/dq_src_bs.md#проверки), логи `DQ_SRC_BS`) |
| `dq-stg-event` | DQ `event_dds` за `--report-date` и `--event-dds-path` ([checks](documents/dq/stg/dq_stg_event.md#проверки), логи `DQ_STG_EVENT`) |
| `dq-stg-geo-all` | DQ `stg_geo_all` за день (`schema/nulls/ranges/distribution`) ([checks](documents/dq/stg/dq_stg_geo_all.md#проверки), логи `DQ_STG_GEO_ALL`) |
| `dq-stg-geo-intervals` | DQ `stg_geo_intervals` за день (`schema/nulls/ranges/cgi_list/key`) ([checks](documents/dq/stg/dq_stg_geo_intervals.md#проверки), логи `DQ_STG_GEO_INTERVALS`) |
| `build-stg-msisdn-imsi` | Месячный `stg_msisdn_imsi`, ежедневный инкремент из `stg_geo_all` ([doc](documents/stg/build_stg_msisdn_imsi.md)) |
| `build-stg-msisdn-imei` | Месячный `stg_msisdn_imei`, ежедневный инкремент ([doc](documents/stg/build_stg_msisdn_imei.md)) |
| `build-stg-bs` | Полный `src_bs` + SCD-merge → `stg_bs` ([doc](documents/stg/build_stg_bs.md)) |
| `nb-perf-metrics` | Notebook-дашборд по `command_timing.jsonl` ([doc](documents/nb/nb_perf_metrics.md)) |
| `dq-stg-person` | *(план)* DQ `stg_person` / `stg_person_sim` ([spec](documents/dq/stg/dq_stg_person.md)) |

Флаг **`--day YYYY-MM-DD`** — для `build-stg-day` (по умолчанию `2025-01-01`).

Флаг **`--target-per-operator N`** — для `build-src-person` и `build-src` (по умолчанию `50000`).

Флаг **`--excl-pct-of-ab PCT`** — для `build-src-excl` и `build-src` (по умолчанию `0.7` — доля строк АБ в исключениях).

Флаг **`--report-date YYYY-MM-DD`** — для `dq-src-mobile`, `build-stg-event`, `dq-stg-event`, `dq-stg-geo-all`, `dq-stg-geo-intervals`, `build-stg-msisdn-*`, `build-stg-geo-intervals`, `build-move-event`. Для `build-stg-person` — строго **1-е число месяца** (`2025-01-01`).

Флаг **`--src-bs-path PATH`** — для `build-stg-bs` / `dq-src-bs`: входной `src_bs` (по умолчанию `data/src/bs.parquet`).

Флаг **`--src-person-path PATH`** — для `build-stg-person`: входной `src_person` parquet, дневной каталог или корень layout (по умолчанию `data/src/person`).

Флаг **`--stg-tac-path PATH`** — для `build-stg-person`: справочник TAC для исключения M2M SIM (по умолчанию `data/stg/tac.parquet`; нужен `build-stg-tac`).

Флаг **`--oktmo-path PATH`** — для `build-stg-bs`: справочник ОКТМО (по умолчанию `data/stg/oktmo.parquet`).

Флаг **`--time-zones-path PATH`** — для `build-stg-bs` / `build-stg-geo-intervals`: справочник часовых поясов (по умолчанию `data/stg/time_zones.parquet`).

Флаг **`--event-dds-path PATH`** — для `dq-stg-event` / `build-stg-geo-all`: корень `data/stg/event_dds`, каталог `YYYY-MM-DD` или файл `{dc}.parquet`.

Флаг **`--stg-geo-all-path PATH`** — для `build-stg-msisdn-*` / `build-stg-geo-intervals` / `dq-stg-geo-all`: входной `stg_geo_all` parquet или каталог `data/stg/geo_all`.

Флаг **`--stg-geo-intervals-path PATH`** — для `dq-stg-geo-intervals`: входной `stg_geo_intervals` parquet или каталог `data/stg/geo_intervals`.

Флаг **`--stg-msisdn-imsi-path PATH`** — для `build-stg-geo-intervals`: входной `stg_msisdn_imsi` parquet (fill `imsi`).

Флаг **`--stg-msisdn-imei-path PATH`** — для `build-stg-geo-intervals`: входной `stg_msisdn_imei` parquet (fill `imei`).

Флаг **`--output-path PATH`** — для `build-stg-msisdn-*` / `build-stg-bs` / `build-stg-geo-all` / `build-stg-geo-intervals` / `build-stg-person`: выходной Parquet.

Флаг **`--dc`** — для `dq-src-mobile` / `build-stg-event` / `dq-stg-event`: `central` или `far-east`. Без `--dc` — оркестратор по дням и ЦОД. Опционально **`--mobile-root`**.

| Команда | Конфиг / источник | Вход | Выход |
|---------|-------------------|------|-------|
| `build-stg-day` | — | raw CSV (см. ниже) | `data/stg/load_day={day}/*.parquet` |
| `build-stg-oktmo` | — | `src/mobile/raw_data/oktmo_v001.csv` | `data/stg/oktmo.parquet` (snappy) |
| `build-stg-time-zones` | — | `src/mobile/raw_data/time_zones.csv` | `data/stg/time_zones.parquet` (snappy) |
| `build-stg-tac` | — | `src/mobile/raw_data/tacdb_v001.csv` | `data/stg/tac.parquet` (snappy) |
| `build-stg-oksm` | — | `src/mobile/raw_data/oksm_v001.csv` | `data/stg/oksm.parquet` (snappy) |
| `dq-stg-oktmo` | — | `data/stg/oktmo.parquet` | логи + `command_timing.jsonl` |
| `dq-stg-time-zones` | — | `data/stg/time_zones.parquet` | логи + timing |
| `dq-stg-tac` | — | `data/stg/tac.parquet` | логи + timing |
| `dq-stg-oksm` | — | `data/stg/oksm.parquet` | логи + timing |
| `dq-stg-bs` | — | `data/stg/bs.parquet` | логи + timing |
| `build-stg-event` | `--dc`, `--report-date` | `data/src/mobile/{dc}/operator/...` | `data/stg/event/{YYYY}/{MM}/{DD}/{dc}/events.parquet` |
| `build-stg-geo-all` | `--report-date`, `--event-dds-path`, `--stg-bs-path`, `--output-path` | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet`, `data/stg/bs.parquet` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` |
| `build-stg-geo-intervals` | `--report-date`, … | `stg_geo_all` за день, `msisdn_imsi`/`imei` за **месяц** `{YYYY-MM-01}.parquet` | `data/stg/geo_intervals/{YYYY-MM-DD}.parquet` |
| `build-stg-person` | `--report-date` (1-е число месяца), … | `src_person`, `stg_tac`, `msisdn_imsi`/`imei` за месяц, `msisdn_operator`, ledger | `person/`, `person_sim/`, `person_id_ledger/` |
| `build-stg-msisdn-operator` | `--report-date` | все срезы `src_person` месяца | `data/stg/msisdn_operator/{YYYY-MM-01}.parquet` |
| `build-stg-msisdn-imsi` | `--report-date` (день) | `stg_geo_all` за день | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` (инкремент) |
| `build-stg-msisdn-imei` | `--report-date` (день) | `stg_geo_all` за день | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` (инкремент) |
| `build-move-event` | `--report-date` | `data/stg/event/.../events.parquet` | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet` |
| `dq-src-mobile` | `--dc`, `--report-date` | `data/src/mobile/{dc}/operator/...` | логи `DQ_SRC_MOBILE` + timing |
| `dq-src-bs` | `--src-bs-path` | `data/src/bs.parquet` | логи `DQ_SRC_BS` + timing |
| `dq-stg-event` | `--report-date`, `--event-dds-path`, `--dc` | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet` | логи `DQ_STG_EVENT` + timing |
| `dq-stg-geo-all` | `--report-date`, `--stg-geo-all-path` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | логи `DQ_STG_GEO_ALL` + timing |
| `dq-stg-geo-intervals` | `--report-date`, `--stg-geo-intervals-path` | `data/stg/geo_intervals/{YYYY-MM-DD}.parquet` | логи `DQ_STG_GEO_INTERVALS` + timing |
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
- [`documents/stg/build_stg_geo_intervals.md`](documents/stg/build_stg_geo_intervals.md)
- [`documents/stg/build_stg_person.md`](documents/stg/build_stg_person.md)
- [`documents/stg/build_stg_msisdn_operator.md`](documents/stg/build_stg_msisdn_operator.md)
- [`documents/stg/build_move_event.md`](documents/stg/build_move_event.md)
- [`documents/stg/build_stg_msisdn_imsi.md`](documents/stg/build_stg_msisdn_imsi.md)
- [`documents/stg/build_stg_msisdn_imei.md`](documents/stg/build_stg_msisdn_imei.md)
- [`documents/stg/build_stg_bs.md`](documents/stg/build_stg_bs.md)
- [`documents/dq/stg/dq_stg_oktmo.md`](documents/dq/stg/dq_stg_oktmo.md)
- [`documents/dq/stg/dq_stg_time_zones.md`](documents/dq/stg/dq_stg_time_zones.md)
- [`documents/dq/stg/dq_stg_tac.md`](documents/dq/stg/dq_stg_tac.md)
- [`documents/dq/stg/dq_stg_bs.md`](documents/dq/stg/dq_stg_bs.md)
- [`documents/dq/stg/dq_stg_geo_all.md`](documents/dq/stg/dq_stg_geo_all.md)
- [`documents/dq/stg/dq_stg_geo_intervals.md`](documents/dq/stg/dq_stg_geo_intervals.md)
- [`documents/dq/stg/dq_stg_person.md`](documents/dq/stg/dq_stg_person.md) *(спецификация, CLI в разработке)*
- [`documents/nb/nb_perf_metrics.md`](documents/nb/nb_perf_metrics.md)
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
- `src/mobile/pipelines/stg/geo_intervals.py` — `run_build()`
- `src/mobile/pipelines/stg/person.py` — `run_build()`
- `src/mobile/pipelines/stg/person_identity.py` — union-find, ledger
- `src/mobile/pipelines/stg/msisdn_operator.py` — `build_operator_intervals_from_src`
- `src/mobile/pipelines/stg/binding_intervals.py` — merge / daily upsert / `refresh_month_bindings_from_geo`
- `src/mobile/pipelines/stg/src_person_month.py` — `read_src_person_month`
- `src/mobile/pipelines/stg/move_event.py` — `run_move()`
- `src/mobile/pipelines/dq/stg/event.py` — `run_dq()`
- `src/mobile/pipelines/stg/msisdn_imsi.py` — `run_build()`
- `src/mobile/pipelines/stg/msisdn_imei.py` — `run_build()`
- `src/mobile/pipelines/stg/bs.py` — `run_build()`
- `src/mobile/pipelines/dq/stg/oktmo.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/time_zones.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/tac.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/bs.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/geo_all.py` — `run_dq()`
- `src/mobile/pipelines/dq/stg/geo_intervals.py` — `run_dq()`
- `src/mobile/pipelines/dq/src/mobile.py` — `run_dq(dc, report_date, cdr_path, …)`
- `src/mobile/pipelines/dq/src/bs.py` — `run_dq(parquet_path)`
- `src/mobile/pipelines/src/bs.py` — `run()`
- `src/mobile/pipelines/src/person.py` — `run()`
- `src/mobile/pipelines/src/excl.py` — `run()`
- `src/mobile/pipelines/src/mobile.py` — `run_mobile_all()`
- `src/mobile/pipelines/nb/perf_metrics.py` — `run()`
