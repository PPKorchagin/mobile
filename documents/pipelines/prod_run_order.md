# Порядок запуска пайплайнов (прод)

Описание последовательности и зависимостей шагов в production-контуре.

Референс CLI: [`cli.py`](../../src/mobile/cli.py) (`BUILD_PROD_STAGE1_*`, `BUILD_PROD_STAGE2_*`, `BUILD_PROD_PERSON_*`, `run_build_prod_stage1`, `run_build_prod_stage2`, `run_build_prod_person`).

---

## Stage 1 — mobile → DDS

**Расписание:** **ежедневно** за отчётный календарный день `T` (после поступления src-mobile витрин за `T`).

**Команда-обёртка:** `uv run mobile build-prod-stage1 --report-date YYYY-MM-DD`

Опционально: `--dc` (один ЦОД вместо всех), `--mobile-root`, пути витрин (`--cdr-path`, …), `--output-path` для `build-dds-event`.

### Шаги (порядок фиксирован)

| # | Команда | Режим в проде | Описание |
| --- | ------- | ------------- | -------- |
| 1 | [`dq-src-mobile`](../dq/src/dq_src_mobile.md) | автоматически (× ЦОД) | DQ mobile-витрин за `T` |
| 2 | [`build-dds-event`](../dds/build_dds_event.md) | автоматически (× ЦОД) | Сборка `dds_event` за `T` |
| 3 | [`build-dds-move-event`](../dds/build_dds_move_event.md) | **вручную** | Перенос в DDS-layout: на проде выполняет **поставщик**, не планировщик |

> **Важно:** [`build-dds-move-event`](../dds/build_dds_move_event.md) в боевом контуре **не** входит в ежедневное автоматическое расписание. Локально и в dev/test обёртка `build-prod-stage1` может выполнить все три шага подряд (заглушка переноса). В проде после шагов 1–2 дождаться ручной доставки файлов поставщиком; шаг 3 запускать отдельно при необходимости сверки в dev или не вызывать из cron вовсе.

### Пример (один день, все ЦОД)

```bash
uv run mobile build-prod-stage1 --report-date 2025-01-15
```

Эквивалент по шагам (без обёртки): для каждого ЦОД [`dq-src-mobile`](../dq/src/dq_src_mobile.md) → [`build-dds-event`](../dds/build_dds_event.md), затем (только dev/ручной прод) [`build-dds-move-event`](../dds/build_dds_move_event.md) `--report-date 2025-01-15`.

**Предусловие stage 2:** завершён stage 1 за `T` (в т.ч. DDS-layout `event_dds` за `T` после ручного [`build-dds-move-event`](../dds/build_dds_move_event.md) на проде).

---

## Stage 2 — справочники, fct_bs, geo и binding

**Расписание:** **ежедневно** за отчётный календарный день `T` (после stage 1 и появления `data/dds/event_dds/{T}/`).

**Команда-обёртка:** `uv run mobile build-prod-stage2 --report-date YYYY-MM-DD`

Передаёт `--report-date` и пути по умолчанию в шаги, которым нужен явный прогон за день. Справочники ОКТМО / time zones, [`dq-src-bs`](../dq/src/dq_src_bs.md) и [`build-fct-bs`](../fct/build_fct_bs.md) в начале цепочки выполняются с дефолтными путями. Опционально можно переопределить `--csv-path`, `--oktmo-path`, `--time-zones-path`, `--src-bs-path`, `--fct-bs-path`, `--event-dds-path`, `--stg-geo-all-path`, layout binding-витрин и т.д. (те же флаги, что у одиночных команд).

### Шаги (порядок фиксирован)

| # | Команда | За день `T` | Описание |
| --- | ------- | ----------- | -------- |
| 1 | [`build-dim-oktmo`](../dim/build_dim_oktmo.md) | — | Справочник ОКТМО |
| 2 | [`dq-dim-oktmo`](../dq/dim/dq_dim_oktmo.md) | — | DQ ОКТМО |
| 3 | [`build-dim-time-zones`](../dim/build_dim_time_zones.md) | — | Справочник часовых поясов |
| 4 | [`dq-dim-time-zones`](../dq/dim/dq_dim_time_zones.md) | — | DQ time zones |
| 5 | [`dq-src-bs`](../dq/src/dq_src_bs.md) | — | DQ справочника `src_bs` |
| 6 | [`build-fct-bs`](../fct/build_fct_bs.md) | — | Витрина `fct_bs` из `src_bs` |
| 7 | [`dq-fct-bs`](../dq/fct/dq_fct_bs.md) | — | DQ `fct_bs` |
| 8 | [`dq-dds-event`](../dq/dds/dq_dds_event.md) | да | DQ DDS-среза `event_dds` |
| 9 | [`build-stg-geo-all`](../stg/build_stg_geo_all.md) | да | Дневная `stg_geo_all` |
| 10 | [`dq-stg-geo-all`](../dq/stg/dq_stg_geo_all.md) | да | DQ `stg_geo_all` |
| 11 | [`build-fct-msisdn-imei`](../fct/build_fct_msisdn_imei.md) | да (месяц) | Добавление интервалов MSISDN–IMEI за `T` |
| 12 | [`dq-fct-msisdn-imei`](../dq/fct/dq_fct_msisdn_imei.md) | да (месяц) | DQ `fct_msisdn_imei` |
| 13 | [`build-fct-msisdn-imsi-operator`](../fct/build_fct_msisdn_imsi_operator.md) | да (месяц) | Добавление интервалов MSISDN–IMSI за `T` |
| 14 | [`dq-fct-msisdn-imsi-operator`](../dq/fct/dq_fct_msisdn_imsi_operator.md) | да (месяц) | DQ `fct_msisdn_imsi` |
| 15 | [`build-fct-geo-intervals`](../fct/build_fct_geo_intervals.md) | да | Дневная `fct_geo_intervals` |
| 16 | [`dq-fct-geo-intervals`](../dq/fct/dq_fct_geo_intervals.md) | да | DQ `fct_geo_intervals` |

### Пример

```bash
uv run mobile build-prod-stage2 --report-date 2025-01-15
```

**Предусловие person:** за отчётный месяц `M` накоплены дневные/месячные витрины из stage 2 ([`stg_geo_all`](../stg/build_stg_geo_all.md), binding IMEI/IMSI, [`fct_geo_intervals`](../fct/build_fct_geo_intervals.md) и т.д.).

---

## Person — TAC, ОКСМ, профиль физлиц

**Расписание:** **1-го числа каждого месяца** — обработка данных за **предыдущий** календарный месяц `M` (`M` = `YYYY-MM-01` … последний день месяца).

**Команда-обёртка:** `uv run mobile build-prod-person`

Без `--report-date` берётся предыдущий календарный месяц относительно даты запуска (типичный cron: `0 2 1 * *`). Явно: `uv run mobile build-prod-person --report-date 2025-01-01` — январь 2025.

Опционально: `--csv-path`, `--tac-path`, `--oksm-path`, `--src-person-path`, layout binding-витрин, `--fct-person-path`, пути excl (как у [`build-fct-person`](../fct/build_fct_person.md) / [`dq-src-person`](../dq/src/dq_src_person.md)).

### Шаги (порядок фиксирован)

| # | Команда | Период | Описание |
| --- | ------- | ------ | -------- |
| 1 | [`build-dim-tac`](../dim/build_dim_tac.md) | — | Справочник TAC |
| 2 | [`dq-dim-tac`](../dq/dim/dq_dim_tac.md) | — | DQ TAC |
| 3 | [`build-dim-oksm`](../dim/build_dim_oksm.md) | — | Справочник ОКСМ |
| 4 | [`dq-dim-oksm`](../dq/dim/dq_dim_oksm.md) | — | DQ ОКСМ |
| 5 | [`dq-src-excl`](../dq/src/dq_src_excl.md) | — | DQ списков исключений (IMSI, IMEI, MSISDN) |
| 6 | [`dq-src-person`](../dq/src/dq_src_person.md) | месяц `M` | DQ `src_person` за `[M .. конец M]` |
| 7 | [`build-fct-person`](../fct/build_fct_person.md) | месяц `M` | Сборка `fct_person` за `M` |
| 8 | [`dq-fct-person`](../dq/fct/dq_fct_person.md) | месяц `M` | DQ `fct_person` |

### Примеры

Cron 1-го числа (январь за декабрь при запуске 2025-01-01):

```bash
uv run mobile build-prod-person
```

Явный месяц:

```bash
uv run mobile build-prod-person --report-date 2025-01-01
```
