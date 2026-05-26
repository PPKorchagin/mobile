# DQ (data quality)

Read-only проверки после build-пайплайнов. Логи: `data/logs/mobile.log`, тег `DQ_*`. CLI не завершается с ненулевым exit code при failed checks.

## Команды

| Команда | Витрина / объект | Документация (перечень checks) |
|---------|------------------|--------------------------------|
| `dq-stg-oktmo` | `stg_oktmo` parquet | [`stg/dq_stg_oktmo.md`](stg/dq_stg_oktmo.md#проверки) |
| `dq-stg-time-zones` | `stg_time_zones` parquet | [`stg/dq_stg_time_zones.md`](stg/dq_stg_time_zones.md#проверки) |
| `dq-stg-tac` | `stg_tac` parquet | [`stg/dq_stg_tac.md`](stg/dq_stg_tac.md#проверки) |
| `dq-src-mobile` | CDR / SMS / GPRS / location за отчётную дату и ЦОД | [`src/dq_src_mobile.md`](src/dq_src_mobile.md#проверки) |

`build-stg-day` вызывает три STG-DQ подряд после каждого build — см. [`../stg/build_stg_day.md`](../stg/build_stg_day.md#проверки-dq-в-цепочке).

## Статусы checks

| Статус | Смысл |
|--------|--------|
| **info** / **ok** | Метрика или gate пройден |
| **warning** | Отклонение от ожиданий, run продолжается |
| **failed** | Критичное отклонение (в т.ч. нет файла / нет колонок схемы) |

Итог каждого run: check `summary` с `total_checks`, `warning_checks`, `failed_checks`.
