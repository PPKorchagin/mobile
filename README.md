# Список команд и параметров


| Номер команды | Команда              | Описание                                             | Параметры                                                                               | Ссылка на документацию                            |
| ------------- | -------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------- |
| 1             | build-stg-oktmo      | Генерация справочника ОКМО                           | --csv-path src/mobile/raw_data/oktmo_v001.csv --output-path data/stg/oktmo.parquet      | [Документ](documents/stg/build_stg_oktmo.md)      |
| 2             | dq-stg-oktmo         | Проверка качества сгенерированного справочника ОКТМО | --oktmo-path data/stg/oktmo.parquet                                                     | [Документ](documents/dq/stg/dq_stg_oktmo.md)      |
| 3             | nb-stg-oktmo         | Визуализация метрик DQ проверок и справочника ОКТМО  | —                                                                                       | —                                                 |
| 4             | build-stg-time-zones | Генерация справочника часовых поясов                 | --csv-path src/mobile/raw_data/time_zones.csv --output-path data/stg/time_zones.parquet | [Документ](documents/stg/build_stg_time_zones.md) |
| 5             | dq-stg-time-zones    | Проверка качества справочника часовых поясов         | --time-zones-path data/stg/time_zones.parquet                                           | [Документ](documents/dq/stg/dq_stg_time_zones.md) |
| 6             | nb-stg-time-zones    | Визуализация метрик DQ и карта таймзон               | —                                                                                       | —                                                 |
| 7             | build-stg-tac        | Генерация справочника TAC                            | --csv-path src/mobile/raw_data/tacdb_v001.csv --output-path data/stg/tac.parquet        | [Документ](documents/stg/build_stg_tac.md)        |
| 8             | dq-stg-tac           | Проверка качества справочника TAC                    | --tac-path data/stg/tac.parquet                                                         | [Документ](documents/dq/stg/dq_stg_tac.md)        |
| 9             | nb-stg-tac           | Визуализация метрик DQ и сводка справочника TAC      | —                                                                                       | —                                                 |
| 10            | build-stg-oksm       | Генерация справочника ОКСМ                           | --csv-path src/mobile/raw_data/oksm_v001.csv --output-path data/stg/oksm.parquet        | [Документ](documents/stg/build_stg_oksm.md)       |
| 11            | dq-stg-oksm          | Проверка качества справочника ОКСМ                   | --oksm-path data/stg/oksm.parquet                                                       | [Документ](documents/dq/stg/dq_stg_oksm.md)       |
| 12            | nb-stg-oksm          | Визуализация метрик DQ и сводка справочника ОКСМ     | —                                                                                       | —                                                 |
| 13            | build-src-bs         | Генерация синтетического справочника базовых станций | —                                                                                       | [Документ](documents/src/build_src_bs.md)         |
| 14            | dq-src-bs            | Проверка качества справочника базовых станций        | --src-bs-path data/src/bs.parquet                                                       | [Документ](documents/dq/src/dq_src_bs.md)         |
| 15            | nb-src-bs            | Визуализация метрик DQ и карта базовых станций       | —                                                                                       | —                                                 |

