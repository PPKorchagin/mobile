# Список команд и параметров


| Номер команды | Команда              | Описание                                             | Параметры                                                                               | Ссылка на документацию                            | Пайплайн run-all | Пайплайн build-src |
| ------------- | -------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------- | ---------------- | ------------------ |
| 1             | build-stg-oktmo      | Генерация справочника ОКМО                           | --csv-path src/mobile/raw_data/oktmo_v001.csv --output-path data/stg/oktmo.parquet      | [Документ](documents/stg/build_stg_oktmo.md)      | ✓                | ✓                  |
| 2             | dq-stg-oktmo         | Проверка качества сгенерированного справочника ОКТМО | --oktmo-path data/stg/oktmo.parquet                                                     | [Документ](documents/dq/stg/dq_stg_oktmo.md)      | ✓                |                    |
| 3             | nb-stg-oktmo         | Визуализация метрик DQ проверок и справочника ОКТМО  | —                                                                                       | —                                                 | ✓                |                    |
| 4             | build-stg-time-zones | Генерация справочника часовых поясов                 | --csv-path src/mobile/raw_data/time_zones.csv --output-path data/stg/time_zones.parquet | [Документ](documents/stg/build_stg_time_zones.md) | ✓                | ✓                  |
| 5             | dq-stg-time-zones    | Проверка качества справочника часовых поясов         | --time-zones-path data/stg/time_zones.parquet                                           | [Документ](documents/dq/stg/dq_stg_time_zones.md) | ✓                |                    |
| 6             | nb-stg-time-zones    | Визуализация метрик DQ и карта таймзон               | —                                                                                       | —                                                 | ✓                |                    |


