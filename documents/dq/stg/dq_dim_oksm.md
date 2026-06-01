# dq-dim-oksm

**Витрина:** `dim_oksm` · **Команда:** `dq-dim-oksm` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: `[pipelines/dq/stg/oksm.py](../../../src/mobile/pipelines/dq/stg/oksm.py)`. Контракт: `[oksm.json](../../../src/mobile/schema/dim/oksm.json)`.

---

## Задачи pipeline


| #   | Задача                                        | Результат                               |
| --- | --------------------------------------------- | --------------------------------------- |
| 1   | Прочитать parquet по пути из CLI              | DataFrame витрины                       |
| 2   | Выполнить проверки по полям `DIM_OKSM_FIELDS` | JSON-строки в лог с тегом `DQ_DIM_OKSM` |
| 3   | Итог `summary`                                | Счётчики checks                         |


**Бизнес-назначение:** контроль качества справочника ОКСМ после `build-dim-oksm`.

**В scope задач:** наличие файла, колонки из `fields`, null/cardinality, целостность `numeric_code` / `alpha2` / `alpha3`, наименования, `autokey`, наличие записи RU (`643`).

---

## TODO

1. При обновлении ОКСМ добавить перекрёстную проверку пар `numeric_code` ↔ `alpha2` ↔ `alpha3` против эталона ISO 3166.
2. При необходимости ужесточить пороги (failed вместо warning) для `russia_presence`.

---

## Параметры запуска

Вызов: `run_dq(oksm_path)` (`[cli.py](../../../src/mobile/cli.py)` → `dq-dim-oksm`).


| Переменная  | Тип           | Обязательность | Значение по умолчанию   | Описание               |
| ----------- | ------------- | -------------- | ----------------------- | ---------------------- |
| `oksm_path` | string (path) | Да             | `data/dim/oksm.parquet` | CLI: `**--oksm-path`** |


```bash
uv run mobile dq-dim-oksm
uv run mobile dq-dim-oksm --oksm-path data/dim/oksm.parquet
```

**Схема полей в runtime:** `DIM_OKSM_FIELDS` в `[pipelines/stg/oksm.py](../../../src/mobile/pipelines/stg/oksm.py)`; JSON `[oksm.json](../../../src/mobile/schema/dim/oksm.json)` — контракт документации.

**Константа DQ:** `_RUSSIA_NUMERIC_CODE = "643"`.

**Предусловие:** `uv run mobile build-dim-oksm`.

Локальный запуск:

```bash
uv run mobile dq-dim-oksm
```

Логи: `data/logs/mobile.log` (тег `DQ_DIM_OKSM`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-dim-oksm`.

---

## Структура проверяемой витрины


| Свойство    | Значение                                                                                                                                               |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Имя таблицы | `dim_oksm`                                                                                                                                             |
| Формат      | Parquet                                                                                                                                                |
| Поля        | `numeric_code`, `name_short`, `name_full`, `alpha2`, `alpha3`, `autokey` — `DIM_OKSM_FIELDS` / `[oksm.json](../../../src/mobile/schema/dim/oksm.json)` |


---

## Источники


| #   | Источник    | Путь                                  | Назначение |
| --- | ----------- | ------------------------------------- | ---------- |
| 1   | Витрина STG | `data/dim/oksm.parquet` (`oksm_path`) | Объект DQ  |


---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `resolved = _resolve_oksm_path(oksm_path)` (относительно `PROJECT_ROOT`).
2. `expected_columns` — имена из `DIM_OKSM_FIELDS`.

### Шаг 1. Наличие данных

Если parquet отсутствует: один check `dataset_presence` (**failed**), `summary`, **return** (exit code CLI = 0).

### Шаг 2. Базовые проверки

1. `dataset_basic` — число строк и колонок, `oksm_path`.
2. `schema_columns` — **failed**, если нет колонок из `fields`.
3. Для каждого поля: `nulls.{field}`, `cardinality.{field}` (status **ok**).

### Шаг 3. Предметные проверки

Для каждой проверки — отдельная запись в лог с `tag=DQ_DIM_OKSM`.

1. `**numeric_code_integrity`:** все `numeric_code` match `^\d{3}$`; `duplicate_numeric_code_count` → **failed** при нарушении.
2. `**russia_presence`:** есть строка с `numeric_code = 643`; иначе **warning** (`has_numeric_code_643`).
3. `**alpha2_integrity`:** непустые `alpha2` match `^[A-Z]{2}$`, без дублей; **failed** при нарушении.
4. `**alpha3_integrity`:** непустые `alpha3` match `^[A-Z]{3}$`, без дублей; **failed** при нарушении.
5. `**alpha_pair_cardinality`:** число distinct пар `(alpha2, alpha3)` — метрика `distinct_alpha2_alpha3_pairs`.
6. `**name_quality`:** пустые `name_short` / `name_full` → **failed**.
7. `**autokey_integrity`:** пустой или дублирующийся `autokey` → **failed**.

### Шаг 4. Итог

`summary`; тег `DQ_DIM_OKSM`. Формат строки: `{"tag":"DQ_DIM_OKSM","check":"...","status":"...","metrics":{...}}`.

### Типовые ошибки


| Ошибка           | Причина                   |
| ---------------- | ------------------------- |
| Нет parquet      | `dataset_presence` failed |
| pandas / pyarrow | Битый parquet             |


---

## Проверки

Статусы: **ok** / **warning** / **failed** (`nulls.`*, `cardinality.*` — всегда **ok**).

### Наличие и схема


| Check              | Статус при сбое | Смысл                                                                                                           | Обоснование                                                           |
| ------------------ | --------------- | --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| `dataset_presence` | **failed**      | Parquet по `oksm_path` не найден; дальнейшие checks не выполняются                                              | Без файла витрины DQ и downstream (`build-stg-person`) не имеют входа |
| `dataset_basic`    | **ok**          | `row_count`, `column_count`, `oksm_path`                                                                        | Фиксация объёма среза для сравнения прогонов и пустого справочника    |
| `schema_columns`   | **failed**      | Отсутствуют колонки из `DIM_OKSM_FIELDS` (6 полей, см. `[oksm.json](../../../src/mobile/schema/dim/oksm.json)`) | Контракт колонок совпадает с ETL и `OksmLookup`                       |


### По каждому полю схемы

Для каждого присутствующего поля из `DIM_OKSM_FIELDS`:


| Check                 | Статус | Метрики                    | Обоснование                                          |
| --------------------- | ------ | -------------------------- | ---------------------------------------------------- |
| `nulls.{field}`       | **ok** | `null_count`, `null_ratio` | Доля пропусков по полю контракта                     |
| `cardinality.{field}` | **ok** | `nunique`                  | Число distinct значений — профиль полноты и выбросов |


### Предметные checks


| Check                    | Статус при сбое | Смысл / метрики                                                                                         | Обоснование                                                 |
| ------------------------ | --------------- | ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `numeric_code_integrity` | **failed**      | `numeric_code` не `^\d{3}$` или дубликаты; `invalid_numeric_code_count`, `duplicate_numeric_code_count` | `numeric_code` — ключ страны для `stg_person.citizenship`   |
| `russia_presence`        | **warning**     | Нет записи с кодом `643`; `has_numeric_code_643`                                                        | Россия — дефолт citizenship и основной рынок                |
| `alpha2_integrity`       | **failed**      | Невалидный или дублирующийся alpha-2; `invalid_alpha2_count`, `duplicate_alpha2_count`                  | ISO alpha-2 для lookup по кодам в person                    |
| `alpha3_integrity`       | **failed**      | Невалидный или дублирующийся alpha-3; `invalid_alpha3_count`, `duplicate_alpha3_count`                  | Дополнительная проверка ISO-кодов                           |
| `alpha_pair_cardinality` | **ok**          | Число distinct пар alpha2+alpha3; `distinct_alpha2_alpha3_pairs`                                        | Контроль согласованности пар кодов                          |
| `name_quality`           | **failed**      | Пустые `name_short` / `name_full`; `empty_name_short_count`, `empty_name_full_count`                    | Наименования нужны для `match_country_names` в `OksmLookup` |
| `autokey_integrity`      | **failed**      | Пустой или дублирующийся `autokey`; `duplicate_autokey_count`, `empty_autokey_count`                    | Ключ записи в источнике ОКСМ                                |


### Итог


| Check     | Смысл                                             | Обоснование                         |
| --------- | ------------------------------------------------- | ----------------------------------- |
| `summary` | `total_checks`, `warning_checks`, `failed_checks` | Сводка прогона для мониторинга и CI |


---

## Ссылки


| Артефакт  | Путь                                                                       |
| --------- | -------------------------------------------------------------------------- |
| Схема     | `[oksm.json](../../../src/mobile/schema/dim/oksm.json)`                    |
| ETL build | `[pipelines/stg/oksm.py](../../../src/mobile/pipelines/stg/oksm.py)`       |
| DQ        | `[pipelines/dq/stg/oksm.py](../../../src/mobile/pipelines/dq/stg/oksm.py)` |
| Build doc | `[documents/stg/build_dim_oksm.md](../../stg/build_dim_oksm.md)`           |
| Пути      | `[project_paths.py](../../../src/mobile/project_paths.py)`                 |


