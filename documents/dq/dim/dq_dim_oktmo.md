# dq-dim-oktmo

**Витрина:** `dim_oktmo` · **Команда:** `dq-dim-oktmo` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: `[pipelines/dq/dim/oktmo.py](../../../src/mobile/pipelines/dq/dim/oktmo.py)`. Схема (контракт): `[oktmo.json](../../../src/mobile/schema/dim/oktmo.json)`.

---

## Задачи pipeline


| #   | Задача                                                       | Результат                                |
| --- | ------------------------------------------------------------ | ---------------------------------------- |
| 1   | Прочитать parquet по пути из CLI                             | DataFrame витрины                        |
| 2   | Выполнить проверки по полям `DIM_OKTMO_FIELDS` (как в build) | JSON-строки в лог с тегом `DQ_DIM_OKTMO` |
| 3   | Итог `summary`                                               | Счётчики checks                          |


**Бизнес-назначение:** контроль качества справочника ОКТМО после `build-dim-oktmo`.

**В scope задач:** наличие файла, колонки из `fields`, null/cardinality, level 1–2, коды, иерархия parent↔code, WKT.

---

## TODO

1. При необходимости ужесточить пороги (failed вместо warning).

---

## Параметры запуска

Вызов: `run_dq(oktmo_path)` (`[cli.py](../../../src/mobile/cli.py)` → `dq-dim-oktmo`).


| Переменная   | Тип           | Обязательность | Значение по умолчанию    | Описание                |
| ------------ | ------------- | -------------- | ------------------------ | ----------------------- |
| `oktmo_path` | string (path) | Да             | `data/dim/oktmo.parquet` | CLI: `**--oktmo-path`** |


```bash
uv run mobile dq-dim-oktmo
uv run mobile dq-dim-oktmo --oktmo-path data/dim/oktmo.parquet
```

**Схема полей в runtime:** `DIM_OKTMO_FIELDS` в `[pipelines/dim/oktmo.py](../../../src/mobile/pipelines/dim/oktmo.py)`; JSON `[oktmo.json](../../../src/mobile/schema/dim/oktmo.json)` — только контракт документации.

**Предусловие:** `uv run mobile build-dim-oktmo`.

Локальный запуск:

```bash
uv run mobile dq-dim-oktmo
```

Логи: `data/logs/mobile.log` (тег `DQ_DIM_OKTMO`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-dim-oktmo`.

---

## Структура проверяемой витрины


| Свойство    | Значение                                                                                                                       |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Имя таблицы | `dim_oktmo`                                                                                                                    |
| Формат      | Parquet                                                                                                                        |
| Поля        | `WKT`, `level`, `parent_code`, `code`, `name` — `DIM_OKTMO_FIELDS` / `[oktmo.json](../../../src/mobile/schema/dim/oktmo.json)` |


---

## Источники


| #   | Источник    | Путь                                    | Назначение |
| --- | ----------- | --------------------------------------- | ---------- |
| 1   | Витрина STG | `data/dim/oktmo.parquet` (`oktmo_path`) | Объект DQ  |


---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `resolved = _resolve_oktmo_path(oktmo_path)` (относительно `PROJECT_ROOT`).
2. `expected_columns` — имена из `DIM_OKTMO_FIELDS`.

### Шаг 1. Наличие данных

Если parquet отсутствует: один check `dataset_presence` (**failed**), `summary`, **return** (exit code CLI = 0).

### Шаг 2. Базовые проверки

1. `dataset_basic` — число строк и колонок.
2. `schema_columns` — **failed**, если нет колонок из `fields`.
3. Для каждого поля: `nulls.{field}`, `cardinality.{field}` (status **ok**).

### Шаг 3. Предметные проверки

Последовательный проход по DataFrame (после `read_parquet`):

1. `**level_distribution`:** `level ∈ {1, 2}`; иначе **warning** + `level_counts`, `invalid_level_count`.
2. `**code_quality`:** `code` числовой и уникален в пределах файла; дубликаты / нечисловые → **warning**.
3. `**parent_code_quality`:** числовой `parent_code` где задан.
4. `**hierarchy_integrity`:**
  - level=1 не должен иметь `parent_code`;
  - level=2 должен иметь `parent_code`, существующий среди `code` level=1;
  - у каждого parent level=1 — хотя бы один child level=2 (опционально warning).
5. `**name_quality`:** пустые, `-`, `null` в `name` → **warning**.
6. `**wkt_geometry`:** построчно `shapely.wkt.loads(WKT)`:
  - `parse_error_count`, `invalid_topology_count`, `empty_geometry_count`;
  - `unsupported_geom_type_count` (не POLYGON/MULTIPOLYGON);
  - **warning** при любом ненулевом счётчике (кроме допустимых типов).

### Шаг 4. Итог

`summary` с агрегатами; return dict со `status`, `oktmo_path`, счётчиками checks.

Каждый check — JSON в лог: `{"tag":"DQ_DIM_OKTMO","check":"...","status":"...","metrics":{...}}`.

### Типовые ошибки


| Ошибка             | Причина                                               |
| ------------------ | ----------------------------------------------------- |
| Отсутствие parquet | `dataset_presence` failed, процесс завершается штатно |
| pandas / pyarrow   | Повреждённый parquet                                  |


---

## Проверки

Статусы: **ok** / **warning** / **failed** (кроме метрик `nulls.`* и `cardinality.`* — всегда **ok**).

### Наличие и схема


| Check              | Статус при сбое | Смысл                                                                                     | Обоснование                                                        |
| ------------------ | --------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `dataset_presence` | **failed**      | Parquet по `oktmo_path` не найден; дальнейшие checks не выполняются                       | Без файла витрины DQ и downstream-пайплайны не имеют входа         |
| `dataset_basic`    | **ok**          | `row_count`, `column_count`, `oktmo_path`                                                 | Фиксация объёма среза для сравнения прогонов и пустого справочника |
| `schema_columns`   | **failed**      | Отсутствуют колонки из `DIM_OKTMO_FIELDS` (`WKT`, `level`, `parent_code`, `code`, `name`) | Контракт колонок совпадает с ETL и ожиданиями geo-джойнов          |


### По каждому полю схемы

Для каждого присутствующего поля из `DIM_OKTMO_FIELDS`:


| Check                 | Статус | Метрики                    | Обоснование                                          |
| --------------------- | ------ | -------------------------- | ---------------------------------------------------- |
| `nulls.{field}`       | **ok** | `null_count`, `null_ratio` | Доля пропусков по полю контракта                     |
| `cardinality.{field}` | **ok** | `nunique`                  | Число distinct значений — профиль полноты и выбросов |


### Предметные checks


| Check                 | Статус при сбое | Смысл / метрики                                                                                                                                                                                                             | Обоснование                                         |
| --------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| `level_distribution`  | **warning**     | `level` не в {1, 2}; `level_counts`, `invalid_level_count`                                                                                                                                                                  | В STG только субъекты РФ (1) и муниципалитеты (2)   |
| `code_quality`        | **warning**     | Дубли `code` или нечисловой `code`; `duplicate_code_count`, `non_numeric_code_count`                                                                                                                                        | `code` — ключ справочника для джойнов               |
| `parent_code_quality` | **warning**     | Нечисловой `parent_code`; `non_numeric_parent_code_count`                                                                                                                                                                   | Корректный формат ссылок в иерархии                 |
| `hierarchy_integrity` | **warning**     | level1 с parent, level2 без parent, child без parent в справочнике, parent без children; счётчики по уровням                                                                                                                | Согласованность parent↔child для агрегаций по ОКТМО |
| `name_quality`        | **warning**     | Пустые / `-` / `null` в `name`; `invalid_name_count`                                                                                                                                                                        | Читаемые наименования для отчётов и UI              |
| `wkt_geometry`        | **warning**     | WKT (`shapely.wkt.loads` построчно): `parse_error_count`, `invalid_topology_count`, `empty_geometry_count`, `unsupported_geom_type_count` (допустимы `POLYGON`, `MULTIPOLYGON`), `geom_type_counts`, `valid_geometry_count` | Геометрия нужна для point-in-polygon и карт         |


### Итог


| Check     | Смысл                                             | Обоснование                         |
| --------- | ------------------------------------------------- | ----------------------------------- |
| `summary` | `total_checks`, `warning_checks`, `failed_checks` | Сводка прогона для мониторинга и CI |


---

## Ссылки


| Артефакт  | Путь                                                                         |
| --------- | ---------------------------------------------------------------------------- |
| Схема     | `[oktmo.json](../../../src/mobile/schema/dim/oktmo.json)`                    |
| ETL build | `[pipelines/dim/oktmo.py](../../../src/mobile/pipelines/dim/oktmo.py)`       |
| DQ        | `[pipelines/dq/dim/oktmo.py](../../../src/mobile/pipelines/dq/dim/oktmo.py)` |
| Пути      | `[project_paths.py](../../../src/mobile/project_paths.py)`                   |


