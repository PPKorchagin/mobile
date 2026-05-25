# dq-stg-oktmo

**Витрина:** `stg_oktmo` · **Команда:** `dq-stg-oktmo` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/oktmo.py`](../../../src/mobile/pipelines/dq/stg/oktmo.py). Схема (контракт): [`oktmo.json`](../../../src/mobile/schema/stg/oktmo.json).

---

## Задачи pipeline


| #   | Задача                                                       | Результат                                |
| --- | ------------------------------------------------------------ | ---------------------------------------- |
| 1   | Прочитать parquet по пути из CLI                             | DataFrame витрины                        |
| 2   | Выполнить проверки по полям `STG_OKTMO_FIELDS` (как в build) | JSON-строки в лог с тегом `DQ_STG_OKTMO` |
| 3   | Итог `summary`                                               | Счётчики checks                          |


**Бизнес-назначение:** контроль качества справочника ОКТМО после `build-stg-oktmo`.

**В scope задач:** наличие файла, колонки из `fields`, null/cardinality, level 1–2, коды, иерархия parent↔code, WKT.

---

## TODO

1. При необходимости ужесточить пороги (failed вместо warning).
2. Связать с notebook-визуализацией DQ (если перенесём nb из geo).

---

## Параметры запуска

Вызов: `run_dq(parquet_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-stg-oktmo`).


| Переменная     | Тип           | Обязательность | Значение по умолчанию    | Описание                                                                                  |
| -------------- | ------------- | -------------- | ------------------------ | ----------------------------------------------------------------------------------------- |
| `parquet_path` | string (path) | Да             | `data/stg/oktmo.parquet` | `DEFAULT_STG_OKTMO_OUTPUT_PATH` в [`project_paths.py`](../../../src/mobile/project_paths.py) |


Флагов CLI **нет** (путь задаётся в CLI из дефолта, как у `build-stg-oktmo` → `output_path`).

**Схема полей в runtime:** `STG_OKTMO_FIELDS` в [`pipelines/stg/oktmo.py`](../../../src/mobile/pipelines/stg/oktmo.py); JSON [`oktmo.json`](../../../src/mobile/schema/stg/oktmo.json) — только контракт документации.

**Предусловие:** `uv run mobile build-stg-oktmo`.

Локальный запуск:

```bash
uv run mobile dq-stg-oktmo
```

Логи: `data/logs/mobile.log` (тег `DQ_STG_OKTMO`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-stg-oktmo`.

---

## Структура проверяемой витрины


| Свойство    | Значение                                                                                                                    |
| ----------- | --------------------------------------------------------------------------------------------------------------------------- |
| Имя таблицы | `stg_oktmo`                                                                                                                 |
| Формат      | Parquet                                                                                                                     |
| Поля        | `WKT`, `level`, `parent_code`, `code`, `name` — `STG_OKTMO_FIELDS` / [`oktmo.json`](../../../src/mobile/schema/stg/oktmo.json) |


---

## Источники


| #   | Источник    | Путь                                      | Назначение |
| --- | ----------- | ----------------------------------------- | ---------- |
| 1   | Витрина STG | `data/stg/oktmo.parquet` (`parquet_path`) | Объект DQ  |


---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `resolved = _resolve_parquet_path(parquet_path)` (относительно `PROJECT_ROOT`).
2. `expected_columns` — имена из `STG_OKTMO_FIELDS`.

### Шаг 1. Наличие данных

Если parquet отсутствует: один check `dataset_presence` (**failed**), `summary`, **return** (exit code CLI = 0).

### Шаг 2. Базовые проверки

1. `dataset_basic` — число строк и колонок.
2. `schema_columns` — **failed**, если нет колонок из `fields`.
3. Для каждого поля: `nulls.{field}`, `cardinality.{field}` (status **ok**).

### Шаг 3. Предметные проверки


| Check                 | Условие warning/failed                                                                   |
| --------------------- | ---------------------------------------------------------------------------------------- |
| `level_distribution`  | **warning** — `level` не в {1, 2}                                                        |
| `code_quality`        | **warning** — дубли или нечисловой `code`                                                |
| `parent_code_quality` | **warning** — нечисловой `parent_code`                                                   |
| `hierarchy_integrity` | **warning** — level1 с parent, level2 без parent, разрывы parent↔child                   |
| `name_quality`        | **warning** — пустые/мусорные `name`                                                     |
| `wkt_geometry`        | **warning** — parse/topology/empty/unsupported type (`POLYGON`/`MULTIPOLYGON` допустимы) |


WKT: построчно `shapely.wkt.loads` в `_collect_wkt_metrics`.

### Шаг 4. Итог

`summary` с агрегатами; return dict со `status`, `parquet_path`, счётчиками checks.

Каждый check — JSON в лог: `{"tag":"DQ_STG_OKTMO","check":"...","status":"...","metrics":{...}}`.

### Типовые ошибки


| Ошибка             | Причина                                               |
| ------------------ | ----------------------------------------------------- |
| Отсутствие parquet | `dataset_presence` failed, процесс завершается штатно |
| pandas / pyarrow   | Повреждённый parquet                                  |


---

## Ссылки


| Артефакт | Путь |
|----------|------|
| Схема | [`oktmo.json`](../../../src/mobile/schema/stg/oktmo.json) |
| ETL build | [`pipelines/stg/oktmo.py`](../../../src/mobile/pipelines/stg/oktmo.py) |
| DQ | [`pipelines/dq/stg/oktmo.py`](../../../src/mobile/pipelines/dq/stg/oktmo.py) |
| Пути | [`project_paths.py`](../../../src/mobile/project_paths.py) |


