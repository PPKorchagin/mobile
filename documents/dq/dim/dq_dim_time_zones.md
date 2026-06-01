# dq-dim-time-zones

**Витрина:** `dim_time_zones` · **Команда:** `dq-dim-time-zones` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: `[pipelines/dq/dim/time_zones.py](../../../src/mobile/pipelines/dq/dim/time_zones.py)`. Контракт полей: `[time_zones.json](../../../src/mobile/schema/dim/time_zones.json)`.

---

## Задачи pipeline


| #   | Задача                                                | Результат                |
| --- | ----------------------------------------------------- | ------------------------ |
| 1   | Прочитать конфиг и путь parquet                       | Целевой файл DQ          |
| 2   | Проверить схему, `code`, `timezone`, WKT в `geometry` | Логи `DQ_DIM_TIME_ZONES` |
| 3   | Итог `summary`                                        | Счётчики checks          |


**Бизнес-назначение:** контроль качества справочника тайм-зон после `build-dim-time-zones`.

**В scope задач:** наличие файла, колонки, null/cardinality, диапазон timezone, геометрия.

---

## TODO

1. При необходимости добавить перекрёстную проверку с `dim_oktmo` в DQ (сейчас только parquet time_zones; в notebook — совместная карта).

---

## Параметры запуска

Вызов: `run_dq(time_zones_path)` (`[cli.py](../../../src/mobile/cli.py)` → `dq-dim-time-zones`).


| Переменная        | Тип           | Обязательность | Значение по умолчанию         | Описание                     |
| ----------------- | ------------- | -------------- | ----------------------------- | ---------------------------- |
| `time_zones_path` | string (path) | Да             | `data/dim/time_zones.parquet` | CLI: `**--time-zones-path`** |


```bash
uv run mobile dq-dim-time-zones
uv run mobile dq-dim-time-zones --time-zones-path data/dim/time_zones.parquet
```

**Схема полей в runtime:** `DIM_TIME_ZONES_FIELDS` в `[pipelines/dim/time_zones.py](../../../src/mobile/pipelines/dim/time_zones.py)`; JSON `[time_zones.json](../../../src/mobile/schema/dim/time_zones.json)` — контракт документации.

**Предусловие:** `uv run mobile build-dim-time-zones`.

Локальный запуск:

```bash
uv run mobile dq-dim-time-zones
```

Логи: `data/logs/mobile.log`. Метрики: `command=dq-dim-time-zones` в `command_timing.jsonl`.

---

## Структура проверяемой витрины


| Свойство    | Значение                                                 |
| ----------- | -------------------------------------------------------- |
| Имя таблицы | `dim_time_zones`                                         |
| Поля        | `code`, `name`, `timezone`, `geometry` — JSON → `fields` |


---

## Источники


| #   | Источник | Путь                          |
| --- | -------- | ----------------------------- |
| 1   | Parquet  | `data/dim/time_zones.parquet` |


---

## Алгоритм обработки данных

### Шаг 0. Инициализация

`_resolve_parquet_path(parquet_path)`; `expected_columns` из `DIM_TIME_ZONES_FIELDS`.

### Шаг 1. Наличие данных

Нет parquet → `dataset_presence` (**failed**), summary, return.

### Шаг 2. Базовые проверки

`dataset_basic`, `schema_columns` (**failed** при missing), `nulls.*`, `cardinality.*`.

### Шаг 3. Предметные проверки

1. `**timezone_quality`:** пустые / дубликаты `timezone` → **warning** с `empty_timezone_count`, `duplicate_timezone_count`.
2. `**utc_offset_range`:** `utc_offset` в [-12, 14]; выход за диапазон → **failed** или **warning** по порогу доли.
3. `**wkt_geometry`:** аналогично ОКТМО — парсинг WKT, тип POLYGON/MULTIPOLYGON, топология; метрики `parse_error_count`, `geom_type_counts`.
4. `**point_in_polygon_sample` (если реализовано):** spot-check нескольких тестовых точек против полигонов.

Детали checks — [Проверки](#проверки).

### Шаг 4. Итог

`summary`; тег логов `DQ_DIM_TIME_ZONES`. Формат строки: `{"tag":"DQ_DIM_TIME_ZONES","check":"...","status":"...","metrics":{...}}`.

### Типовые ошибки


| Ошибка           | Причина                   |
| ---------------- | ------------------------- |
| Нет parquet      | `dataset_presence` failed |
| pandas / pyarrow | Битый parquet             |


---

## Проверки

Статусы: **ok** / **warning** / **failed** (`nulls.*`, `cardinality.*` — всегда **ok**).

### Наличие и схема


| Check              | Статус при сбое | Смысл                                                                                          | Обоснование                                                                 |
| ------------------ | --------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `dataset_presence` | **failed**      | Parquet по `time_zones_path` не найден; дальнейшие checks не выполняются                       | Без файла витрины DQ и downstream (`build-fct-bs`, `build-fct-geo-intervals`) не имеют входа |
| `dataset_basic`    | **ok**          | `row_count`, `column_count`, `time_zones_path`                                                 | Фиксация объёма среза для сравнения прогонов и пустого справочника          |
| `schema_columns`   | **failed**      | Отсутствуют колонки из `DIM_TIME_ZONES_FIELDS` (`code`, `name`, `timezone`, `geometry`)        | Контракт колонок совпадает с ETL и ожиданиями джойнов по региону            |


### По каждому полю схемы

Для каждого присутствующего поля из `DIM_TIME_ZONES_FIELDS`:


| Check                 | Статус | Метрики                    | Обоснование                                          |
| --------------------- | ------ | -------------------------- | ---------------------------------------------------- |
| `nulls.{field}`       | **ok** | `null_count`, `null_ratio` | Доля пропусков по полю контракта                     |
| `cardinality.{field}` | **ok** | `nunique`                  | Число distinct значений — профиль полноты и выбросов |


### Предметные checks


| Check              | Статус при сбое | Смысл / метрики                                                                                                                                                                                          | Обоснование                                                                 |
| ------------------ | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `code_quality`     | **warning**     | Дубли `code` или NaN после `to_numeric`; `duplicate_code_count`, `invalid_code_count`                                                                                                                    | `code` — ключ региона для связи с ОКТМО и локальным временем по БС          |
| `timezone_range`   | **warning**     | `timezone` вне [-12, 14]; `invalid_timezone_count`, `timezone_min`, `timezone_max`, **`distribution`** — доли по значениям timezone (%)                                                                  | Смещение UTC должно быть физически допустимым; distribution — профиль по РФ |
| `geometry_quality` | **warning**     | WKT в `geometry`: `parse_error_count`, `invalid_topology_count`, `empty_geometry_count`, `unsupported_geom_type_count`, `geom_type_counts`, `valid_geometry_count` (допустимы `POLYGON`, `MULTIPOLYGON`) | Полигоны регионов нужны для point-in-polygon и карты в `nb-dim-time-zones`  |


### Итог


| Check     | Смысл                                             | Обоснование                         |
| --------- | ------------------------------------------------- | ----------------------------------- |
| `summary` | `total_checks`, `warning_checks`, `failed_checks` | Сводка прогона для мониторинга и CI |


---

## Ссылки


| Артефакт  | Путь                                                                                   |
| --------- | -------------------------------------------------------------------------------------- |
| Схема     | `[time_zones.json](../../../src/mobile/schema/dim/time_zones.json)`                    |
| ETL build | `[pipelines/dim/time_zones.py](../../../src/mobile/pipelines/dim/time_zones.py)`       |
| DQ        | `[pipelines/dq/dim/time_zones.py](../../../src/mobile/pipelines/dq/dim/time_zones.py)` |
| Пути      | `[project_paths.py](../../../src/mobile/project_paths.py)`                             |


