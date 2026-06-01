# build-dim-time-zones

**Витрина:** `dim_time_zones` · **Команда:** `build-dim-time-zones` · **Режим:** полная перезапись одного Parquet-файла.

Референс: `[pipelines/stg/time_zones.py](../../src/mobile/pipelines/stg/time_zones.py)`. Схема витрины: `[time_zones.json](../../src/mobile/schema/dim/time_zones.json)`.

---

## Задачи pipeline


| #   | Задача                                                         | Результат                                       |
| --- | -------------------------------------------------------------- | ----------------------------------------------- |
| 1   | Загрузить сырой CSV справочника тайм-зон                       | Данные в памяти по чанкам                       |
| 2   | Привести набор колонок и типы к целевой схеме `dim_time_zones` | Нормализованный DataFrame                       |
| 3   | Записать витрину в Parquet с заданным сжатием                  | Файл `output_path`, готовый к чтению downstream |


**Бизнес-назначение:** справочник тайм-зон по регионам на STG-слое. Колонка `geometry` — WKT-строка; парсинг геометрии на STG не выполняется.

**В scope задач:** чтение CSV, маппинг 1:1, приведение типов, запись Parquet.

---

## TODO

1. Проверить актуальность справочника тайм-зон относительно ОКТМО/регионов.
2. Автоматизировать загрузку от внешнего поставщика, если такого удастся найти.

---

## Параметры запуска

Переменные, передаваемые в job (аргументы `time_zones.run()`).


| Переменная    | Тип           | Обязательность | Значение по умолчанию                | Описание                               |
| ------------- | ------------- | -------------- | ------------------------------------ | -------------------------------------- |
| `csv_path`    | string (path) | Да             | `src/mobile/raw_data/time_zones.csv` | Входной CSV (CLI `--csv-path`)         |
| `output_path` | string (path) | Да             | `data/dim/time_zones.parquet`        | Выходной Parquet (CLI `--output-path`) |


Пути **относительные к корню репозитория** `mobile`, если не заданы абсолютные (в коде: `PROJECT_ROOT`).

**Константы ETL в коде** (`[time_zones.py](../../src/mobile/pipelines/stg/time_zones.py)`, на вход job **не передаются**):


| Константа                     | Значение                                                                                      |
| ----------------------------- | --------------------------------------------------------------------------------------------- |
| `DIM_TIME_ZONES_TABLE`        | `dim_time_zones`                                                                              |
| `DIM_TIME_ZONES_FIELDS`       | порядок и типы колонок (см. `[time_zones.json](../../src/mobile/schema/dim/time_zones.json)`) |
| `CSV_SEP`                     | `;`                                                                                           |
| `CSV_ENCODING`                | `utf-8`                                                                                       |
| `CSV_CHUNK_SIZE`              | `200000`                                                                                      |
| `SOURCE_MAPPING_COLUMNS`      | `code`, `name`, `timezone`, `geometry` → те же имена (1:1)                                    |
| `DEFAULT_PARQUET_COMPRESSION` | `snappy` — сжатие Parquet при записи                                                          |


Локальный запуск референса:

```bash
uv run mobile build-dim-time-zones
uv run mobile build-dim-time-zones --csv-path src/mobile/raw_data/time_zones.csv --output-path data/dim/time_zones.parquet
```

---

## Структура генерируемой витрины


| Свойство                       | Значение                                                                                      |
| ------------------------------ | --------------------------------------------------------------------------------------------- |
| Имя таблицы                    | `dim_time_zones` — `[time_zones.json](../../src/mobile/schema/dim/time_zones.json)` → `table` |
| Описание                       | Справочник тайм-зон по регионам — `description` в JSON                                        |
| Формат хранения                | Parquet                                                                                       |
| Партиционирование              | Нет                                                                                           |
| Календарный срез / `load_date` | Нет (актуальный snapshot)                                                                     |
| Сжатие                         | `DEFAULT_PARQUET_COMPRESSION` (`snappy`)                                                      |


### Поля витрины

Контракт полей — `[time_zones.json](../../src/mobile/schema/dim/time_zones.json)` → `fields`; в ETL дублируется в `DIM_TIME_ZONES_FIELDS` (`[time_zones.py](../../src/mobile/pipelines/stg/time_zones.py)`).


| #   | Поле       | Тип    | Nullable | Смысл                                       |
| --- | ---------- | ------ | -------- | ------------------------------------------- |
| 1   | `code`     | int32  | Да*      | Код региона                                 |
| 2   | `name`     | string | Да*      | Наименование региона                        |
| 3   | `timezone` | int32  | Да*      | Смещение от UTC в часах                     |
| 4   | `geometry` | string | Да*      | Геометрия региона в WKT; не парсится на STG |


 Nullable после cast: для `int32` нечисловые значения → null; строки — как в CSV.

### Ожидаемый объём (эталон `time_zones.csv`)

~**86** строк, **4** колонки. Использовать для sanity-check после деплоя.

---

## Источники витрины

Единственный внешний источник — **сырой CSV тайм-зон**.


| Атрибут       | Значение                                                   |
| ------------- | ---------------------------------------------------------- |
| Путь          | `src/mobile/raw_data/time_zones.csv` (параметр `csv_path`) |
| Происхождение | Справочник тайм-зон по регионам                            |
| Формат        | CSV: разделитель `;`, UTF-8, первая строка — заголовок     |
| Чтение        | Потоково, `chunksize = 200000`                             |


**Обязательные колонки в CSV** (точные имена):


| Колонка    | Описание           |
| ---------- | ------------------ |
| `code`     | Код региона        |
| `name`     | Наименование       |
| `timezone` | Смещение UTC, часы |
| `geometry` | Геометрия WKT      |


Прочие колонки CSV в витрину **не попадают**.

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Проверить существование `csv_path`; иначе `FileNotFoundError`.
2. Взять целевую схему из `DIM_TIME_ZONES_FIELDS` (согласована с `[time_zones.json](../../src/mobile/schema/dim/time_zones.json)`, чтение JSON в runtime не выполняется).
3. Разрешить пути относительно `PROJECT_ROOT` при необходимости.

### Шаг 1. Чтение источника

```
FOR EACH chunk IN read_csv(csv_path, sep=';', encoding='utf-8', chunksize=200000):
    обработать chunk (шаг 2)
```

### Шаг 2. Нормализация чанка

1. Проверка: все колонки из `SOURCE_MAPPING_COLUMNS` присутствуют; иначе `ValueError`.
2. Переименование 1:1 в имена витрины (`timezone`, `utc_offset`, `geometry`, …).
3. Отбор и упорядочивание по `DIM_TIME_ZONES_FIELDS`.
4. Приведение типов по схеме JSON:
  - `string` → pandas `string`;
  - `int32` / `int64` → nullable integer;
  - `float64` → float;
  - `bool` → boolean.
5. **geometry (WKT):**
  - trim строки WKT;
  - опциональная валидация через `shapely.wkt.loads` (невалидные — log/warning в build, строгий DQ — в `[dq_dim_time_zones](../dq/dim/dq_dim_time_zones.md)`);
  - допустимые типы для downstream point-in-polygon: `POLYGON`, `MULTIPOLYGON`.
6. **utc_offset:** numeric, ожидаемый диапазон часовых сдвигов (DQ: [-12, 14]).

### Шаг 3. Сборка и запись

1. `pd.concat` всех чанков (`ignore_index=True`).
2. `output_path.parent.mkdir(parents=True, exist_ok=True)`.
3. `to_parquet(output_path, compression=snappy, index=False)` — полная перезапись файла.
4. Используется в `[build-fct-bs](../fct/build_fct_bs.md)` и `[build-fct-geo-intervals](../fct/build_fct_geo_intervals.md)` для timezone по координатам.

### Типовые ошибки


| Ошибка              | Причина                                              |
| ------------------- | ---------------------------------------------------- |
| `FileNotFoundError` | Нет CSV                                              |
| `ValueError`        | Нет обязательной колонки / неподдерживаемый тип поля |
| pandas / pyarrow    | Повреждённый CSV, сбой записи                        |


---

## Ссылки


| Артефакт          | Путь                                                                                     |
| ----------------- | ---------------------------------------------------------------------------------------- |
| Схема витрины     | `[src/mobile/schema/dim/time_zones.json](../../src/mobile/schema/dim/time_zones.json)`   |
| ETL               | `[src/mobile/pipelines/stg/time_zones.py](../../src/mobile/pipelines/stg/time_zones.py)` |
| Пути по умолчанию | `[src/mobile/project_paths.py](../../src/mobile/project_paths.py)`                       |


