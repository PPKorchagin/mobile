# build-dim-tac

**Витрина:** `dim_tac` · **Команда:** `build-dim-tac` · **Режим:** полная перезапись одного Parquet-файла.

Референс: `[pipelines/stg/tac.py](../../src/mobile/pipelines/stg/tac.py)`. Схема витрины: `[tac.json](../../src/mobile/schema/dim/tac.json)`.

---

## Задачи pipeline


| #   | Задача                                          | Результат                         |
| --- | ----------------------------------------------- | --------------------------------- |
| 1   | Загрузить сырой CSV TACDB                       | DataFrame источника               |
| 2   | Нормализовать TAC, даты, признак `is_m2m`, типы | DataFrame целевой схемы `dim_tac` |
| 3   | Записать витрину в Parquet с заданным сжатием   | Файл `output_path`                |


**Бизнес-назначение:** справочник TAC (Type Allocation Code) для классификации терминалов и M2M/IoT.

**В scope задач:** чтение CSV, нормализация TAC (8 цифр), даты аллокации (`YYYY-MM-DD`), вычисление `is_m2m`, запись Parquet.

---

## TODO

1. Сверить список `M2M_EQUIPMENT_TYPES` с актуальной таксономией GSMA.
2. Периодически обновлять `tacdb_v001.csv` (Osmocom TACDB) или перейти на источник от поставщика.

---

## Параметры запуска

Переменные, передаваемые в job (аргументы `tac.run()`).


| Переменная    | Тип           | Обязательность | Значение по умолчанию                | Описание                               |
| ------------- | ------------- | -------------- | ------------------------------------ | -------------------------------------- |
| `csv_path`    | string (path) | Да             | `src/mobile/raw_data/tacdb_v001.csv` | Входной CSV (CLI `--csv-path`)         |
| `output_path` | string (path) | Да             | `data/dim/tac.parquet`               | Выходной Parquet (CLI `--output-path`) |


Пути **относительные к корню репозитория** `mobile`, если не заданы абсолютные (в коде: `PROJECT_ROOT`).

Сжатие Parquet — константа `DEFAULT_PARQUET_COMPRESSION` в `[cli_defaults.py](../../src/mobile/cli_defaults.py)` (по умолчанию `snappy`); в job **не передаётся**.

**Константы ETL в коде** (`[tac.py](../../src/mobile/pipelines/stg/tac.py)`, на вход job **не передаются**):


| Константа                | Значение                                                                        |
| ------------------------ | ------------------------------------------------------------------------------- |
| `DIM_TAC_TABLE`          | `dim_tac`                                                                       |
| `DIM_TAC_FIELDS`         | порядок и типы колонок (см. `[tac.json](../../src/mobile/schema/dim/tac.json)`) |
| `CSV_SEP`                | `;`                                                                             |
| `CSV_ENCODING`           | `utf-8-sig`                                                                     |
| `SOURCE_MAPPING_COLUMNS` | колонки CSV → витрина (1:1), без `is_m2m`                                       |
| `M2M_EQUIPMENT_TYPES`    | `Module`, `WLAN Router`, `Vehicle Unit`, `IoT Device`, `Modem`, `M2M Module`    |


Локальный запуск референса:

```bash
uv run mobile build-dim-tac
uv run mobile build-dim-tac --csv-path src/mobile/raw_data/tacdb_v001.csv --output-path data/dim/tac.parquet
```

---

## Структура генерируемой витрины


| Свойство                       | Значение                                                                 |
| ------------------------------ | ------------------------------------------------------------------------ |
| Имя таблицы                    | `dim_tac` — `[tac.json](../../src/mobile/schema/dim/tac.json)` → `table` |
| Описание                       | Справочник TAC — `description` в JSON                                    |
| Формат хранения                | Parquet                                                                  |
| Партиционирование              | Нет                                                                      |
| Календарный срез / `load_date` | Нет (актуальный snapshot)                                                |
| Сжатие                         | `DEFAULT_PARQUET_COMPRESSION` (`snappy`)                                 |


### Поля витрины

Контракт полей — `[tac.json](../../src/mobile/schema/dim/tac.json)` → `fields`; в ETL — `DIM_TAC_FIELDS` (`[tac.py](../../src/mobile/pipelines/stg/tac.py)`). Поле `is_m2m` **вычисляется** (`equipment_type ∈ M2M_EQUIPMENT_TYPES`).


| #   | Поле               | Тип    | Смысл                       |
| --- | ------------------ | ------ | --------------------------- |
| 1   | `tac`              | string | TAC, 8 цифр                 |
| 2   | `manufacturer`     | string | Производитель (GSMA)        |
| 3   | `model_name`       | string | Модель                      |
| 4   | `marketing_name`   | string | Коммерческое наименование   |
| 5   | `equipment_type`   | string | Класс оборудования GSMA     |
| 6   | `radio_technology` | string | Радиотехнология             |
| 7   | `sim_form_factor`  | string | Форм-фактор SIM             |
| 8   | `allocation_date`  | string | Дата аллокации `YYYY-MM-DD` |
| 9   | `reporting_body`   | string | Источник записи             |
| 10  | `chipset`          | string | Chipset                     |
| 11  | `comment`          | string | Комментарий                 |
| 12  | `is_m2m`           | bool   | M2M/IoT по `equipment_type` |


### Ожидаемый объём (эталон `tacdb_v001.csv`)

~**22 553** строк, **12** колонок.

---

## Источники витрины

Единственный внешний источник — **сырой CSV TACDB**.


| Атрибут       | Значение                                                                   |
| ------------- | -------------------------------------------------------------------------- |
| Путь          | `src/mobile/raw_data/tacdb_v001.csv` (параметр `csv_path`)                 |
| Происхождение | Osmocom TACDB / выгрузка поставщика                                        |
| Формат        | CSV: разделитель `;`, UTF-8 с BOM (`utf-8-sig`), заголовок в первой строке |
| Чтение        | Целиком в память (объём эталона ~22k строк)                                |


**Обязательные колонки в CSV** (точные имена, без `is_m2m`):

`tac`, `manufacturer`, `model_name`, `marketing_name`, `equipment_type`, `radio_technology`, `sim_form_factor`, `allocation_date`, `reporting_body`, `chipset`, `comment`.

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Проверить существование `csv_path`; иначе `FileNotFoundError`.
2. Схема из `DIM_TAC_FIELDS` (согласована с `[tac.json](../../src/mobile/schema/dim/tac.json)`, JSON в runtime не читается).

### Шаг 1. Чтение источника

```
raw = read_csv(csv_path, sep=';', encoding='utf-8-sig')
```

### Шаг 2. Нормализация

1. Проверка наличия всех колонок из `SOURCE_MAPPING_COLUMNS`; rename 1:1 в имена витрины.
2. **TAC (ключ справочника):**
  - `strip` → оставить только цифры;
  - `zfill(8)` и взять **последние 8** символов (устойчивость к префиксам);
  - валидация `^\d{8}$`; иначе `ValueError` с указанием строки.
3. Строковые поля (`manufacturer`, `model_name`, …) — `strip`; пустые допускаются.
4. **allocation_date:**
  - первичный парсинг `%d.%m.%Y`;
  - fallback `pd.to_datetime(..., dayfirst=True)`;
  - выход — строка `YYYY-MM-DD` или null.
5. **is_m2m (флаг IoT):**
  - `equipment_type ∈ M2M_EQUIPMENT_TYPES` (константа в `[tac.py](../../src/mobile/pipelines/stg/tac.py)`);
6. Приведение типов: строковые колонки → pandas `string`; `is_m2m` → `boolean`.
7. Порядок колонок строго `DIM_TAC_FIELDS`.
8. `duplicated(subset=["tac"]).any()` → `ValueError` (ключ TAC уникален).

### Шаг 3. Запись

`to_parquet(output_path, compression=DEFAULT_PARQUET_COMPRESSION, index=False)` — полная перезапись.

### Типовые ошибки


| Ошибка              | Причина                                                    |
| ------------------- | ---------------------------------------------------------- |
| `FileNotFoundError` | Нет CSV                                                    |
| `ValueError`        | Невалидный TAC, непарсимая дата, дубликат TAC, нет колонки |
| pandas / pyarrow    | Битый CSV, сбой записи                                     |


---

## Ссылки


| Артефакт          | Путь                                                                       |
| ----------------- | -------------------------------------------------------------------------- |
| Схема витрины     | `[src/mobile/schema/dim/tac.json](../../src/mobile/schema/dim/tac.json)`   |
| ETL               | `[src/mobile/pipelines/stg/tac.py](../../src/mobile/pipelines/stg/tac.py)` |
| Пути по умолчанию | `[src/mobile/project_paths.py](../../src/mobile/project_paths.py)`         |


