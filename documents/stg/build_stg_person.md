# build-stg-person

**Витрина:** `stg_person` · **Команда:** `build-stg-person` · **Режим:** месячный STG-профиль физлиц из `src_person` (ID + демография).

Референс: [`pipelines/stg/person.py`](../../src/mobile/pipelines/stg/person.py). Схема витрины: [`person.json`](../../src/mobile/schema/stg/person.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Принять `report_date` = 1-е число отчётного месяца | `2025-01-01` |
| 2 | Обойти `load_day` за период `01..31`, выбрать max дату с `_SUCCESS` | Parquet `person.parquet` |
| 3 | Прочитать `stg_msisdn_imsi` и `stg_msisdn_imei` | Lookup-таблицы для дозаполнения ID |
| 4 | Исключить M2M по TAC из `stg_tac` (`is_m2m=true`) | Без IoT/M2M-терминалов |
| 5 | Отфильтровать физлиц и интервалы, пересекающие месяц | Подмножество валидных строк |
| 6 | Нормализовать и дозаполнить `msisdn`/`imsi`/`imei` | Единый и более полный набор ID |
| 7 | Построить `person_id`, `gender`, `age`, `citizenship` | Один ряд на физлицо |
| 8 | Записать витрину в Parquet | `data/stg/person/{YYYY-MM-01}.parquet` |

**Бизнес-назначение:** получить стабильный месячный слой персон (идентификатор + демография) для downstream-джойнов с geo и event витринами.

**В scope задач:** чтение последнего `src_person` месяца, исключение M2M по справочнику TAC, фильтрация `client_type=0`, пересечение интервалов с месяцем, binding-fill ID, дедуп по `person_key`, профиль как в `synthetic_data`.

**Предусловие:** [`build-stg-tac`](build_stg_tac.md) → `data/stg/tac.parquet` (иначе M2M-фильтр пропускается с warning).

---

## TODO

1. Добавить DQ-команду `dq-stg-person` (контракт, nulls, пересечения интервалов).
2. Добавить профилирование по операторам и покрытию `src_person` в `command_timing`.

---

## Параметры запуска

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | **Только 1-е число отчётного месяца** (`YYYY-MM-01`, например `2025-01-01`) |
| `src_person_path` | string (path) | Нет | `data/src/person` | Корень layout или явный parquet |
| `stg_msisdn_imsi_path` | string (path) | Нет | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | Binding MSISDN↔IMSI |
| `stg_msisdn_imei_path` | string (path) | Нет | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Binding MSISDN↔IMEI |
| `stg_tac_path` | string (path) | Нет | `data/stg/tac.parquet` | Справочник TAC для исключения M2M |
| `output_path` | string (path) | Нет | `data/stg/person/{YYYY-MM-01}.parquet` | Выходной parquet |

### Выбор `src_person`

Период месяца: с `report_date` (1-е число) по последний день месяца (`2025-01-01` … `2025-01-31`).

Алгоритм в `data/src/person/load_year=YYYY/load_month=MM/load_day=DD/`:

1. перебрать все каталоги `load_day=*` в пределах периода;
2. оставить только те, где есть `_SUCCESS` и `person.parquet`;
3. взять каталог с **максимальной** датой `load_day`;
4. прочитать `person.parquet` из него.

Если подходящего каталога нет — `FileNotFoundError`.

Локальный запуск:

```bash
uv run mobile build-stg-person --report-date 2025-01-01
uv run mobile build-stg-person --report-date 2025-01-01 --src-person-path data/src/person
```

Логи: `data/logs/mobile.log`. Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-person`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_person` |
| Формат хранения | Parquet |
| Партиционирование | Один файл на отчётный месяц (`report_date = YYYY-MM-01`) |
| Сжатие | `snappy` |

### Поля витрины

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `report_date` | date | 1-е число отчётного месяца |
| 2 | `person_id` | string | `prs_` + SHA256(`person_cluster_key`)[:24] |
| 3 | `msisdn` | string | Нормализованный MSISDN |
| 4 | `imsi` | string | Нормализованный IMSI |
| 5 | `imei` | string | Нормализованный IMEI |
| 6 | `gender` | string | `M` / `F` / `U` (по ФИО) |
| 7 | `age` | string | Возраст на начало месяца или `U` |
| 8 | `citizenship` | string | Код страны или `U` |
| 9 | `operator_id` | long | Код оператора из `src_person.operator_Id` |
| 10 | `actually_from` | timestamp | Начало интервала (последняя запись по person) |
| 11 | `actually_to` | timestamp | Конец интервала |

---

## Источники витрины

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `src_person` | `data/src/person/load_year=YYYY/load_month=MM/load_day=*/person.parquet` | Последний срез месяца |
| 2 | `stg_msisdn_imsi` | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | Дозаполнение IMSI/MSISDN |
| 3 | `stg_msisdn_imei` | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Дозаполнение IMEI/MSISDN |
| 4 | `stg_tac` | `data/stg/tac.parquet` | M2M: TAC с `is_m2m=true` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Проверить `report_date.day == 1`.
2. Период: `period_start = report_date`, `period_end = последний день месяца`.
3. Разрешить `output_path` → `data/stg/person/{report_date}.parquet`.
4. Выбрать `src_person`: max `load_day` с `_SUCCESS` в `[period_start, period_end]`.
5. Разрешить binding parquet за `report_date`.
6. Разрешить `stg_tac_path` → `data/stg/tac.parquet`.

### Шаг 1. Чтение

`pd.read_parquet` по выбранному `src_person` и binding-таблицам.

### Шаг 2. Исключение M2M по TAC

1. Из `stg_tac` — множество `tac`, где `is_m2m=true`.
2. TAC абонента = **первые 8 цифр IMEI** (`src_person.imei`).
3. Строки с TAC ∈ множестве M2M **удаляются**; метрика `excluded_m2m_tac_rows`.
4. Если `stg_tac` отсутствует или без колонок `tac`/`is_m2m` — warning, фильтр не применяется.

### Шаг 3. Фильтрация бизнес-правил

1. `client_type == 0` (физлица).
2. Интервал актуальности пересекает отчётный месяц:
   - `month_start = report_month`;
   - `month_end = последний день месяца`;
   - `actually_from <= month_end` и `actually_to >= month_start`.

### Шаг 4. Нормализация и дозаполнение ID

1. Нормализация `msisdn` / `imsi` / `imei`.
2. Binding-fill на срезе `at = конец month_end` (как в synthetic_data):
   - `msisdn ↔ imsi` через `stg_msisdn_imsi`;
   - `msisdn ↔ imei` через `stg_msisdn_imei`;
   - повторный проход после взаимного заполнения.
3. `operator_id` из `operator_Id`, отбор строк с полным ключом.

### Шаг 5. Стабильный `person_id` (кластер идентификаторов)

**Цель:** смена `msisdn` / `imsi` / `imei` не меняет `person_id`, если это одна и та же персона.

1. **Узлы графа:** `msisdn:…`, `imsi:…`, `imei:…`, `bio:фамилия|имя|отчество|дата_рождения|цифры_документа`.
2. **Рёбра (union-find):**
   - на каждой строке `src_person` — все узлы строки в один кластер (co-occurrence + bio);
   - в `stg_msisdn_imsi` / `stg_msisdn_imei` — пары, чей интервал пересекает отчётный месяц.
3. **`person_cluster_key`** = канонический id кластера (лексикографически минимальный узел).
4. **`person_id`** = `prs_` + SHA256(`person_cluster_key`)[:24].
5. Дедуп: одна строка на кластер, с максимальным `actually_from`; в выходе — актуальные `msisdn`/`imsi`/`imei` этой строки.
6. `gender` / `age` / `citizenship` — с выбранной «последней» строки кластера.

**Ограничения:** без ФИО+даты рождения и без связи через bindings/co-occurrence разные наборы ID останутся разными персонами; полные однофамильцы с одной датой рождения теоретически сольются.

### Шаг 6. Финализация и запись

1. `report_date = report_month` (1-е число).
2. Дедуп по `person_id`.
3. Запись parquet.

### Типовые ошибки

| Ошибка/ситуация | Поведение |
|-----------------|-----------|
| `report_date` не 1-е число | `ValueError` / `SystemExit` в CLI |
| Нет `_SUCCESS` за период месяца | `FileNotFoundError` |
| Пустой выход | нет физлиц с валидными ID за месяц |
| Нет `stg_tac` | warning, M2M-фильтр пропущен |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/stg/person.json`](../../src/mobile/schema/stg/person.json) |
| ETL | [`src/mobile/pipelines/stg/person.py`](../../src/mobile/pipelines/stg/person.py) |
| Нормализация ID | [`src/mobile/pipelines/stg/subscriber_ids.py`](../../src/mobile/pipelines/stg/subscriber_ids.py) |
| Пути по умолчанию | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| Источник Person | [`documents/src/build_src_person.md`](../src/build_src_person.md) |
| Справочник TAC | [`build_stg_tac.md`](build_stg_tac.md) |
