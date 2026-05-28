# dq-stg-bs

**Витрина:** `stg_bs` · **Команда:** `dq-stg-bs` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/bs.py`](../../../src/mobile/pipelines/dq/stg/bs.py). Контракт: [`bs.json`](../../../src/mobile/schema/stg/bs.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_bs` parquet | DataFrame витрины |
| 2 | Проверить схему, ключи, интервалы, координаты, геометрию | Логи `DQ_STG_BS` |
| 3 | Сформировать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества исторической витрины БС после `build-stg-bs`.

---

## TODO

1. Добавить исторические тренды quality-метрик (по дням/версиям витрины).
2. Подключить визуальный отчёт с динамикой warning/failed checks.

---

## Параметры запуска

Вызов: `run_dq(parquet_path)` → `dq-stg-bs`.

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `parquet_path` | string (path) | Да | `data/stg/bs.parquet` | Историческая витрина `stg_bs` |

```bash
uv run mobile dq-stg-bs
```

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Путь по умолчанию | `data/stg/bs.parquet` |
| Контракт полей | `STG_BS_FIELDS` из `pipelines/stg/bs.py` |
| Формат | Parquet |
| Проверяемый слой | Историческая SCD-витрина БС |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `stg_bs` | `data/stg/bs.parquet` | Источник для DQ checks |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Разрешить `parquet_path` относительно корня проекта.
2. Загрузить ожидаемый контракт колонок из `STG_BS_FIELDS`.
3. Инициализировать счетчики `total/warning/failed`.

### Шаг 1. Проверка наличия и чтение

1. Проверить существование parquet.
2. Если файл отсутствует:
   - зафиксировать `dataset_presence=failed`,
   - вывести `summary`,
   - завершить DQ.
3. Если файл есть — прочитать DataFrame и зафиксировать `dataset_basic`.

### Шаг 2. Контракт и базовые профили

1. Проверить `schema_columns` (все обязательные поля контракта присутствуют).
2. Для каждой доступной колонки контракта рассчитать:
   - `nulls.<field>`,
   - `cardinality.<field>`.
3. Сформировать baseline-профиль, который используется для интерпретации остальных checks.

### Шаг 3. Ключи и интервальная целостность

1. `key_presence`: null в `(mcc,mnc,lac,cell_id)` недопустимы.
2. `key_uniqueness_per_snapshot`: дубликаты ключа `(mcc,mnc,lac,cell_id,date_on)` помечаются warning.
3. `temporal_consistency`:
   - проверить `date_off >= date_on`,
   - дополнительно посчитать долю «открытых» строк с `date_off = 2262-04-11`.

### Шаг 4. Доменные и геометрические проверки

1. `coords_range`: диапазоны широты/долготы.
2. `bs_type_vocab`: допустимые типы БС `{m,f,i,x,o}`.
3. `telecomstandard_vocab`: допустимые стандарты `{2G,3G,4G}`.
4. `geometry.sector_wkt` и `geometry.mapinfo_wkt`:
   - парсинг WKT,
   - проверка типа геометрии,
   - проверка топологии и пустых геометрий.

### Шаг 5. Финализация

1. Зафиксировать `summary` с итоговыми счетчиками.
2. Вернуть итоговый статус:
   - `failed`, если есть хотя бы один failed-check;
   - `warning`, если failed нет, но есть warning;
   - `ok`, если все проверки прошли без warning.

### Типовые ошибки

| Ошибка/ситуация | Поведение |
|-----------------|-----------|
| `parquet_path` не существует | `dataset_presence=failed` |
| Поврежденный parquet | исключение чтения на этапе запуска команды |
| Невалидные WKT в геометрии | warning в `geometry.*` |

---

## Проверки

Статусы: **ok** / **warning** / **failed**.

| Check | Статус при сбое | Смысл |
|-------|------------------|-------|
| `dataset_presence` | **failed** | Нет parquet |
| `schema_columns` | **failed** | Нет колонок из контракта `STG_BS_FIELDS` |
| `nulls.*`, `cardinality.*` | **ok** | Профили полей |
| `key_presence` | **failed** | Есть строки с null в `(mcc,mnc,lac,cell_id)` |
| `key_uniqueness_per_snapshot` | **warning** | Дубли по `(mcc,mnc,lac,cell_id,date_on)` |
| `temporal_consistency` | **failed** | `date_off < date_on`; плюс метрика открытых строк (`date_off=2262-04-11`) |
| `coords_range` | **warning** | Координаты вне диапазона |
| `bs_type_vocab` | **warning** | `bs_type` не из `{m,f,i,x,o}` |
| `telecomstandard_vocab` | **warning** | `telecomstandard` не из `{2G,3G,4G}` |
| `geometry.sector_wkt` | **warning** | Невалидная/пустая/неподдерживаемая геометрия |
| `geometry.mapinfo_wkt` | **warning** | Невалидная/пустая/неподдерживаемая геометрия |
| `summary` | **ok** | `total_checks`, `warning_checks`, `failed_checks` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`bs.json`](../../../src/mobile/schema/stg/bs.json) |
| ETL build | [`pipelines/stg/bs.py`](../../../src/mobile/pipelines/stg/bs.py) |
| DQ | [`pipelines/dq/stg/bs.py`](../../../src/mobile/pipelines/dq/stg/bs.py) |
