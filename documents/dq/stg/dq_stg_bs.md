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

1. **`key_presence`:** для `mcc`, `mnc`, `lac`, `cell_id` — `null_count` должен быть 0; иначе **failed**.
2. **`key_uniqueness_per_snapshot`:** в срезе с одинаковым `date_on` не должно быть двух строк с одним `(mcc,mnc,lac,cell_id)`; `duplicate_key_count` → **warning**.
3. **`temporal_consistency`:**
   - для всех строк `date_off >= date_on`;
   - `inverted_interval_count` → **failed** при > 0.
4. **`temporal_open_ratio`:** доля строк с `date_off` = открытый конец SCD (`2262-04-11` / `_OPEN_END_TS`); **warning**, если доля вне ожидаемого диапазона.
5. **Пересечения интервалов** (опционально): для одного CGI два открытых интервала — **warning**.

### Шаг 4. Доменные и геометрические проверки

1. **`coords_range`:** `lat ∈ [-90,90]`, `lon ∈ [-180,180]`; `out_of_range_count` → **warning**/**failed**.
2. **`bs_type_vocab`:** `bs_type ∈ {m,f,i,x,o}`; неизвестные → **failed**.
3. **`telecomstandard_vocab`:** `{2G,3G,4G}`; иначе **warning**.
4. **`geometry.sector_wkt`:**
   - построчный `shapely.wkt.loads`;
   - тип POLYGON / MULTIPOLYGON / GEOMETRYCOLLECTION (по политике);
   - `sector_wkt_area > 0` где задано.
5. **`geometry.mapinfo_wkt`:** те же проверки для ячеек Вороного;
   - `mapinfo_wkt_centroid_*` в допустимых координатах.
6. **`oktmo_codes`:** непустые `oktmo_code_1/2` для доли БС (операционный порог).

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
