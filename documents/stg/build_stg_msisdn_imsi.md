# build-stg-msisdn-imsi

**Витрина:** `stg_msisdn_imsi` · **Команда:** `build-stg-msisdn-imsi` · **Режим:** месячный parquet с **ежедневным** инкрементом из `stg_geo_all`.

Референс: [`msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py), [`binding_intervals.py`](../../src/mobile/pipelines/stg/binding_intervals.py). Схема: [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_geo_all` за **отчётный день** `report_date` | События за сутки |
| 2 | Построить суточные интервалы MSISDN↔IMSI | `valid_from` / `valid_to` в пределах дня |
| 3 | Убрать из месячного файла старый вклад этого дня | Идемпотентность |
| 4 | Склеить с остальными днями месяца и записать | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` |

**Бизнес-назначение:** накопительная месячная картина привязки MSISDN↔IMSI для [`build-stg-person`](./build_stg_person.md) и fill в [`build-stg-geo-intervals`](./build_stg_geo_intervals.md).

**В scope:** один запуск = один календарный день; файл витрины — **один на месяц** (ключ пути `YYYY-MM-01`).

---

## TODO

1. DQ `dq-stg-msisdn-imsi`.
2. Маркер «последний обновлённый день» в метриках / sidecar (опционально).

---

## Параметры запуска

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да* | — | **Любой день** месяца (`2025-01-15` → пишет в `2025-01-01.parquet`) |
| `stg_geo_all_path` | path | Нет | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | Geo за этот день |
| `output_path` | path | Нет | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | Месячный файл |

\* Без `--report-date` — цикл по `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` (каждый день обновляет свой месяц).

**Предусловие:** `build-stg-geo-all` за тот же день.

```bash
# обновить январь 2025 по мере появления geo_all:
uv run mobile build-stg-msisdn-imsi --report-date 2025-01-01
uv run mobile build-stg-msisdn-imsi --report-date 2025-01-02
# …

# пересобрать весь месяц из geo (устаревшее имя команды):
uv run mobile build-stg-msisdn-imsi-month --report-date 2025-01-01
```

Логи: `command=build-stg-msisdn-imsi` или `build-stg-msisdn-imsi-{date}`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Таблица | `stg_msisdn_imsi` |
| Файл | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` |
| Обновление | Идемпотентно по дню: повторный запуск за тот же `report_date` перезаписывает вклад дня |
| Сжатие | `snappy` |

### Поля

| Поле | Смысл |
|------|-------|
| `msisdn` | Нормализованный MSISDN |
| `imsi` | Нормализованный IMSI |
| `valid_from` | Начало интервала (может быть раньше текущего дня после склейки) |
| `valid_to` | Конец интервала |

---

## Источники

| Источник | Путь |
|----------|------|
| `stg_geo_all` (день) | `data/stg/geo_all/{YYYY-MM-DD}.parquet` |
| Месячный файл (чтение перед merge) | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` |

---

## Алгоритм обработки данных

Точка входа: `run_build(report_date)` → `_run_build` в [`msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py).

### Шаг 0. Инициализация

1. `day_start` / `day_end` = границы календарного `report_date` (00:00:00 … 23:59:59).
2. `output_path` = `stg_msisdn_imsi_output_path(report_date)` → **всегда** `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` (`report_month_start`).
3. Разрешение `stg_geo_all_path`: файл `geo_all/{report_date}.parquet` или `{каталог}/{report_date}.parquet`.
4. Загрузка контракта колонок из [`msisdn_imsi.json`](../../src/mobile/schema/stg/msisdn_imsi.json).

### Шаг 1. Чтение `stg_geo_all`

1. `pd.read_parquet` с колонками `msisdn`, `imsi`, `start_time_utc`.
2. Если файла нет — warning, пустой DataFrame (суточных интервалов не будет).
3. Метрика `geo_rows_read`.

### Шаг 2. Подготовка событий (`_prepare_pair_events`)

1. `event_ts` ← `pd.to_datetime(start_time_utc)`.
2. `msisdn` ← [`normalize_msisdn`](../../src/mobile/pipelines/stg/subscriber_ids.py).
3. `imsi` ← [`normalize_imsi`](../../src/mobile/pipelines/stg/subscriber_ids.py).
4. Оставить строки, где все три поля not null.
5. Метрика `event_rows_with_pair`.

### Шаг 3. Суточные интервалы (`_build_temporal_intervals`)

Для каждого `msisdn` (сортировка по `event_ts`):

1. Идти по событиям; при смене `imsi` закрыть сегмент `[valid_from, valid_to]` предыдущего значения.
2. Внутри сегмента: `valid_from` = min `event_ts`, `valid_to` = max `event_ts`.
3. Обрезка сегмента: `valid_from >= day_start`, `valid_to <= day_end`.
4. Отбросить интервалы с `valid_from > valid_to`.
5. Метрика `day_interval_rows` после `_coerce_output`.

### Шаг 4. Инкремент в месячный parquet (`upsert_daily_into_month_parquet`)

Реализация: [`binding_intervals.py`](../../src/mobile/pipelines/stg/binding_intervals.py).

1. **Снять старый вклад дня** (`drop_intervals_overlapping_day`):
   - из существующего month-файла удалить строки, где `valid_from <= day_end` и `valid_to >= day_start`;
   - это делает повторный прогон за тот же день идемпотентным.
2. **Объединить:** `combined = existing_without_day ∪ day_intervals`.
3. **Склейка** (`merge_binding_intervals`):
   - группировка `(msisdn, imsi)`;
   - сортировка по `valid_from`;
   - смежные сегменты, если `next.valid_from <= prev.valid_to + 1s`, сливаются в один интервал.
4. Нормализация и запись в `month_path` (snappy).
5. Метрика `month_interval_rows`, `distinct_msisdn`.

### Шаг 5. Потребители

| Потребитель | Как использует month-файл |
|-------------|---------------------------|
| [`build-stg-geo-intervals`](./build_stg_geo_intervals.md) | fill `imsi` по `msisdn` и `start_time_utc ∈ [valid_from, valid_to]` |
| [`build-stg-person`](./build_stg_person.md) | рёбра графа `msisdn↔imsi`, если интервал пересекает отчётный месяц |

Полная пересборка месяца: `build-stg-msisdn-imsi-month` → `refresh_month_bindings_from_geo` (цикл по дням с `stg_geo_all`).

### Типовые ситуации

| Ситуация | Поведение |
|----------|-----------|
| Нет geo за день | warning; day_intervals пуст; month не меняется по этому дню |
| Первый день месяца | создаётся month-файл из суточных интервалов |
| Повторный запуск за день | идемпотентно (снятие вклада дня + merge) |
| Смена IMSI внутри дня | несколько суточных сегментов; после merge month могут склеиться с соседними днями |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py) |
| Merge / refresh | [`binding_intervals.py`](../../src/mobile/pipelines/stg/binding_intervals.py) |
| IMEI | [`build_stg_msisdn_imei.md`](./build_stg_msisdn_imei.md) |
| Person | [`build_stg_person.md`](./build_stg_person.md) |
