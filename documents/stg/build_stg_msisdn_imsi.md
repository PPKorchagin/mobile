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

### Шаг 1. Суточные интервалы

Из `stg_geo_all` за `report_date`: события → сегменты по смене IMSI на MSISDN; обрезка в границах суток.

### Шаг 2. Инкремент в месячный файл

1. `month_path = stg_msisdn_imsi_output_path(report_date)` → всегда `YYYY-MM-01`.
2. Прочитать существующий month parquet (если есть).
3. Удалить строки, **пересекающие** `[00:00 .. 23:59:59]` этого `report_date`.
4. `concat` + `merge_binding_intervals` по `(msisdn, imsi)`.
5. Записать в `month_path`.

### Шаг 3. Потребители

- **geo-intervals** за день `D`: тот же month-файл; fill по `start_time_utc` ∈ `[valid_from, valid_to]`.
- **person** за месяц `M`: month-файл после прогона binding по всем дням (или `refresh_month_bindings_from_geo`).

### Типовые ситуации

| Ситуация | Поведение |
|----------|-----------|
| Нет geo за день | warning, суточных интервалов нет; month без изменений по дню |
| Первый день месяца | создаётся новый month-файл |
| Повторный запуск за день | идемпотентно |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`msisdn_imsi.py`](../../src/mobile/pipelines/stg/msisdn_imsi.py) |
| Merge / refresh | [`binding_intervals.py`](../../src/mobile/pipelines/stg/binding_intervals.py) |
| IMEI | [`build_stg_msisdn_imei.md`](./build_stg_msisdn_imei.md) |
| Person | [`build_stg_person.md`](./build_stg_person.md) |
