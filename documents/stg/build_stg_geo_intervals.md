# build-stg-geo-intervals

**Витрина:** `stg_geo_intervals` · **Команда:** `build-stg-geo-intervals` · **Режим:** сборка дневных интервалов пребывания из `stg_geo_all` с дозаполнением `imsi/imei`.

Референс: [`pipelines/stg/geo_intervals.py`](../../src/mobile/pipelines/stg/geo_intervals.py). Схема витрины: [`geo_intervals.json`](../../src/mobile/schema/stg/geo_intervals.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_geo_all`, `stg_bs`, `stg_time_zones` | Входные DataFrame |
| 2 | Дозаполнить `imsi/imei` из `stg_msisdn_imsi`/`stg_msisdn_imei` | Подготовленные события |
| 3 | Сформировать интервалы по методике AGG_GEO_INTERVALS | Интервалы с `cgi_list` |
| 4 | Обогатить timezone и `time_key` | Готовая витрина |
| 5 | Записать parquet и метрики timing | Дневной файл и запись в `command_timing` |

**Бизнес-назначение:** получить устойчивые интервалы пребывания абонентов (не отдельные события) для аналитики треков и перемещений.

**В scope задач:** дозаполнение subscriber-id из binding-витрин, агрегация 5-минутных окон, фильтры indoor/outdoor, merge соседних интервалов, гео-точка интервала и timezone.

---

## TODO

1. Добавить DQ-команду `dq-stg-geo-intervals`.
2. Добавить метрики качества интервалов (длина, число cgi в интервале, доля merged).

---

## Параметры запуска

Вызов: `run_build(report_date, stg_geo_all_path, stg_bs_path, time_zones_path, output_path)` ([`cli.py`](../../src/mobile/cli.py) → `build-stg-geo-intervals`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | Отчётный день |
| `stg_geo_all_path` | path | Нет | `data/stg/geo_all/{report_date}.parquet` | Входной `stg_geo_all` (файл или каталог) |
| `stg_bs_path` | path | Нет | `data/stg/bs.parquet` | Входной `stg_bs` lookup |
| `time_zones_path` | path | Нет | `data/stg/time_zones.parquet` | Полигоны часовых поясов |
| `stg_msisdn_imsi_path` | path | Нет | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | Месячный binding (1-е число месяца `report_date`) |
| `stg_msisdn_imei_path` | path | Нет | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Месячный binding |
| `output_path` | path | Нет | `data/stg/geo_intervals/{report_date}.parquet` | Выходной parquet |

Локальный запуск:

```bash
uv run mobile build-stg-geo-intervals --report-date 2025-01-01
uv run mobile build-stg-geo-intervals --report-date 2025-01-01 --stg-geo-all-path data/stg/geo_all
uv run mobile build-stg-geo-intervals --report-date 2025-01-01 --output-path data/stg/geo_intervals/2025-01-01.parquet
```

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_geo_intervals` — [`geo_intervals.json`](../../src/mobile/schema/stg/geo_intervals.json) |
| Формат хранения | Parquet |
| Календарный срез | `report_date` |
| Сжатие | `snappy` |

### Поля витрины

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | long | MSISDN абонента |
| 2 | `imsi` | long | IMSI абонента |
| 3 | `imei` | long | IMEI абонента |
| 4 | `start_time_utc` | timestamp | Начало интервала |
| 5 | `end_time_utc` | timestamp | Конец интервала |
| 6 | `cgi_list` | list<string> | Отсортированный список CGI интервала |
| 7 | `sub_lat` | double | Оценочная широта абонента |
| 8 | `sub_lon` | double | Оценочная долгота абонента |
| 9 | `bs_type` | string | Тип БС интервала |
| 10 | `timezone` | int | Смещение к UTC в часах |
| 11 | `oktmo_code_1` | string | Доминирующий ОКТМО-1 |
| 12 | `oktmo_code_2` | string | Доминирующий ОКТМО-2 |
| 13 | `time_key` | date | Календарный день партиции |

---

## Источники витрины

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `stg_geo_all` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | События с координатами/CGI |
| 2 | `stg_bs` | `data/stg/bs.parquet` | Центроиды и fallback timezone |
| 3 | `stg_time_zones` | `data/stg/time_zones.parquet` | Point-in-polygon timezone |
| 4 | `stg_msisdn_imsi` | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | Дозаполнение `imsi` (месячный срез) |
| 5 | `stg_msisdn_imei` | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Дозаполнение `imei` |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Разрешить входные/выходные пути.
2. Проверить существование `stg_geo_all`, `stg_bs`, `stg_time_zones`.
3. Подготовить счетчики timing.

### Шаг 1. Чтение и подготовка источников

1. Прочитать `stg_geo_all` за `report_date`.
2. Прочитать `stg_bs` и `stg_time_zones`.
3. Собрать подготовленные timezone-геометрии (`prepared geometry`) для point-in-polygon.

### Шаг 2. Дозаполнение `imsi/imei`

1. Прочитать binding-витрины `stg_msisdn_imsi` и `stg_msisdn_imei`.
2. Нормализовать `msisdn/imsi/imei` в `stg_geo_all` и binding-таблицах.
3. Для строк с пустым `imsi`/`imei` найти запись в binding по:
   - `msisdn`,
   - `valid_from <= start_time_utc <= valid_to`.
4. При нескольких совпадениях выбрать запись с максимальным `valid_from` (самое свежее соответствие).

### Шаг 3. Нормализация событий и 5-минутная агрегация

1. Привести `start_time_utc`/`end_time_utc` к timestamp.
2. Нормализовать `cgi`, координаты, `event_count`, `bs_type`.
3. Округлить `start_time_utc` до 5-минутного bucket.
4. Агрегировать `event_count` по ключу события+БС.

### Шаг 4. Indoor/Outdoor фильтрация и коррекция окон

1. Если в 5m-окне есть indoor-БС, удалить outdoor-строки этого окна.
2. Для outdoor-случаев скорректировать границы окна (`start/end`) по bucket.
3. Для outdoor выполнить отбор по расстоянию Haversine (порог зависит от числа БС в окне).

### Шаг 5. Построение интервалов

1. Сгруппировать строки в интервалы по `(imsi, imei, msisdn, start, end, bs_type)`.
2. Для каждого интервала вычислить `cgi_list` как отсортированный уникальный набор `cgi`.
3. Вычислить `sub_lat/sub_lon` (ядро алгоритма):
   - вес каждой строки: `w_i = total_events_count_i`;
   - суммарный вес: `W = sum(w_i)`;
   - `lat_i/lon_i` — координаты БС (`bs_lat/bs_lon`);
   - `lat_o/lon_o` — «геометрические» координаты для outdoor:
     - берутся `centroid_lat/centroid_lon` из `stg_bs`,
     - если centroid отсутствует, fallback на `bs_lat/bs_lon`;
   - взвешенные средние:
     - `sub_lat_indoor = sum(lat_i * w_i) / W`,
     - `sub_lon_indoor = sum(lon_i * w_i) / W`,
     - `sub_lat_outdoor = sum(lat_o * w_i) / W`,
     - `sub_lon_outdoor = sum(lon_o * w_i) / W`;
   - выбор финальной точки:
     - если `bs_type == "o"` (outdoor) → `sub_lat/sub_lon = sub_lat_outdoor/sub_lon_outdoor`,
     - иначе → `sub_lat/sub_lon = sub_lat_indoor/sub_lon_indoor`.
4. Важные нюансы по `sub_lat/sub_lon`:
   - более «тяжелые» записи (с большим `event_count`) сильнее влияют на точку;
   - после фильтрации и merge соседних интервалов `sub_lat/sub_lon` не пересчитываются для объединенного интервала, используется точка базового интервала-группы;
   - если `W == 0` (практически не ожидается из-за clip `event_count >= 1`), координаты становятся `NaN`.
5. Определить доминирующие `oktmo_code_1/oktmo_code_2`:
   - агрегировать вес `total_events_count` по парам `(oktmo_code_1, oktmo_code_2)`,
   - выбрать пару с максимальным весом.
6. Слить соседние похожие интервалы:
   - днём gap <= 5 мин,
   - ночью gap <= 30 мин.

### Шаг 6. Timezone и финализация

1. Определить `timezone` по `sub_lon/sub_lat` через `stg_time_zones`.
2. Если полигон не найден — fallback timezone из первой БС интервала (`stg_bs`).
3. Заполнить `time_key=report_date`.
4. Вывести строго контрактные поля и записать parquet.
5. Записать метрики в `command_timing.jsonl`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError: stg_geo_all parquet not found` | Нет входного geo_all за день |
| `FileNotFoundError: stg_bs/time_zones parquet not found` | Не подготовлены справочники |
| Пустой выходной parquet | Нет валидных событий для построения интервалов |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/stg/geo_intervals.json`](../../src/mobile/schema/stg/geo_intervals.json) |
| ETL | [`src/mobile/pipelines/stg/geo_intervals.py`](../../src/mobile/pipelines/stg/geo_intervals.py) |
| Источник событий | [`documents/stg/build_stg_geo_all.md`](./build_stg_geo_all.md) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
