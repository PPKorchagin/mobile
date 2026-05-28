# build-stg-bs

**Витрина:** `stg_bs` · **Команда:** `build-stg-bs` · **Режим:** ежедневное обновление с сохранением истории (`date_on`/`date_off`) в одном Parquet.

Референс: [`pipelines/stg/bs.py`](../../src/mobile/pipelines/stg/bs.py). Схема витрины: [`bs.json`](../../src/mobile/schema/stg/bs.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать полный `src_bs` | Полный снимок исходного справочника |
| 2 | Маппинг полей src → stg: `mcc`/`mnc`, сектор, H3 | Нормализованный snapshot |
| 3 | Join timezone (полигоны) и ОКТМО (level 1/2) | `timezone`, `oktmo_code_*` |
| 4 | Расчёт `sector_wkt`, MAPINFO (`mapinfo_wkt`, Вороной) | Геометрии покрытия |
| 5 | Записать витрину в Parquet | Файл `output_path` |

**Бизнес-назначение:** STG-справочник базовых станций с координатами, ОКТМО и геометрией сектора для join в геопайплайнах (`stg_geo_all`, интервалы, trace).

**В scope задач:** на каждом прогоне формируется полный снимок, затем выполняется SCD-merge с историей в `output_path`:
- неизменившиеся БС остаются открытыми;
- изменившиеся/исчезнувшие закрываются на `report_date - 1 сек`;
- новые/изменившиеся добавляются с `date_on = report_date`, `date_off = 2262-04-11`.

---

## TODO

1. DQ-витрина `dq-stg-bs`.

---

## Параметры запуска

Переменные, передаваемые в job (аргументы `run_build()` из CLI).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `src_bs_path` | string (path) | Нет | `data/src/bs.parquet` | Входной parquet `src_bs` |
| `oktmo_path` | string (path) | Нет | `data/stg/oktmo.parquet` | Справочник ОКТМО |
| `time_zones_path` | string (path) | Нет | `data/stg/time_zones.parquet` | UTC offset по координатам (point-in-polygon) |
| `output_path` | string (path) | Нет | `data/stg/bs.parquet` | Выходной Parquet (перезапись) |

Пути **относительные к корню репозитория** `mobile` (`resolve_project_path`).

**Константы ETL:**

| Константа | Значение |
|-----------|----------|
| `STG_BS_TABLE` / `STG_BS_FIELDS` | из [`bs.json`](../../src/mobile/schema/stg/bs.json) |
| `_OPEN_END_TS` | `2262-04-11 00:00:00` |
| `DEFAULT_PARQUET_COMPRESSION` | `snappy` |

**Предусловия:** `build-src-bs`, `build-stg-oktmo`, `build-stg-time-zones`.

```bash
uv run mobile build-stg-bs
uv run mobile build-stg-bs \
  --src-bs-path data/src/bs.parquet \
  --oktmo-path data/stg/oktmo.parquet \
  --time-zones-path data/stg/time_zones.parquet \
  --output-path data/stg/bs.parquet
```

Логи: `data/logs/mobile.log`. Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-bs`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_bs` — [`bs.json`](../../src/mobile/schema/stg/bs.json) |
| Формат | Parquet, один исторический файл |
| Календарный срез | SCD Type 2: интервалы `date_on`/`date_off`; `date_on` = timestamp запуска |
| Сжатие | `snappy` |

### Поля витрины

Контракт — [`bs.json`](../../src/mobile/schema/stg/bs.json) → `fields` (28 колонок). Ключевые:

| Поле | Смысл |
|------|--------|
| `mcc`, `mnc`, `lac`, `cell_id` | Идентификаторы соты (join с событиями) |
| `lon`, `lat` | Координаты БС |
| `bs_type` | `m` / `f` / `i` / `x` / `o` |
| `sector_wkt`, `mapinfo_wkt` | WKT сектора и MAPINFO-мозаики |
| `oktmo_code_1`, `oktmo_code_2` | Территория |
| `date_on`, `date_off` | Интервал актуальности STG-снимка |

---

## Источники витрины

| Источник | Путь (по умолчанию) | Назначение |
|----------|---------------------|------------|
| `src_bs` | `data/src/bs.parquet` | MCC/MNC/LAC/cell, координаты, мощность, период `date_on`/`date_off` |
| `stg_oktmo` | `data/stg/oktmo.parquet` | ОКТМО level 1/2, WKT |
| `stg_time_zones` | `data/stg/time_zones.parquet` | timezone по полигонам |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

Проверка наличия входных parquet; загрузка схемы из JSON.

### Шаг 1. Чтение и подготовка `src_bs`

Чтение полного `src_bs` без фильтра по дате. Перед маппингом выполняется **нормализация** всех read-полей (контракт [`src/bs.json`](../../src/mobile/schema/src/bs.json)):

1. **Схема:** отсутствующие read-колонки заполняются `NA`, ошибка пишется в лог.
2. **Типы:** numeric/timestamp поля приводятся к целевому формату; строковые — `strip`.
3. **Диапазоны:** radio/sector-поля клипуются по контракту; `azimuth`/`thickness` — через `_normalize_*`.
4. **Ошибки строк:** если значение не удалось распарсить или оно вне допустимого диапазона координат/времени — `WARNING` в лог с идентификатором строки:
   `cgi={mcc}-{mnc}-{lac}-{cell} date_on=... date_off=...`.

Job **не прерывается**; проблемные значения становятся `NA`, метрики — в `src_bs_prepare` (`error_count`, `error_samples`).
Логируются **все** ошибочные строки (без лимита и без подавления `suppressed`).

| Группа | Поля | Поведение |
|--------|------|-----------|
| Ключ | `mcc`, `mnc`, `lac`, `cell` | numeric coerce; непарсящиеся — log + `NA` |
| Время | `date_on`, `date_off` | timestamp coerce; `date_off < date_on` — log |
| Координаты | `coord_x`, `coord_y` | numeric + `_normalize_lon`; вне диапазона — log + `NA` |
| Радио / сектор | `generation`, `frequency`, `azimuth`, `thickness`, `power`, `height`, `amplification`, `tilt`, `el_tilt`, `mech_tilt` | coerce + clip / normalize |
| Классификация | `bs_type`, `location`, `description`, `subject`, `rad_class` | string strip |
| Прочее | `avtocod` | numeric coerce |

### Шаг 2. Маппинг и дедупликация

`mcc`, `mnc`, `lac`, `cell_id`, `telecomstandard`, сектор, H3; dedup по `(mcc, mnc, lac, cell_id)`.

### Шаг 3. Обогащение

- timezone — point-in-polygon по `stg_time_zones`;
- ОКТМО:
  1. level 2 (`oktmo_code_2`) определяется по координатам БС (point-in-polygon по полигонам level 2);
  2. если level 2 найден, level 1 (`oktmo_code_1`) берётся из `parent_code` найденного level 2;
  3. если level 2 не найден, fallback для level 1: сначала match по `subject`, затем геопоиск по полигонам level 1;
- `sector_wkt` — секторное покрытие;
- MAPINFO — Вороной по группам `(telecomstandard, frequency)`.

#### Детализация `sector_wkt`

1. Для каждой БС берутся `lon`, `lat`, `sector_azimuth`, `sector_angle`, `sector_radius`.
2. Радиус сектора (`sector_radius`) вычисляется по `bs_type`:
   - `m`: `0.2` км, `i`: `0.1` км, `f`: `0.05` км, остальные: `5.0` км.
3. Геометрия сектора:
   - для `m/i/f` и широкого луча (`angle >= 360`) строится круг;
   - для остальных строится сектор (wedge) по азимуту и углу;
   - для `o` добавляется «задний лепесток» (rear lobe) и объединяется с основным сектором.
4. В витрину пишутся:
   - `sector_wkt` — WKT-геометрия сектора;
   - `sector_wkt_area` — площадь в км² (приближённый пересчёт градусов в км);
   - `sector_wkt_centroid_lon/lat` — центроид сектора.

#### Детализация `MAPINFO` (`mapinfo_wkt`)

1. Для каждой БС валидируются и нормализуются радиопараметры:
   - `power`, `height`, `amplification`, `tilt/el_tilt/mech_tilt`, `rad_class`;
   - пропуски закрываются дефолтами по `bs_type`, значения клипуются в допустимые диапазоны.
2. Рассчитывается `mapinfo_reach_m` (эффективный радиус покрытия, метры) как функция:
   - `sector_radius`, мощности, высоты, наклона и `rad_class`;
   - с нижними/верхними ограничениями по типу БС.
3. Далее внутри каждой группы `(telecomstandard, frequency)`:
   - координаты переводятся в локальную метрическую плоскость;
   - дубликаты точек слегка «раздвигаются» (`jitter`) для устойчивого Вороного;
   - строится диаграмма Вороного;
   - для каждой БС берётся её ячейка и ограничивается буфером радиуса `mapinfo_reach_m`.
4. Результат сохраняется в:
   - `mapinfo_wkt` — WKT ячейки «best server»;
   - `mapinfo_wkt_area` — площадь ячейки, км²;
   - `mapinfo_wkt_centroid_lon/lat` — центроид ячейки.

### Шаг 4. SCD-merge и запись

1. Прочитать существующий `output_path` (если есть).
2. Сравнить открытые записи с текущим снимком по ключу `(mcc, mnc, lac, cell_id)` и payload-полям.
3. Закрыть изменившиеся/удалённые (`date_off = effective_ts - 1 микросекунда`).
4. Вставить новые/изменившиеся (`date_on = effective_ts`, `date_off = _OPEN_END_TS`).
5. Записать объединённую историю в `output_path`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет `src_bs`, oktmo или time_zones |
| `ValueError` | Нет обязательных колонок после маппинга в STG snapshot |
| shapely / h3 | Битые WKT, ошибка H3 |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`src/mobile/schema/stg/bs.json`](../../src/mobile/schema/stg/bs.json) |
| ETL | [`src/mobile/pipelines/stg/bs.py`](../../src/mobile/pipelines/stg/bs.py) |
| src_bs | [`build_src_bs.md`](../src/build_src_bs.md) |
| ОКТМО | [`build_stg_oktmo.md`](./build_stg_oktmo.md) |
