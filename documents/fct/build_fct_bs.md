# build-fct-bs

**Витрина:** `fct_bs` · **Команда:** `build-fct-bs` · **Режим:** ежедневное обновление с сохранением истории (`date_on`/`date_off`) в одном Parquet.

Референс: [`pipelines/fct/bs.py`](../../src/mobile/pipelines/fct/bs.py). Схема витрины: [`bs.json`](../../src/mobile/schema/fct/bs.json).

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

## Параметры запуска

Вызов: `run_build(src_bs_path, oktmo_path, time_zones_path, output_path)` ([`cli.py`](../../src/mobile/cli.py) → `build-fct-bs`). **Все четыре обязательны** — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `src_bs_path` | path | **Да** | Входной parquet `src_bs` |
| `oktmo_path` | path | **Да** | Справочник ОКТМО (`dim_oktmo`) |
| `time_zones_path` | path | **Да** | Справочник часовых поясов (`dim_time_zones`) |
| `output_path` | path | **Да** | Выходной Parquet `fct_bs` (SCD, перезапись) |

Пути **относительные к корню репозитория** `mobile` (`resolve_project_path`). Parquet пишется со сжатием **`snappy`** (`DEFAULT_PARQUET_COMPRESSION`).

**Константы ETL в коде** (на вход job **не передаются**): `FCT_BS_FIELDS`, `_OPEN_END_TS`, H3/MAPINFO-параметры — см. [`bs.py`](../../src/mobile/pipelines/fct/bs.py).

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Один прогон с путями `data/src/bs.parquet`, `data/dim/oktmo.parquet`, `data/dim/time_zones.parquet`, `data/fct/bs.parquet` |
| Все 4 явно | `--src-bs-path`, `--oktmo-path`, `--time-zones-path`, `--output-path` |

**Предусловия:** `build-src-bs`, `build-dim-oktmo`, `build-dim-time-zones`.

Локальный запуск:

```bash
uv run mobile build-src-bs
uv run mobile build-dim-oktmo
uv run mobile build-dim-time-zones
uv run mobile build-fct-bs
uv run mobile build-fct-bs \
  --src-bs-path data/src/bs.parquet \
  --oktmo-path data/dim/oktmo.parquet \
  --time-zones-path data/dim/time_zones.parquet \
  --output-path data/fct/bs.parquet
```

Логи: `data/logs/mobile.log`. Метрики: `data/qa/command_timing.jsonl`, `command=build-fct-bs`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `fct_bs` — [`bs.json`](../../src/mobile/schema/fct/bs.json) |
| Формат | Parquet, один исторический файл |
| Календарный срез | SCD Type 2: интервалы `date_on`/`date_off`; `date_on` = timestamp запуска |
| Сжатие | `snappy` |

### Поля витрины

Контракт — [`bs.json`](../../src/mobile/schema/fct/bs.json) → `fields` (28 колонок). Ключевые:

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
| `dim_oktmo` | `data/dim/oktmo.parquet` | ОКТМО level 1/2, WKT |
| `dim_time_zones` | `data/dim/time_zones.parquet` | timezone по полигонам |

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

1. Построение CGI-ключа: `mcc`, `mnc`, `lac`, `cell_id` из нормализованных полей `src_bs`.
2. `telecomstandard` — маппинг generation → `{2G,3G,4G}`; `bs_type` — буквенный код типа БС.
3. Секторные поля: `sector_azimuth`, `sector_angle`, `sector_radius` — из radio + типа БС.
4. **H3** (если включено): индекс ячейки по `lat/lon` на заданном resolution.
5. **Dedup:** `drop_duplicates(subset=[mcc,mnc,lac,cell_id], keep="first")` на снимке `src_bs`;
   - при конфликте payload берётся первая строка после сортировки по `date_on` (детали в коде `_map_src_bs_row`).
6. Метрики: `rows_in`, `rows_after_dedup`.

### Шаг 3. Обогащение

- timezone — point-in-polygon по `dim_time_zones`;
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
| Схема | [`src/mobile/schema/fct/bs.json`](../../src/mobile/schema/fct/bs.json) |
| ETL | [`src/mobile/pipelines/fct/bs.py`](../../src/mobile/pipelines/fct/bs.py) |
| src_bs | [`build_src_bs.md`](../src/build_src_bs.md) |
| ОКТМО | [`build_dim_oktmo.md`](../dim/build_dim_oktmo.md) |
