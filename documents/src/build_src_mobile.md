# build-src-mobile

**Витрины:** CDR, SMS, GPRS, location (ОСС) · **Команда:** `build-src-mobile` · **Режим:** суточные Parquet по оператору и типу события.

Референс: [`pipelines/src/mobile.py`](../../src/mobile/pipelines/src/mobile.py). Схемы: [`cdr.json`](../../src/mobile/schema/src/cdr.json), [`sms.json`](../../src/mobile/schema/src/sms.json), [`gprs.json`](../../src/mobile/schema/src/gprs.json), [`location.json`](../../src/mobile/schema/src/location.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Загрузить BS и пулы абонентов из `src_person` по месяцам | Контекст для событий |
| 2 | Сгенерировать события по дням и операторам (4 витрины) | DataFrame на витрину × день × оператор |
| 3 | Записать Parquet в layout из JSON | Каталоги `data/src/mobile/operator/...` |

**Бизнес-назначение:** синтетические мобильные события (звонки, SMS, GPRS, location) для операторов.

**В scope задач:** привязка к BS и person, локальное время (опционально TZ CSV), cross-mart шум, запись parquet.

---

## TODO

1. Сверить веса `Service` и cross-mart с DQ (OCC-018, OCC-003 / GEN-018).
2. При необходимости вынести период и `movement_ratio` в CLI.

---

## Параметры запуска

Вызов: `run_mobile_all(bs_parquet_path, person_config_path, cdr/sms/gprs/location configs, params)` ([`cli.py`](../../src/mobile/cli.py)).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `bs_parquet_path` | string (path) | Да | `data/src/bs.parquet` | Справочник БС |
| `person_config_path` | string (path) | Да | `src/mobile/schema/src/person.json` | Layout person для пула |
| `cdr_config_path` … `location_config_path` | string (path) | Да | `src/mobile/schema/src/{cdr,sms,gprs,location}.json` | Схемы и layout витрин |
| `params` | `BuildSrcMobileParams` | Да | `default_mobile_params()` | Период, операторы, seed |

Флагов CLI **нет**.

**Поля `BuildSrcMobileParams`** ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)):

| Параметр | По умолчанию | Смысл |
|----------|--------------|-------|
| `start_date` / `end_date` | `2024-12-25` … `2025-02-05` | Период генерации |
| `operators` | 4 оператора | Ключи: `билайн`, `мегафон`, `мтс`, `теле2` |
| `seed` | `20250407` | Активность и шум |
| `max_workers` | `4` | **Один OS-процесс на оператора** |
| `movement_ratio` | `0.22` | Доп. смена БС в течение дня |
| `region_subjects` | `()` | Без фильтра BS по `subject` |

Локальный запуск:

```bash
uv run mobile build-src-mobile
```

---

## Структура генерируемых витрин

| Витрина | Event | JSON | Шаблон каталога (`s3_layout`) |
|---------|-------|------|-------------------------------|
| CDR | `10001` | [`cdr.json`](../../src/mobile/schema/src/cdr.json) | `data/src/mobile/operator/cdr/{name_operator}/10001/{YYYY}/{MM}/{DD}/` |
| SMS | `10002` | [`sms.json`](../../src/mobile/schema/src/sms.json) | `.../sms/.../10002/...` |
| GPRS | `10003` | [`gprs.json`](../../src/mobile/schema/src/gprs.json) | `.../gprs/.../10003/...` |
| location | `10004` | [`location.json`](../../src/mobile/schema/src/location.json) | `.../location/.../10004/...` |

`{name_operator}` — `beeline`, `megafon`, `mts`, `tele2` (`OPERATOR_SLUG` в [`mobile.py`](../../src/mobile/pipelines/src/mobile.py)).

| Свойство | Значение |
|----------|----------|
| Формат | Parquet (один файл на витрину × оператор × день) |
| Сжатие | `snappy` |
| Поля | Контракт в соответствующем JSON → `fields` (десятки колонок на витрину) |

Примечания из JSON: колонки nullable; между витринами возможны перекрёстные типы событий (см. `notes` в схемах).

### Синтетические отклонения (в коде)

| ID | Правило |
|----|---------|
| OCC-018 `Service` | Веса `SERVICE_*_WEIGHTS`, `pick_weighted_service` |
| OCC-003 / GEN-018 | `inject_cross_mart_rows`: ~2.5% перенос между витринами; ~2% неверный `Event` |

---

## Источники витрины

| # | Источник | Путь (по умолчанию) | Назначение |
|---|----------|---------------------|------------|
| 1 | `src_bs` | `data/src/bs.parquet` | Геометрия и радио БС |
| 2 | `src_person` | `data/src/person/load_year=.../load_month=.../load_day=.../` | Последний parquet с `_SUCCESS` **в месяце** |
| 3 | Часовые пояса (опционально) | `src/mobile/raw_data/time_zones.csv` | Локальное время по `subject` + координатам БС |

**Предусловия:** `build-src-bs`, `build-src-person` (на каждый месяц периода — `_SUCCESS` в каталоге person).

---

## Алгоритм обработки данных

Точка входа: `run_mobile_all(...)` → `run_mobile_oss_all(...)` в [`mobile.py`](../../src/mobile/pipelines/src/mobile.py). Параметры — `BuildSrcMobileParams` (алиас `BuildSrcMobileOssParams`).

### Шаг 0. Предзагрузка (главный процесс)

1. `person_config_path` обязателен; иначе `ValueError`.
2. `task_dates = calendar_dates_inclusive(start_date, end_date)`.
3. **`build_person_pool_by_operator_month_slices`:**
   - `load_src_person_latest_success_by_month` — для каждого `(year, month)` в периоде последний каталог с `_SUCCESS`, чтение `person.parquet` с колонками `PERSON_SNAPSHOT_COLUMNS`;
   - для каждого календарного дня: `person_interval_overlaps_day(month_frame, day)` — строки, у которых `[actually_from, actually_to]` пересекают сутки;
   - `person_rows_for_operator`: `operator_Id == OPERATOR_MNC[op]`, `filter_physical_person_rows`, непустые `isdn`, `imsi`, `imei`;
   - ключ пула: `(operator, day) → DataFrame`.
4. **BS:** `read_parquet(bs_parquet_path)`; при `params.region_subjects` — фильтр по колонке `subject` (отсутствие колонки → `ValueError`).
5. **`prepare_bs_by_operator`:** `ensure_bs_local_offset_column` (колонка `_local_utc_offset_hours` из `time_zones.csv` по subject/lon/lat или дефолт); разбиение по `mnc == OPERATOR_MNC[operator]`; пустой срез оператора → `ValueError`.

### Шаг 1. Staging и процессы

1. `tempfile.TemporaryDirectory(prefix="mobile_oss_")`.
2. Для каждого оператора:
   - BS среза → `{staging}/{slug}_{sha}.parquet`;
   - `stage_operator_person_pool` — parquet-файлы по дням в `{staging}/person_{slug}_{hash}/`.
3. `ProcessPoolExecutor(max_workers=len(operators))` + shared `tqdm` lock.
4. На каждый оператор — `_run_mobile_oss_for_one_operator(payload)` (см. шаг 2).
5. Агрегация `row_count` / `file_count` по четырём витринам; `append_command_metrics`.

`params.max_workers` / `module_parallelism` **не уменьшают** число процессов: всегда **один процесс на оператора**.

### Шаг 2. Воркер одного оператора (`_run_mobile_oss_for_one_operator`)

**Инициализация воркера:**

- `bs_op = read_parquet(bs_op_path)`; `_build_bs_spatial_context(bs_op)` — индексы БС по субъектам для соседних/дальних переходов.
- `person_pool = load_staged_operator_person_pool(operator, person_pool_dir, calendar_days)` — `(operator, day)` → frame.
- Загрузка `fields` + `s3_layout` + compression из четырёх JSON.

**Цикл по дням** (последовательно, `tqdm` на оператора):

1. **`person_subset_after_active_sample_for_day`:** из `person_pool[(operator, day)]` отбор абонентов (`active_ratio=PERSON_ACTIVE_RATIO_ALL` = 1.0, т.е. все физлица с валидными id в срезе).
2. Если `sampled.empty` — пустые витрины (finalize с пустыми rows / schema-only).
3. Иначе:
   - `subscriber_states_from_person_rows` — `SubscriberDayState` на абонента (home/serving operator, BS, movement_ratio, spatial_ctx).
   - **Чанки по `MOBILE_OSS_SUBSCRIBER_CHUNK_SIZE` (10_000):**
     - `build_subscriber_activity_journey_bundles` — общая активность и маршрут на чанк;
     - `generate_*_rows_from_subscriber_states` для cdr / sms / gprs / location (один `bundles` на чанк, накопление в `all_*` списки dict).
   - **`inject_cross_mart_rows`** (in-place):
     - `n_moves = max(1, int(total_rows * 0.025))` — перенос строк между витринами с `_wrong_event_for_mart`;
     - для оставшихся строк с вероятностью `0.02` — неверный `Event`; для cdr/gprs — `pick_weighted_service` (OCC-018).
4. **Запись дня** (по одному parquet на витрину):
   - `finalize_cdr_day_parquet_from_rows` / `finalize_sms_*` / `finalize_gprs_*` / `finalize_location_*`;
   - путь: `resolve_mobile_output_path(out_template, operator_slug(operator), day)` → подстановка `{name_operator}`, `{YYYY}`, `{MM}`, `{DD}`.
5. Лог строк cdr/sms/gprs/location за день; `files_per_mart = n_days`.

### Шаг 3. Вспомогательная логика (используется внутри генераторов)

| Функция | Назначение |
|---------|------------|
| `subscriber_daily_activity` / `_pick_activity_count` | Число событий на абонента (профили heavy/light) |
| `subscriber_journey_points` / `_transition_bs` | Траектория БС в течение дня, `movement_ratio` |
| `spread_journey_points_for_events` | Разнесение точек маршрута по событиям |
| `event_within_person_interval` | Событие внутри `actually_from`/`actually_to` person |
| `coerce_valid_lac_cell` | Согласование LAC/Cell с BS |
| `apply_owner_isdn_peer_columns` | Колонки владельца/пира для CDR/SMS |
| `pick_weighted_service` | Веса Service по витрине |

Локальное время событий: `bs_local_utc_offset_hours` / `_local_utc_offset_hours` на строке BS.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет `bs.parquet` или person parquet за месяц |
| `ValueError` | Нет `person_config_path`; пустой период; нет `subject` при `region_subjects` |
| pandas / pyarrow | Запись parquet |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схемы ОСС | [`cdr.json`](../../src/mobile/schema/src/cdr.json), [`sms.json`](../../src/mobile/schema/src/sms.json), [`gprs.json`](../../src/mobile/schema/src/gprs.json), [`location.json`](../../src/mobile/schema/src/location.json) |
| Person layout | [`person.json`](../../src/mobile/schema/src/person.json) |
| ETL | [`src/mobile/pipelines/src/mobile.py`](../../src/mobile/pipelines/src/mobile.py) |
| Пути | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
