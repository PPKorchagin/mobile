# build-src-person

**Витрина:** `src_person` · **Команда:** `build-src-person` · **Режим:** суточные Parquet-каталоги, маркер `_SUCCESS` на полных срезах.

Референс: [`pipelines/src/person.py`](../../src/mobile/pipelines/src/person.py). Схема витрины: [`person.json`](../../src/mobile/schema/src/person.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Сгенерировать профили абонентов по календарным дням и операторам | Суточные `person.parquet` |
| 2 | Пометить дни полного среза файлом `_SUCCESS` | Каталоги full snapshot |
| 3 | Записать метрики сборки | `command=build-src-person` в JSONL |

**Бизнес-назначение:** синтетическая витрина принадлежности/профиля абонента (Person) по дням.

**В scope задач:** Faker `ru_RU`, пулы абонентов по операторам, суточная активность, parquet по layout из JSON.

---

## TODO

1. Вынести `extra_random_full_snapshot_days` и период в аргументы CLI при необходимости.
2. Сверить `abonent_status` с поставщиком (PER-016).

---

## Параметры запуска

Вызов: `person.run(output_layout, compression, success_flag, params)` ([`cli.py`](../../src/mobile/cli.py)).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `output_layout` | string (template) | Да | `data/src/person/load_year={YYYY}/load_month={MM}/load_day={DD}` | Шаблон каталога среза |
| `compression` | string | Да | `snappy` | Сжатие Parquet |
| `success_flag` | string | Да | `_SUCCESS` | Маркер полного среза в каталоге дня |
| `params` | `BuildSrcPersonParams` | Да | `default_person_params(...)` | Правила генерации и объём |

| Переменная CLI | Тип | По умолчанию | Описание |
|----------------|-----|--------------|----------|
| `--target-per-operator` | int | `50000` | Абонентов на оператора в **полный** день |

**Поля `BuildSrcPersonParams`** ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)):

| Параметр | По умолчанию | Смысл |
|----------|--------------|-------|
| `start_date` / `end_date` | `2024-12-25` … `2025-02-05` | **43** календарных дня |
| `operators` | 4 оператора | MNC → `operator_Id` |
| `target_active_subscribers_per_operator` | `--target-per-operator` или `50000` | Пул в полный день |
| `daily_active_ratio_min` / `max` | `0.55` / `0.95` | Доля пула в частичные дни |
| `closed_contract_ratio` | `0.18` | Закрытые договоры |
| `inactive_ratio` | `0.12` | Неактивные |
| `corporate_ratio` | `0.14` | `client_type=1` |
| `inter_operator_transition_ratio` | `0.10` | Смена оператора |
| `movement_ratio` | `0.22` | «Переезд» home operator |
| `foreign_subscriber_ratio` | `0.10` | Иностранные ФЛ |
| `extra_random_full_snapshot_days` | `7` | Случайные полные дни поверх month-end |
| `mnp_portability_ratio` | `0.02` | Доля ФЛ с **MNP**: тот же MSISDN, новый `operator_Id` / IMSI / IMEI / ICCID с `actually_from` = день среза |
| `multi_sim_per_contract_ratio` | `0.015` | Доля ФЛ с **второй SIM** на том же `contract_number` (другой IMSI/ICCID) |
| `seed` | `20250407` | Faker и выбор random full-дней |
| `max_workers` | `default_max_workers()` | Параллелизм по **дням** (потоки) |

**Константы в коде:** `SRC_PERSON_TABLE`, `SRC_PERSON_FIELDS` (см. [`person.json`](../../src/mobile/schema/src/person.json)), `ACTUALLY_TO_OPEN`, `OPERATOR_PROCESS_WORKERS=4`, `PERSON_CHUNK_SIZE`.

Локальный запуск:

```bash
uv run mobile build-src-person
uv run mobile build-src-person --target-per-operator 5000
```

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `person` — [`person.json`](../../src/mobile/schema/src/person.json) → `table` |
| Описание | Источник Person (принадлежность/профиль абонента) |
| Формат хранения | Parquet по каталогам дня |
| Layout | `data/src/person/load_year={YYYY}/load_month={MM}/load_day={DD}/person.parquet` |
| Маркер full snapshot | `_SUCCESS` в каталоге дня (`SRC_PERSON_SUCCESS_FLAG`) |
| Сжатие | `snappy` |

**Ключ матчинга с ОСС:** `isdn` + `actually_from` / `actually_to`. `operator_Id` = MNC. У активных: `actually_to` = `ACTUALLY_TO_OPEN` (в Q&A — `9999-12-31`).

### Поля витрины

Контракт — [`person.json`](../../src/mobile/schema/src/person.json) → `fields` (**99** полей). Порядок колонок в Parquet — как в JSON.

Ключевые идентификаторы: `isdn`, `imsi`, `imei`, `operator_Id`, `actually_from`, `actually_to`, `abonent_status`, `client_type`, `abonent_last_location`, адреса и документы ФЛ/ЮЛ — см. JSON.

### Полные срезы (`_SUCCESS`)

`select_full_snapshot_days`: концы месяцев в периоде + **7** случайных дней (детерминированно по `seed`).

Пример при дефолтном seed (10 дней): month-end `2024-12-31`, `2025-01-31`, `2025-02-05`; random `2025-01-03`, `01-05`, `01-07`, `01-08`, `01-09`, `01-12`, `01-15`.

На полном срезе — `target_active_subscribers_per_operator` строк на оператора; на остальных днях — доля пула `daily_active_ratio_min` … `max`.

### Ожидаемый объём

Зависит от `--target-per-operator` и числа дней (43 × 4 оператора × доля активности).

---

## Источники витрины

Внешние файлы-справочники **не используются** — только JSON-схема и синтетическая генерация (Faker).

---

## Алгоритм обработки данных

Точка входа: `run(output_layout, compression, success_flag, params)` в [`person.py`](../../src/mobile/pipelines/src/person.py).

### Шаг 0. Подготовка и календарь

1. `fields = SRC_PERSON_FIELDS`, layout и `success_flag` — аргументы job (`project_paths`).
2. Список дней `tasks`: каждый день от `start_date` до `end_date` включительно; пустой список → `ValueError`.
3. `full_snapshot_days = select_full_snapshot_days(tasks, extra_random_day_count, seed)`:
   - `_month_end_snapshot_days`: последний день каждого месяца в периоде (не позже `end_date`).
   - Из оставшихся дней — `extra_random_day_count` штук через `np.random.default_rng(stable_seed(...))` без повтора.
4. Faker `ru_RU` с seed `stable_seed("faker_pool", params.seed)`; `_build_faker_pool` — предгенерированные списки имён, адресов, паспортов и т.д.

**Параллелизм по дням:**

`day_parallelism = min(len(tasks), DAY_PARALLELISM_CAP (=3), max(1, max_workers // OPERATOR_PROCESS_WORKERS (=4)))`.

`ThreadPoolExecutor(day_parallelism)` → на каждый день `_generate_and_write_day(...)`.

### Шаг 1. Один календарный день (`_generate_and_write_day`)

1. `output_path = _resolve_output_path(out_template, day)` — подстановка `{YYYY}/{MM}/{DD}`, при отсутствии `.parquet` в пути добавляется `person.parquet` относительно `PROJECT_ROOT`.
2. `arrow_schema = _build_arrow_schema(fields)`, `field_order` — порядок колонок из JSON.
3. **Объём на оператора:**
   - если `day ∈ full_snapshot_days` → `daily_ratio = 1.0`;
   - иначе `rng.uniform(daily_active_ratio_min, daily_active_ratio_max)` с seed `stable_seed("src_person_daily_ratio", day, seed)`;
   - `per_operator_count = target` или `max(1, int(target * daily_ratio))`.
4. **Ветка без процессов** (`operator_workers == 1`): один `ParquetWriter`; для каждого оператора чанки по `PERSON_CHUNK_SIZE` (250_000).
5. **Ветка с процессами** (`operator_workers = min(4, len(operators))`):
   - временный каталог `.tmp_person_{YYYYMMDD}_{ms}`;
   - `ProcessPoolExecutor` + `_init_operator_worker(seed)` — Faker pool в дочернем процессе;
   - `_write_operator_temp_file` → отдельный parquet на оператора;
   - финальный `ParquetWriter` склеивает batch'и из tmp через `iter_batches`;
   - `shutil.rmtree(tmp_dir)`.
6. **Маркер `_SUCCESS`:** если full snapshot — создать пустой файл; иначе удалить существующий.

### Шаг 2. Срез одного оператора (`_generate_operator_slice`)

Детерминированный RNG: `stable_seed("src_person", serving_operator, day, seed, local_id_offset, count)`.

| Этап | Код |
|------|-----|
| Идентичность | `home_operator` = serving; с вероятностью `movement_ratio` — соседний оператор (`_neighbor_operator_vectorized`); с `inter_operator_transition_ratio` — другой оператор для triplet (`_neighbor_operator`) |
| MSISDN/IMSI/IMEI | `_identity_triplet(home_for_ids, sid)`; ~2% `INVALID_ISDN_PROBABILITY` → `_sample_invalid_isdn_digits`; ~3% `IDENTITY_FIELD_LEAK_PROBABILITY` — imsi/imei в «чужих» `identity_type` |
| Статусы | `closed_contract_ratio`, `inactive_ratio` → `active_now`; `abonent_status` 0/1 |
| Тип клиента | `corporate_ratio` → `client_type` 0/1; ФЛ/ЮЛ поля, иностранцы `foreign_subscriber_ratio` → `_assign_citizenship_codes` (alpha-2 в генераторе; в `stg_person` → `numeric_code` через [`build-stg-person`](../stg/build_stg_person.md)) |
| identity_type | веса `[0.74, 0.12, 0.06, 0.05, 0.03]` для типов 2/4/5/3/1; условные колонки GSM/data/VoIP/CDMA |
| Интервалы | `actually_from = day`; `actually_to` = `ACTUALLY_TO_OPEN` если активен, иначе конец дня; договор/услуги согласованы с `closed_contract` |
| Локация | `abonent_last_location` с весами; `lac`/`cell` только при `== 0` |
| Гео | `coordinates`, `geo_json` из latitude/longitude пула |
| SCD2 PER-002 | `_append_scd2_overlap_rows`: +4% строк-копий с пересекающимися `actually_from`/`actually_to`; 35% из них с `ACTUALLY_TO_OPEN` |
| MNP / multi-SIM | `_append_mnp_and_multi_sim_rows` после основного среза (см. ниже) |

Возвращается `DataFrame` → `_align_columns_for_schema` → `pa.Table.from_pandas(..., schema=arrow_schema)`.

### Шаг 2a. MNP и вторая SIM (для `build-stg-person`)

Вызов: `_append_mnp_and_multi_sim_rows` после склейки чанков операторов, до записи parquet дня.

**MNP** (`mnp_portability_ratio`, clamp 0…0.2):

1. Индекс ФЛ с GSM (`identity_type` / GSM-поля заполнены).
2. `n_mnp = floor(len(gsm) * ratio)`; случайный выбор без возврата (`rng.choice`).
3. Для каждой выбранной строки — **копия**:
   - `isdn` (MSISDN) **без изменений**;
   - новый `operator_Id` — соседний/случайный оператор (`OPERATORS`);
   - новые `imsi`, `imei`, `iccid` (детерминированный seed от строки);
   - `actually_from` = начало дня среза;
   - `actually_to` = `ACTUALLY_TO_OPEN` (`2999-12-31`).
4. Копии **добавляются** к выходу дня (concat), не заменяют исходную строку.

**Multi-SIM** (`multi_sim_per_contract_ratio`, clamp 0…0.2):

1. Индекс ФЛ (`client_type=0`).
2. Выбор строк; копия с тем же `contract_number` и `isdn`;
3. Новые `imsi` + `iccid`; `actually_from` = день; `actually_to` = open.

**Downstream:** при нескольких `load_day` в месяце [`build-stg-msisdn-operator`](../stg/build_stg_msisdn_operator.md) видит две записи с одним MSISDN и разными `operator_Id`; [`build-stg-person`](../stg/build_stg_person.md) связывает их через `msisdn↔imsi` в union-find.

### Шаг 3. Завершение оркестратора

1. `as_completed` по дням, агрегация `generated_rows`, `full_days`.
2. `append_command_metrics` с `elapsed_total_sec`, `rows`, `files`, `day_workers`, `operator_process_workers`, `full_days`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет `person.json` |
| `ValueError` | Пустой период; `extra_random_full_snapshot_days < 0` |
| `KeyError` | Оператор не из `OPERATORS` |
| pyarrow | Запись parquet / несовместимость схемы |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/src/person.json`](../../src/mobile/schema/src/person.json) |
| ETL | [`src/mobile/pipelines/src/person.py`](../../src/mobile/pipelines/src/person.py) |
| STG person | [`build_stg_person.md`](../stg/build_stg_person.md) |
| Пути по умолчанию | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
