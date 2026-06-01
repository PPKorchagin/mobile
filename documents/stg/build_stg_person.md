# build-stg-person

**Витрина:** `stg_person` · **Команда:** `build-stg-person` · **Режим:** один месячный срез физлиц с устойчивым `person_id`.

Референс: [`pipelines/stg/person.py`](../../src/mobile/pipelines/stg/person.py).

Схема: [`person.json`](../../src/mobile/schema/stg/person.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Принять `report_date` = 1-е число отчётного месяца | `2025-01-01` |
| 2 | `src_person` — **последний** `load_day` с `_SUCCESS` (профиль) | `person.parquet` |
| 3 | `stg_oksm` → `OksmLookup` | `citizenship` = `numeric_code` или `U` |
| 4 | Синхронизация месячных `stg_msisdn_imsi` / `stg_msisdn_imei` из `stg_geo_all` по дням месяца | `{YYYY-MM-01}.parquet` для обеих binding-витрин |
| 5 | Чтение месячных binding + обогащение `src_person` | MSISDN↔IMSI, MSISDN↔IMEI, MNP на интервалах |
| 6 | Исключения: `src_imsi` / `src_imei` / `src_msisdn` + M2M по `stg_tac` | Строки excl и IoT не попадают в кластеризацию |
| 7 | Union-find: `bio`, `iccid`, ID + binding + operator | `person_cluster_key` |
| 8 | `person_id` из прошлого `stg_person` по `person_cluster_key` | Стабильный ID между месяцами |
| 9 | Запись `stg_person` | 1 строка на персону, поле `sim_count` |

**Бизнес-назначение:** стабильный месячный слой персон (идентификатор + демография + основная подписка) для джойнов с geo/event.

**В scope:** кластеризация по био/ICCID и техническим ID; месячные binding-витрины; исключения из [`build-src-excl`](../src/build_src_excl.md). Номер договора (`contract_number`) **не** участвует в графе.

**Предусловия:**

- [`build-src-excl`](../src/build_src_excl.md) → `data/src/excl/src_{imsi,imei,msisdn}.parquet`.
- [`build-stg-tac`](build_stg_tac.md) → `data/stg/tac.parquet` (иначе M2M-фильтр пропускается с warning).
- [`build-stg-oksm`](build_stg_oksm.md) → `data/stg/oksm.parquet` (обязателен для `citizenship`).
- [`build-stg-geo-all`](build_stg_geo_all.md) по дням месяца (pipeline дополняет binding-витрины при `sync_bindings_from_geo=true`, по умолчанию).

---

## TODO

1. Профилирование по операторам и `person_confidence` в `command_timing`.

---

## Параметры запуска

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | **Только `YYYY-MM-01`** |
| `src_person_path` | path | Нет | `data/src/person` | Корень layout или parquet |
| `stg_msisdn_imsi_path` | path | Нет | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | MSISDN↔IMSI за месяц |
| `stg_msisdn_imei_path` | path | Нет | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | MSISDN↔IMEI за месяц |
| `stg_tac_path` | path | Нет | `data/stg/tac.parquet` | M2M по TAC |
| `stg_oksm_path` | path | Нет | `data/stg/oksm.parquet` | Справочник ОКСМ для `citizenship` |
| `src_excl_imsi_path` | path | Нет | `data/src/excl/src_imsi.parquet` | Список исключений IMSI |
| `src_excl_imei_path` | path | Нет | `data/src/excl/src_imei.parquet` | Список исключений IMEI |
| `src_excl_msisdn_path` | path | Нет | `data/src/excl/src_msisdn.parquet` | Список исключений MSISDN |
| `output_path` | path | Нет | `data/stg/person/{YYYY-MM-01}.parquet` | Выход `stg_person` |

### Выбор `src_person` (профиль)

Режим **`latest_snapshot`** (в [`person.py`](../../src/mobile/pipelines/stg/person.py), `_read_src_person_latest_snapshot`):

1. Период: с `report_date` по последний день месяца.
2. Каталоги `load_day=*` с `_SUCCESS` и `person.parquet`.
3. Взять **максимальный** `load_day`.
4. Прочитать один `person.parquet`.

MNP и смена IMSI учитываются через интервалы в месячной `stg_msisdn_imsi` (наблюдения из `stg_geo_all` по дням месяца).

```bash
uv run mobile build-src-excl
uv run mobile build-stg-person --report-date 2025-01-01
```

Binding-витрины можно собрать заранее по дням (`build-stg-msisdn-imei`, `build-stg-msisdn-imsi-operator`) или положиться на синхронизацию внутри `build-stg-person` из `stg_geo_all`.

Логи: `data/logs/mobile.log`. Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-person`.

---

## Структура витрины

| Витрина | Путь | Гранулярность |
|---------|------|----------------|
| `stg_person` | `data/stg/person/{YYYY-MM-01}.parquet` | 1 строка на `person_id` (основная подписка + `sim_count`) |

Формат: Parquet, `snappy`.

### Поля `stg_person`

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `report_date` | date | 1-е число месяца |
| 2 | `person_id` | string | `prs_` + SHA256[:24]; стабилен через ledger |
| 3 | `person_cluster_key` | string | Канонический ключ кластера |
| 4 | `person_confidence` | string | `high` / `medium` / `low` |
| 5 | `sim_count` | long | Число distinct SIM (`imsi` \| `iccid`) |
| 6 | `msisdn` | string | Основной MSISDN |
| 7 | `imsi` | string | Основной IMSI |
| 8 | `imei` | string | Основной IMEI |
| 9 | `gender` | string | `M` / `F` / `U` |
| 10 | `age` | string | Возраст на начало месяца или `U` |
| 11 | `citizenship` | string | Цифровой код ОКСМ (`numeric_code`) или `U` |
| 12 | `operator_id` | long | Оператор основной подписки |
| 13 | `actually_from` | timestamp | Начало интервала основной SIM |
| 14 | `actually_to` | timestamp | Конец интервала |

---

## Источники витрины

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `src_person` (latest) | `data/src/person/.../person.parquet` | Профиль, bio, iccid |
| 2 | `stg_msisdn_imsi` | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | MSISDN↔IMSI + `operator_id`, MNP |
| 3 | `stg_msisdn_imei` | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | MSISDN↔IMEI |
| 4 | `src_imsi` / `src_imei` / `src_msisdn` | `data/src/excl/*.parquet` | Исключения из анализа |
| 5 | `stg_tac` | `data/stg/tac.parquet` | M2M по TAC |
| 6 | `stg_oksm` | `data/stg/oksm.parquet` | Коды гражданства |
| 7 | `stg_person` (прошлый месяц) | `data/stg/person/{prev YYYY-MM-01}.parquet` | Стабильный `person_id` |
| 8 | `stg_geo_all` (по дням) | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | Инкремент binding внутри pipeline |

Документация вспомогательных сборок:

- [`build_stg_tac.md`](./build_stg_tac.md)
- [`build_stg_oksm.md`](./build_stg_oksm.md)
- [`build_stg_msisdn_imsi_operator.md`](./build_stg_msisdn_imsi_operator.md)
- [`build_stg_msisdn_imsi_operator.md`](./build_stg_msisdn_imsi_operator.md) (geo / IMSI)
- [`build_stg_msisdn_imei.md`](./build_stg_msisdn_imei.md)

---

## Алгоритм обработки данных

Точка входа: `run_build(report_date, …)` в [`person.py`](../../src/mobile/pipelines/stg/person.py).

### Шаг 0. Инициализация

1. Валидация: `report_date.day == 1` (`_validate_report_month`).
2. Период месяца: `period_start = report_date`, `period_end` = последний календарный день.
3. `month_start` / `month_end` как `pd.Timestamp` (конец месяца 23:59:59 для фильтров).
4. Разрешение путей: `person_out`, месячные `stg_msisdn_imsi` / `stg_msisdn_imei`, списки excl.
5. Загрузка контракта полей из [`person.json`](../../src/mobile/schema/stg/person.json).
6. Старт `timed_stage` и счётчиков (`src_rows_read`, `excluded_excl_rows`, `binding_days_synced`, …).

### Шаг 1. Чтение `src_person` (профиль)

1. `_read_src_person_latest_snapshot(...)`:
   - обход `load_day=*` в `[period_start, period_end]` с `_SUCCESS`;
   - выбор **максимального** `load_day`;
   - один `person.parquet` (не concat всех срезов).
2. Метрика `src_load_days` — какие `load_day` участвовали в выборе.
3. При отсутствии среза — `FileNotFoundError`.

### Шаг 2. Загрузка справочника ОКСМ

1. `stg_oksm_path` (по умолчанию `data/stg/oksm.parquet`, CLI `--stg-oksm-path`).
2. [`oksm.load_lookup`](../../src/mobile/pipelines/stg/oksm.py) → `OksmLookup` (индексы `alpha2`/`alpha3` → `numeric_code`, токены из `name_short`/`name_full`).
3. Метрика `load_oksm_sec` в `command_timing.jsonl`.
4. Если parquet отсутствует — `FileNotFoundError` (нужен `build-stg-oksm`).

### Шаг 3. Исключение M2M по TAC

1. Чтение `stg_tac.parquet` (`tac`, `is_m2m`); множество `m2m_tacs`.
2. Для каждой строки: `imei_tac = первые 8 цифр IMEI` после нормализации цифр.
3. Удаление строк, где `imei_tac ∈ m2m_tacs`; счётчик `excluded_m2m_tac_rows`.
4. Если файла TAC нет или нет колонок — **warning**, фильтр не применяется.

### Шаг 4. Витрина `stg_msisdn_imsi` (MNP из src_person)

1. Если `build_operator_vitrine=true`:
   - повторное чтение `src_person` в режиме **`all_snapshots`** (concat всех `load_day` месяца);
   - повторный M2M-фильтр;
   - `build_operator_intervals_from_src` → группировка `(msisdn, operator_id, imsi)`:
     - `valid_from = min(actually_from)`, `valid_to = max(actually_to)`;
     - только `client_type=0`, интервал ∩ месяц.
2. Запись `operator_out` (parquet snappy).
3. Если витрина уже есть и `build_operator_vitrine=false` — чтение с диска.
4. **Рёбра MNP** в графе строятся только как `msisdn`↔`imsi` на интервале operator (не по одному `operator_id`).

### Шаг 5. Месячные binding MSISDN↔IMSI/IMEI

1. Пути: `stg_msisdn_imsi_output_path(report_month)` → `…/msisdn_imsi/{YYYY-MM-01}.parquet` (месячный файл).
2. Если `build_bindings_month=true` и файла нет — `_refresh_month_bindings_from_geo` ([`person.py`](../../src/mobile/pipelines/stg/person.py)):
   - для каждого дня месяца с `stg_geo_all` вызвать `msisdn_imsi.run_build` и `msisdn_imei.run_build` (инкремент в month parquet).
3. `_read_binding_parquet` — нормализация `msisdn`/`imsi`/`imei`, `valid_from`/`valid_to`.

### Шаг 6. Ledger прошлого месяца

1. `_previous_report_month(report_month)` → 1-е число предыдущего месяца.
2. `_load_previous_ledger`: чтение `person_id_ledger` прошлого месяца (если есть) — колонки `person_id`, `person_cluster_key`, `node`.

### Шаг 7. Подготовка подписок (`_prepare_subscriptions`)

1. **Фильтр ФЛ:** `client_type == 0`.
2. **Пересечение с месяцем:** `actually_from <= month_end` и `actually_to >= month_start`; `actually_to` без значения → `2999-12-31`.
3. **Нормализация ID:**
   - `msisdn` ← `normalize_msisdn(isdn)`;
   - `imsi`, `imei`, `iccid` — строковые поля;
   - `operator_id` ← `operator_Id`.
4. **Binding-fill** на момент `binding_at` = конец последнего дня месяца (`_enrich_identifiers_from_bindings`):
   - для пустого `imsi` — lookup по `msisdn` в `stg_msisdn_imsi` (интервал содержит `at`);
   - симметрично `msisdn`←`imsi`, `imei`↔`msisdn`;
   - при нескольких интервалах — запись с **максимальным** `valid_from` (самая свежая привязка);
   - метрики `binding_fill` (сколько полей дозаполнено).
5. Отбор строк с полным ключом: `msisdn`, `imsi`, `imei`, `operator_id`, интервалы не null.

### Шаг 8. Кластеризация (`_assign_clusters`, union-find)

1. Инициализация `UnionFind`.
2. **Узлы co-occurrence** на каждой строке `src_person`:
   - `_unite_pair_column`: `msisdn↔imsi`, `msisdn↔imei`, `msisdn↔iccid`;
   - `bio_key` = `bio:фамилия|имя|отчество|дата_рождения|цифры_документа` (casefold, только при валидном ФИО+ДР);
   - `msisdn↔bio`.
3. **Рёбра из binding** (`binding_edges_in_month`):
   - все пары `(msisdn, imsi)` / `(msisdn, imei)`, у которых `[valid_from, valid_to]` пересекает `[month_start, month_end]`.
4. **Рёбра MNP** (`operator_observation_edges`):
   - для каждой строки operator-витрины: `union(msisdn:…, imsi:…)` если IMSI не пуст.
5. **Канонический ключ кластера** (`canonical_cluster_key`):
   - приоритет: первый лексикографически `bio:` → `iccid:` → иначе `min(все узлы кластера)`.
6. Для каждой строки: `roots` = `find()` по всем узлам строки; `person_cluster_key` = canonical корня `min(roots)`; `person_confidence` по типам узлов в кластере.

### Шаг 9. Назначение `person_id` (`assign_person_ids_with_ledger`)

1. Из ledger прошлого месяца: индексы `node → person_id` и `person_cluster_key → person_id`.
2. Для каждого `person_cluster_key` текущего месяца:
   - если ключ уже в ledger → тот же `person_id`;
   - иначе если **любой** узел кластера встречался в ledger → взять его `person_id`;
   - иначе `person_id = prs_` + SHA256(`person_cluster_key`)[:24].
3. Обновление индексов для всех узлов кластера (для следующих строк того же месяца).

### Шаг 10. Выходные витрины (`_build_outputs`)

1. **`stg_person_sim`:**
   - все строки `work` с `report_date`, `person_id`;
   - `is_primary=true` для строки с **максимальным** `actually_from` внутри `person_id` (последняя активная подписка);
   - остальные `is_primary=false`.
2. **`stg_person`:**
   - одна строка на `person_id` — из primary-строки;
   - `sim_count` = `nunique(imsi|iccid)` по подпискам персоны;
   - `gender` ← `_derive_gender` (по полю/ФИО);
   - `age` ← возраст на `month_start` из `birth_day` или `U`;
   - **`citizenship`** — `_derive_citizenship_from_row` + `OksmLookup` (см. ниже); в витрине только **`numeric_code`** (3 цифры) или `U`, не alpha-2.
3. **`stg_person_id_ledger`:**
   - для каждого `(person_cluster_key, person_id)` — по одной строке на каждый узел графа (`node` = `msisdn:…`, `bio:…`, …);
   - снимок для стабильности ID в следующем месяце.

#### Определение `citizenship` (`_derive_citizenship`)

Вход: `dul_department`, `document`, ФИО из primary-строки `src_person` (в `src_person` гражданство **не хранится** — только подсказки в текстах документов/подразделений).

Порядок разрешения (каждый шаг возвращает `numeric_code` через `OksmLookup.from_alpha2` или `match_country_names`):

1. Подстроки в `dul_department` → `_DEPT_MAP` (alpha-2) → ОКСМ (`мвд` → `643`, …).
2. Подстроки в `document` → `_DOC_MAP` (`паспорт рф`, `казахстан`, …).
3. Подстроки в объединённом тексте → `_NAME_HINTS` (`kaz`, `uzb`, …).
4. Совпадение `name_short` / `name_full` из справочника ОКСМ в тексте.
5. Эвристика русского ФИО (отчество `вич`/`вна`/…): при `мвд`/`паспорт рф` или пустых doc/dept → `default_russia()` (`643`).
6. Иначе `U`.

Константы `_DEPT_MAP`, `_DOC_MAP`, `_NAME_HINTS` — в [`person.py`](../../src/mobile/pipelines/stg/person.py); значения — ISO alpha-2, итог всегда **цифровой код ОКСМ**.

### Шаг 11. Запись и метрики

1. `to_parquet` для трёх витрин (snappy).
2. `append_command_metrics`: `elapsed_*_sec` (в т.ч. `load_oksm_sec`), `stg_rows_written`, `person_sim_rows`, `ledger_rows`, пути входов.

### Типовые ошибки

| Ситуация | Поведение |
|----------|-----------|
| `report_date` не 1-е число | `ValueError` / `SystemExit` |
| Нет `_SUCCESS` за месяц | `FileNotFoundError` |
| Нет `stg_tac` | warning, M2M не фильтруется |
| Нет `stg_oksm` | `FileNotFoundError` при старте |
| Нет `stg_geo_all` за дни | пустые/частичные binding, слабый fill |
| Нет bio и нет связующих рёбер | отдельные кластеры по tech ID, `person_confidence=low` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`person.py`](../../src/mobile/pipelines/stg/person.py) |
| Граф / ID | [`person.py`](../../src/mobile/pipelines/stg/person.py) (`_UnionFind`, `_assign_person_ids`) |
| Чтение src | [`person.py`](../../src/mobile/pipelines/stg/person.py) (`_read_src_person_latest_snapshot`) |
| DQ | [`dq_stg_person.md`](../dq/stg/dq_stg_person.md) · `uv run mobile dq-stg-person` |
| Источник | [`build_src_person.md`](../src/build_src_person.md) |
| Пути | [`project_paths.py`](../../src/mobile/project_paths.py) |
