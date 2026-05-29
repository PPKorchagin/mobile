# build-stg-person

**Витрины:** `stg_person`, `stg_person_sim`, `stg_person_id_ledger` · **Команда:** `build-stg-person` · **Режим:** месячный профиль физлиц с устойчивым `person_id`.

Референс: [`pipelines/stg/person.py`](../../src/mobile/pipelines/stg/person.py), [`person_identity.py`](../../src/mobile/pipelines/stg/person_identity.py).

Схемы: [`person.json`](../../src/mobile/schema/stg/person.json), [`person_sim.json`](../../src/mobile/schema/stg/person_sim.json), [`person_id_ledger.json`](../../src/mobile/schema/stg/person_id_ledger.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Принять `report_date` = 1-е число отчётного месяца | `2025-01-01` |
| 2 | `src_person` — **последний** `load_day` с `_SUCCESS` (профиль) | `person.parquet` |
| 3 | `stg_msisdn_operator` — **все** срезы месяца (MNP) | `data/stg/msisdn_operator/{YYYY-MM-01}.parquet` |
| 4 | Месячные binding (ежедневный инкремент) | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet`, `msisdn_imei` |
| 5 | Исключить M2M по `stg_tac` | Без IoT SIM |
| 6 | Union-find: `bio`, `contract`, `iccid`, ID + bindings + operator | Кластер персоны |
| 7 | `person_id` + ledger прошлого месяца | Стабильный ID между месяцами |
| 8 | `stg_person` (1 строка) + `stg_person_sim` (N SIM) + ledger | Профиль, подписки, узлы |

**Бизнес-назначение:** стабильный месячный слой персон (идентификатор + демография + мульти-SIM) для джойнов с geo/event.

**В scope:** кластеризация по био/договору/ICCID и техническим ID; MNP через operator-витрину; одна строка на персону + детализация SIM.

**Предусловия:**

- [`build-stg-tac`](build_stg_tac.md) → `data/stg/tac.parquet` (иначе M2M-фильтр пропускается с warning).
- Ежедневные [`build-stg-msisdn-imsi`](./build_stg_msisdn_imsi.md) / [`build-stg-msisdn-imei`](./build_stg_msisdn_imei.md) по дням месяца (или авто-`refresh` из `stg_geo_all`).

---

## TODO

1. Реализовать DQ: [`dq_stg_person.md`](../dq/stg/dq_stg_person.md) → `dq-stg-person`.
2. Профилирование по операторам и `person_confidence` в `command_timing`.

---

## Параметры запуска

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | **Только `YYYY-MM-01`** |
| `src_person_path` | path | Нет | `data/src/person` | Корень layout или parquet |
| `stg_msisdn_imsi_path` | path | Нет | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | MSISDN↔IMSI за месяц |
| `stg_msisdn_imei_path` | path | Нет | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | MSISDN↔IMEI за месяц |
| `stg_msisdn_operator_path` | path | Нет | `data/stg/msisdn_operator/{YYYY-MM-01}.parquet` | MNP-интервалы |
| `stg_tac_path` | path | Нет | `data/stg/tac.parquet` | M2M по TAC |
| `output_path` | path | Нет | `data/stg/person/{YYYY-MM-01}.parquet` | `stg_person` |
| `person_sim_path` | path | Нет | `data/stg/person_sim/{YYYY-MM-01}.parquet` | `stg_person_sim` |
| `person_ledger_path` | path | Нет | `data/stg/person_id_ledger/{YYYY-MM-01}.parquet` | ledger узлов |
| `build_bindings_month` | bool | Нет | `true` | `refresh_month_bindings_from_geo`, если нет month parquet |
| `build_operator_vitrine` | bool | Нет | `true` | Пересобрать `stg_msisdn_operator` из всех срезов |

### Выбор `src_person` (профиль)

Режим **`latest_snapshot`** ([`src_person_month.py`](../../src/mobile/pipelines/stg/src_person_month.py)):

1. Период: с `report_date` по последний день месяца.
2. Каталоги `load_day=*` с `_SUCCESS` и `person.parquet`.
3. Взять **максимальный** `load_day`.
4. Прочитать один `person.parquet`.

Для **MNP** отдельно читаются **все** срезы месяца (`all_snapshots`) — см. [`build_stg_msisdn_operator.md`](./build_stg_msisdn_operator.md).

```bash
uv run mobile build-stg-person --report-date 2025-01-01
```

Опционально до person — прогнать binding по дням месяца или один раз:

```bash
uv run mobile build-stg-msisdn-operator --report-date 2025-01-01
uv run mobile build-stg-msisdn-imsi --report-date 2025-01-01   # и за 02, 03, …
# либо пересборка месяца из geo:
uv run mobile build-stg-msisdn-imsi-month --report-date 2025-01-01
```

Логи: `data/logs/mobile.log`. Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-person`.

---

## Структура генерируемых витрин

| Витрина | Путь | Гранулярность |
|---------|------|----------------|
| `stg_person` | `data/stg/person/{YYYY-MM-01}.parquet` | 1 строка на `person_id` |
| `stg_person_sim` | `data/stg/person_sim/{YYYY-MM-01}.parquet` | 1 строка на подписку/SIM-интервал |
| `stg_person_id_ledger` | `data/stg/person_id_ledger/{YYYY-MM-01}.parquet` | Узлы графа → `person_id` |

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
| 11 | `citizenship` | string | Код страны или `U` |
| 12 | `operator_id` | long | Оператор основной подписки |
| 13 | `actually_from` | timestamp | Начало интервала основной SIM |
| 14 | `actually_to` | timestamp | Конец интервала |

### Поля `stg_person_sim`

`person_id`, `msisdn`, `imsi`, `imei`, `iccid`, `operator_id`, `contract_number`, `actually_from`, `actually_to`, `is_primary` — см. [`person_sim.json`](../../src/mobile/schema/stg/person_sim.json).

### Поля `stg_person_id_ledger`

`person_id`, `person_cluster_key`, `node` (`bio:`, `contract:`, `iccid:`, `msisdn:`, …) — см. [`person_id_ledger.json`](../../src/mobile/schema/stg/person_id_ledger.json).

---

## Источники витрины

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `src_person` (latest) | `data/src/person/.../person.parquet` | Профиль, bio, contract, iccid |
| 2 | `src_person` (all snapshots) | те же `load_day` | MNP → operator-витрина |
| 3 | `stg_msisdn_operator` | `data/stg/msisdn_operator/{YYYY-MM-01}.parquet` | Рёбра MNP |
| 4 | `stg_msisdn_imsi` | `data/stg/msisdn_imsi/{YYYY-MM-01}.parquet` | Связи MSISDN↔IMSI (месяц, daily upsert) |
| 5 | `stg_msisdn_imei` | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Связи MSISDN↔IMEI |
| 6 | `stg_tac` | `data/stg/tac.parquet` | M2M |
| 7 | ledger (прошлый месяц) | `data/stg/person_id_ledger/{prev YYYY-MM-01}.parquet` | Стабильный `person_id` |

Документация вспомогательных сборок:

- [`build_stg_msisdn_operator.md`](./build_stg_msisdn_operator.md)
- [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md)
- [`build_stg_msisdn_imei.md`](./build_stg_msisdn_imei.md)

---

## Алгоритм обработки данных

Точка входа: `run_build(report_date, …)` в [`person.py`](../../src/mobile/pipelines/stg/person.py).

### Шаг 0. Инициализация

1. Валидация: `report_date.day == 1` (`_validate_report_month`).
2. Период месяца: `period_start = report_date`, `period_end` = последний календарный день.
3. `month_start` / `month_end` как `pd.Timestamp` (конец месяца 23:59:59 для фильтров).
4. Разрешение путей:
   - `person_out` → `data/stg/person/{YYYY-MM-01}.parquet`;
   - `sim_out` → `person_sim/…`;
   - `ledger_out` → `person_id_ledger/…`;
   - `operator_out`, `imsi_month_path`, `imei_month_path`.
5. Загрузка контракта полей из [`person.json`](../../src/mobile/schema/stg/person.json).
6. Старт `timed_stage` и счётчиков (`src_rows_before_exclusions`, `excluded_m2m_tac_rows`, …).

### Шаг 1. Чтение `src_person` (профиль)

1. `read_src_person_month(..., mode="latest_snapshot")`:
   - обход `load_day=*` в `[period_start, period_end]` с `_SUCCESS`;
   - выбор **максимального** `load_day`;
   - один `person.parquet` (не concat всех срезов).
2. Метрика `src_load_days` — какие `load_day` участвовали в выборе.
3. При отсутствии среза — `FileNotFoundError`.

### Шаг 2. Исключение M2M по TAC

1. Чтение `stg_tac.parquet` (`tac`, `is_m2m`); множество `m2m_tacs`.
2. Для каждой строки: `imei_tac = первые 8 цифр IMEI` после нормализации цифр.
3. Удаление строк, где `imei_tac ∈ m2m_tacs`; счётчик `excluded_m2m_tac_rows`.
4. Если файла TAC нет или нет колонок — **warning**, фильтр не применяется.

### Шаг 3. Витрина `stg_msisdn_operator` (MNP)

1. Если `build_operator_vitrine=true`:
   - повторное чтение `src_person` в режиме **`all_snapshots`** (concat всех `load_day` месяца);
   - повторный M2M-фильтр;
   - `build_operator_intervals_from_src` → группировка `(msisdn, operator_id, imsi)`:
     - `valid_from = min(actually_from)`, `valid_to = max(actually_to)`;
     - только `client_type=0`, интервал ∩ месяц.
2. Запись `operator_out` (parquet snappy).
3. Если витрина уже есть и `build_operator_vitrine=false` — чтение с диска.
4. **Рёбра MNP** в графе строятся только как `msisdn`↔`imsi` на интервале operator (не по одному `operator_id`).

### Шаг 4. Месячные binding MSISDN↔IMSI/IMEI

1. Пути: `stg_msisdn_imsi_output_path(report_month)` → `…/msisdn_imsi/{YYYY-MM-01}.parquet` (месячный файл).
2. Если `build_bindings_month=true` и файла нет — `refresh_month_bindings_from_geo`:
   - для каждого дня месяца с `stg_geo_all` вызвать `build-stg-msisdn-imsi` и `build-stg-msisdn-imei` (инкремент в month parquet).
3. `_read_binding_parquet` — нормализация `msisdn`/`imsi`/`imei`, `valid_from`/`valid_to`.

### Шаг 5. Ledger прошлого месяца

1. `_previous_report_month(report_month)` → 1-е число предыдущего месяца.
2. `_load_previous_ledger`: чтение `person_id_ledger` прошлого месяца (если есть) — колонки `person_id`, `person_cluster_key`, `node`.

### Шаг 6. Подготовка подписок (`_prepare_subscriptions`)

1. **Фильтр ФЛ:** `client_type == 0`.
2. **Пересечение с месяцем:** `actually_from <= month_end` и `actually_to >= month_start`; `actually_to` без значения → `2999-12-31`.
3. **Нормализация ID:**
   - `msisdn` ← `normalize_msisdn(isdn)`;
   - `imsi`, `imei`, `iccid`, `contract_number` — строковые поля;
   - `operator_id` ← `operator_Id`.
4. **Binding-fill** на момент `binding_at` = конец последнего дня месяца (`_enrich_identifiers_from_bindings`):
   - для пустого `imsi` — lookup по `msisdn` в `stg_msisdn_imsi` (интервал содержит `at`);
   - симметрично `msisdn`←`imsi`, `imei`↔`msisdn`;
   - при нескольких интервалах — запись с **максимальным** `valid_from` (самая свежая привязка);
   - метрики `binding_fill` (сколько полей дозаполнено).
5. Отбор строк с полным ключом: `msisdn`, `imsi`, `imei`, `operator_id`, интервалы не null.

### Шаг 7. Кластеризация (`_assign_clusters`, union-find)

1. Инициализация `UnionFind`.
2. **Узлы co-occurrence** на каждой строке `src_person`:
   - `_unite_pair_column`: `msisdn↔imsi`, `msisdn↔imei`, `msisdn↔iccid`;
   - `bio_key` = `bio:фамилия|имя|отчество|дата_рождения|цифры_документа` (casefold, только при валидном ФИО+ДР);
   - `msisdn↔bio`, `msisdn↔contract:{номер}`.
3. **Рёбра из binding** (`binding_edges_in_month`):
   - все пары `(msisdn, imsi)` / `(msisdn, imei)`, у которых `[valid_from, valid_to]` пересекает `[month_start, month_end]`.
4. **Рёбра MNP** (`operator_observation_edges`):
   - для каждой строки operator-витрины: `union(msisdn:…, imsi:…)` если IMSI не пуст.
5. **Канонический ключ кластера** (`canonical_cluster_key`):
   - приоритет: первый лексикографически `bio:` → `contract:` → `iccid:` → иначе `min(все узлы кластера)`.
6. Для каждой строки: `roots` = `find()` по всем узлам строки; `person_cluster_key` = canonical корня `min(roots)`; `person_confidence` по типам узлов в кластере.

### Шаг 8. Назначение `person_id` (`assign_person_ids_with_ledger`)

1. Из ledger прошлого месяца: индексы `node → person_id` и `person_cluster_key → person_id`.
2. Для каждого `person_cluster_key` текущего месяца:
   - если ключ уже в ledger → тот же `person_id`;
   - иначе если **любой** узел кластера встречался в ledger → взять его `person_id`;
   - иначе `person_id = prs_` + SHA256(`person_cluster_key`)[:24].
3. Обновление индексов для всех узлов кластера (для следующих строк того же месяца).

### Шаг 9. Выходные витрины (`_build_outputs`)

1. **`stg_person_sim`:**
   - все строки `work` с `report_date`, `person_id`;
   - `is_primary=true` для строки с **максимальным** `actually_from` внутри `person_id` (последняя активная подписка);
   - остальные `is_primary=false`.
2. **`stg_person`:**
   - одна строка на `person_id` — из primary-строки;
   - `sim_count` = `nunique(imsi|iccid)` по подпискам персоны;
   - `gender` ← `_derive_gender` (по полю/ФИО);
   - `age` ← возраст на `month_start` из `birth_day` или `U`;
   - `citizenship` ← `_derive_citizenship_from_row` или `U`.
3. **`stg_person_id_ledger`:**
   - для каждого `(person_cluster_key, person_id)` — по одной строке на каждый узел графа (`node` = `msisdn:…`, `bio:…`, …);
   - снимок для стабильности ID в следующем месяце.

### Шаг 10. Запись и метрики

1. `to_parquet` для трёх витрин (snappy).
2. `append_command_metrics`: `elapsed_*_sec`, `stg_rows_written`, `person_sim_rows`, `ledger_rows`, пути входов.

### Типовые ошибки

| Ситуация | Поведение |
|----------|-----------|
| `report_date` не 1-е число | `ValueError` / `SystemExit` |
| Нет `_SUCCESS` за месяц | `FileNotFoundError` |
| Нет `stg_tac` | warning, M2M не фильтруется |
| Нет `stg_geo_all` за дни | пустые/частичные binding, слабый fill |
| Нет bio и нет связующих рёбер | отдельные кластеры по tech ID, `person_confidence=low` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`person.py`](../../src/mobile/pipelines/stg/person.py) |
| Граф / ID | [`person_identity.py`](../../src/mobile/pipelines/stg/person_identity.py) |
| Чтение src | [`src_person_month.py`](../../src/mobile/pipelines/stg/src_person_month.py) |
| DQ (план) | [`dq_stg_person.md`](../dq/stg/dq_stg_person.md) |
| Источник | [`build_src_person.md`](../src/build_src_person.md) |
| Пути | [`project_paths.py`](../../src/mobile/project_paths.py) |
