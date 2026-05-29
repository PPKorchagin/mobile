# dq-stg-person

**Витрины:** `stg_person`, `stg_person_sim` · **Команда:** `dq-stg-person` · **Режим:** read-only DQ (процесс не падает при failed checks).

> **Статус:** спецификация и чек-лист; CLI-команда `dq-stg-person` **ещё не реализована** в `mobile`. Сборка — [`build_stg_person.md`](../../stg/build_stg_person.md).

Референс (план): `src/mobile/pipelines/dq/stg/person.py` (будущий). Схемы: [`person.json`](../../../src/mobile/schema/stg/person.json), [`person_sim.json`](../../../src/mobile/schema/stg/person_sim.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти parquet `stg_person` за `report_date` | Путь к месячному срезу |
| 2 | Проверить контракт, nulls, домены | Логи `DQ_STG_PERSON` |
| 3 | Уникальность `person_id` в месяце | Ключевая целостность |
| 4 | Согласованность `stg_person` ↔ `stg_person_sim` | `sim_count`, покрытие SIM |
| 5 | Выдать `summary` | Счётчики checks |

**Бизнес-назначение:** QA месячного демографического слоя после M2M-отсечения и кластеризации персон.

**В scope:** контракт колонок, домены `gender`/`age`/`citizenship`, ключ `person_id`, базовые распределения, связь с `person_sim`.

---

## TODO

1. Реализовать `pipelines/dq/stg/person.py` и зарегистрировать в CLI.
2. Проверки ledger: согласованность `person_id` с узлами прошлого месяца.
3. Пороги warning для `person_confidence=low`.

---

## Параметры запуска (план)

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | **`YYYY-MM-01`** |
| `stg_person_path` | path | Нет | `data/stg/person/{YYYY-MM-01}.parquet` | Профиль |
| `stg_person_sim_path` | path | Нет | `data/stg/person_sim/{YYYY-MM-01}.parquet` | Подписки |

```bash
# после реализации:
uv run mobile dq-stg-person --report-date 2025-01-01
```

Логи (план): тег `DQ_STG_PERSON`. Метрики: `command=dq-stg-person`.

---

## Структура проверяемых витрин

### `stg_person`

| Поле | Проверки |
|------|----------|
| `report_date` | = 1-е число месяца |
| `person_id` | уникален в файле; префикс `prs_` |
| `person_cluster_key` | not null |
| `person_confidence` | ∈ {high, medium, low} |
| `sim_count` | ≥ 1 |
| `msisdn`, `imsi`, `imei` | нормализованные цифры |
| `gender` | M, F, U |
| `age` | 0–120 или U |
| `citizenship` | код или U |

### `stg_person_sim`

| Проверка | Ожидание |
|----------|----------|
| `person_id` ⊆ `stg_person.person_id` | нет «сирот» |
| `is_primary` | ровно одна `true` на `person_id` |
| Строк на `person_id` | ≥ 1; `sim_count` в person ≈ distinct (imsi\|iccid) |

---

## Источники

| # | Источник | Путь |
|---|----------|------|
| 1 | `stg_person` | `data/stg/person/{YYYY-MM-01}.parquet` |
| 2 | `stg_person_sim` | `data/stg/person_sim/{YYYY-MM-01}.parquet` |
| 3 | (опц.) ledger | `data/stg/person_id_ledger/{YYYY-MM-01}.parquet` |

---

## Алгоритм обработки данных (план)

Планируемая точка входа: `run_dq(report_date, …)` в `pipelines/dq/stg/person.py` (ещё не в CLI). Референс логики: `synthetic_data/documents/dq/dq_stg_person.md`.

### Шаг 0. Инициализация

1. Валидация `report_date.day == 1`.
2. Разрешение путей:
   - `stg_person_path` → `data/stg/person/{YYYY-MM-01}.parquet`;
   - `stg_person_sim_path` → `data/stg/person_sim/{YYYY-MM-01}.parquet`;
   - опционально ledger.
3. Инициализация счётчиков `ok` / `warning` / `failed` и списка результатов checks.
4. Если person parquet отсутствует → `dataset_presence` **failed**, досрочный `summary`, return (процесс не падает).

### Шаг 1. Наличие и базовый профиль

1. `pd.read_parquet` person и person_sim.
2. Check `dataset_basic`:
   - `row_count`, `column_count`, `parquet_path`;
   - `distinct_person_id` = `nunique(person_id)`;
   - `distinct_report_date` — ожидается 1 значение = `report_date`;
   - `min_report_date`, `max_report_date`.

### Шаг 2. Контракт и nulls

1. `schema_columns`: множество колонок parquet ⊇ полей из [`person.json`](../../../src/mobile/schema/stg/person.json); лишние колонки — **warning** (опционально).
2. Для каждого обязательного поля `nulls.{field}`:
   - `person_id`, `person_cluster_key`, `report_date`, `msisdn`, `imsi`, `imei`, `operator_id`;
   - порог: `null_ratio > 0` для `person_id` → **failed**; для демографии — **warning**.

### Шаг 3. Ключевая целостность

1. `key.person_id_unique`: `row_count == nunique(person_id)`; иначе **failed** + `duplicate_person_id_count`.
2. `key.person_id_format`: все `person_id` match `^prs_[0-9a-f]{24}$`; иначе **failed**.
3. `key.report_date_single`: ровно одно значение `report_date` в файле и оно равно параметру CLI.
4. Для `person_sim`:
   - `person_id` ⊆ `stg_person.person_id` (нет сирот);
   - иначе **failed** + `orphan_sim_rows`.

### Шаг 4. Домены и бизнес-правила

1. `domain.gender`: значения ⊆ `{M, F, U}`.
2. `domain.age`: целое 0–120 или `U` (строка).
3. `domain.citizenship`: непустая строка, допуск `U`.
4. `domain.person_confidence`: ⊆ `{high, medium, low}`.
5. `domain.sim_count`: `sim_count >= 1`.
6. `distribution.person_confidence`: если доля `low` > порога (например 30%) → **warning**.

### Шаг 5. Связь person ↔ person_sim

1. Join `person` left join agg(`person_sim`) по `person_id`:
   - `sim_rows` = count строк sim;
   - `distinct_sim_keys` = `nunique(imsi|iccid)`.
2. Check `sim_count_consistency`: `person.sim_count` ≈ `distinct_sim_keys` (допуск 0); иначе **warning**.
3. Check `primary_sim`: ровно одна строка `is_primary=true` на `person_id`; 0 или >1 → **failed**.
4. Check `primary_matches_profile`: MSISDN/IMSI/IMEI в `stg_person` совпадают с primary-строкой sim (после нормализации).

### Шаг 6. Ledger (опционально)

1. Если передан ledger: каждый `person_id` в person имеет ≥1 узла в ledger.
2. Узлы `node` имеют ожидаемый префикс (`bio:`, `msisdn:`, …).

### Шаг 7. Summary и timing

1. Check `summary`: `total_checks`, `warning_checks`, `failed_checks`.
2. Логи: `{"tag":"DQ_STG_PERSON","check":"...","status":"ok|warning|failed","metrics":{...}}`.
3. `append_command_metrics(command="dq-stg-person", …)`.

### Типовые ошибки

| Ситуация | Статус |
|----------|--------|
| Нет parquet | failed |
| Дубликаты `person_id` | failed |
| Много `person_confidence=low` | warning |
| `sim_count` расходится с person_sim | warning |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Сборка | [`build_stg_person.md`](../../stg/build_stg_person.md) |
| Схема person | [`person.json`](../../../src/mobile/schema/stg/person.json) |
| Схема sim | [`person_sim.json`](../../../src/mobile/schema/stg/person_sim.json) |
| Референс synthetic | `synthetic_data/documents/dq/dq_stg_person.md` |
