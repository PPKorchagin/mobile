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

### Шаг 0. Инициализация

Разрешить пути; загрузить parquet; счётчики ok/warning/failed.

### Шаг 1. Наличие и базовый профиль

`dataset_basic`: rows, `distinct_person_id`, `report_date`.

### Шаг 2. Контракт и nulls

`schema_columns` по [`person.json`](../../../src/mobile/schema/stg/person.json); `nulls.*` для ключевых полей.

### Шаг 3. Ключевая целостность

- `person_id` unique;
- `person_id` + `report_date` (несколько report_date в одном файле — failed).

### Шаг 4. Домены

`domain.gender`, `domain.age`, `domain.citizenship`.

### Шаг 5. Связь с SIM

Join с `person_sim`: покрытие, `sim_count` vs фактическое число строк.

### Шаг 6. Summary

JSON-логи `{"tag":"DQ_STG_PERSON","check":...,"status":...,"metrics":...}`.

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
