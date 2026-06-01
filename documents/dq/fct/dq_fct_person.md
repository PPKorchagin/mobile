# dq-fct-person

**Витрина:** `fct_person` · **Команда:** `dq-fct-person` · **Режим:** read-only DQ (не изменяет данные, не падает при failed checks).

Референс: [`pipelines/dq/stg/person.py`](../../../src/mobile/pipelines/dq/stg/person.py). Сборка: [`build_fct_person.md`](../../fct/build_fct_person.md). Схема: [`person.json`](../../../src/mobile/schema/fct/person.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти месячный parquet `fct_person` за `report_date` | Путь `{YYYY-MM-01}.parquet` |
| 2 | Проверить контракт колонок и null-профиль | Логи `DQ_FCT_PERSON` |
| 3 | Проверить ключ `person_id`, `report_date`, домены демографии и SIM | Gate-статусы `ok/warning/failed` |
| 4 | Сверить `citizenship` со справочником ОКСМ | `domain.citizenship_oksm` |
| 5 | Выдать `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества месячного профиля физлиц после [`build-fct-person`](../../fct/build_fct_person.md) (кластеризация ID+bio+bindings, M2M-отсечение, `sim_count`) перед downstream-аналитикой абонентской базы.

**В scope:** наличие файла, контракт `FCT_PERSON_FIELDS`, null-профиль критичных и демографических полей, уникальность и формат `person_id`, единый `report_date` в срезе, домены `gender`/`age`/`citizenship`/`person_confidence`/`sim_count`, цифровой формат `msisdn`/`imsi`/`imei`, сверка кодов гражданства с `dim_oksm`.

---

## TODO

1. Сверка `person_id` с **прошлым** месяцем `fct_person` (стабильность между срезами).
2. Динамические пороги `warning/failed` по baseline (доля `person_confidence=low`, null в демографии).

---

## Параметры запуска

Вызов: `run_dq(report_date, fct_person_path, dim_oksm_path?)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-fct-person`). **`report_date` и `fct_person_path` обязательны** при явном прогоне — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Любой календарный день; pipeline приводит к **1-му числу месяца** (`report_month_start`) |
| `fct_person_path` | path | **Да** | Месячный parquet или каталог `data/fct/person` (для каталога — файл `{YYYY-MM-01}.parquet`) |
| `dim_oksm_path` | path | Нет | `data/dim/oksm.parquet` — проверка `domain.citizenship_oksm`; по умолчанию `DEFAULT_DIM_OKSM_OUTPUT_PATH` в коде |

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../../src/mobile/cli_defaults.py)); **один прогон на календарный месяц**, если `fct_person_output_path(month)` существует; timed-run `dq-fct-person-{YYYY-MM-01}` (`dim_oksm` — default в pipeline) |
| Оба явно | `--report-date` (любой день, например `2025-01-15` → месяц `2025-01-01`) и `--fct-person-path`; опционально `--dim-oksm-path` |

**Константы DQ в коде** ([`person.py`](../../../src/mobile/pipelines/dq/stg/person.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `LOG_TAG` | `DQ_FCT_PERSON` |
| `_PERSON_COLUMNS` | имена полей из `FCT_PERSON_FIELDS` ([`stg/person.py`](../../../src/mobile/pipelines/stg/person.py)) |
| `_PERSON_CRITICAL_NULLS` | `person_id`, `person_cluster_key`, `report_date`, `msisdn`, `imsi`, `imei`, `operator_id` — null → **failed** |
| `_PERSON_DEMO_NULLS` | `gender`, `age`, `citizenship` — null → **warning** |
| `_PERSON_ID_RE` | `^prs_[0-9a-f]{24}$` |
| `_LOW_CONFIDENCE_WARN_RATIO` | `0.30` — доля `person_confidence=low` |
| `_GENDER_VALUES` | `M`, `F`, `U` |
| `_CONFIDENCE_VALUES` | `high`, `medium`, `low` |

**Предусловие:** `uv run mobile build-fct-person --report-date YYYY-MM-01` за тот же месяц (binding `fct_msisdn_imsi` / `fct_msisdn_imei`, `src_person`, `src_excl`, `dim_oksm`).

Локальный запуск:

```bash
uv run mobile build-dim-oksm
uv run mobile build-fct-msisdn-imei
uv run mobile build-fct-msisdn-imsi-operator
uv run mobile build-fct-person --report-date 2025-01-01
uv run mobile dq-fct-person
uv run mobile dq-fct-person --report-date 2025-01-15 \
  --fct-person-path data/fct/person/2025-01-01.parquet
uv run mobile dq-fct-person --report-date 2025-01-01 \
  --fct-person-path data/fct/person \
  --dim-oksm-path data/dim/oksm.parquet
uv run mobile nb-fct-person
```

Логи: `data/logs/mobile.log` (тег `DQ_FCT_PERSON`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-fct-person` или `dq-fct-person-{YYYY-MM-01}`. Визуализация: `nb-fct-person` → `data/notebooks/15_fct_person.executed.ipynb`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `fct_person` — [`person.json`](../../../src/mobile/schema/fct/person.json) |
| Путь по умолчанию | `data/fct/person/{YYYY-MM-01}.parquet` |
| Формат | Parquet (`snappy`) |
| Гранулярность | Месячный срез (`report_date` = 1-е число месяца) |
| Контракт полей | `FCT_PERSON_FIELDS` из [`pipelines/stg/person.py`](../../../src/mobile/pipelines/stg/person.py) |

### Поля (контракт)

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `report_date` | date | 1-е число отчётного месяца |
| 2 | `person_id` | string | `prs_` + SHA256(`person_cluster_key`) |
| 3 | `person_cluster_key` | string | Канонический ключ кластера (bio / SIM / tech) |
| 4 | `person_confidence` | string | `high` / `medium` / `low` |
| 5 | `sim_count` | long | Число distinct SIM (imsi\|iccid) у персоны в месяце |
| 6 | `msisdn` | string | Основной MSISDN (последний интервал) |
| 7 | `imsi` | string | Основной IMSI |
| 8 | `imei` | string | Основной IMEI |
| 9 | `gender` | string | `M` / `F` / `U` |
| 10 | `age` | string | Возраст на начало месяца или `U` |
| 11 | `citizenship` | string | `numeric_code` ОКСМ или `U` |
| 12 | `operator_id` | long | `operator_id` основной подписки |
| 13 | `actually_from` | timestamp | Начало интервала основной подписки |
| 14 | `actually_to` | timestamp | Конец интервала основной подписки |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `fct_person` | `data/fct/person/{YYYY-MM-01}.parquet` | Месячный профиль после `build-fct-person` |
| 2 | `dim_oksm` | `data/dim/oksm.parquet` | Справочник кодов гражданства (`domain.citizenship_oksm`) |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `report_month_start(report_date)` — входная дата → 1-е число месяца (в метриках при отличии — `report_date_input`).
2. `_resolve_source_path(report_month, fct_person_path)` — каталог → `{YYYY-MM-01}.parquet`, иначе файл как есть (`resolve_stg_monthly_parquet_path`).
3. `dim_oksm_path` → `DEFAULT_DIM_OKSM_OUTPUT_PATH`, если не передан.
4. Счётчики `total_checks`, `warning_checks`, `failed_checks`.

### Шаг 1. Наличие набора

Нет файла → `dataset_presence` (**failed**), `summary`, return.  
Иначе `pd.read_parquet` → `dataset_basic` (**ok**: `row_count`, `column_count`, `distinct_person_id`).

### Шаг 2. Схема и null-профиль

1. `schema_columns` — все поля `_PERSON_COLUMNS` (**failed** при `missing_columns`; **warning** при `extra_columns`).
2. `nulls.{field}` для `_PERSON_CRITICAL_NULLS` — любой null → **failed**.
3. `nulls.{field}` для `_PERSON_DEMO_NULLS` — null → **warning**.

Пустой DataFrame после шага 2 → `summary`, return (без gate по доменам).

### Шаг 3. Gate-проверки

1. `key.person_id_unique` — `row_count == nunique(person_id)` (**failed**).
2. `key.person_id_format` — `person_id` match `^prs_[0-9a-f]{24}$` (**failed**).
3. `key.report_date_single` — ровно одно значение `report_date` = `report_month` (**failed**).
4. `domain.gender` — ⊆ `{M, F, U}` (**failed**).
5. `domain.age` — целое 0–120 или `U` (**failed**).
6. `domain.citizenship` — непусто; `U` или `^\d{3}$` (**failed**).
7. `domain.citizenship_oksm` — коды (кроме `U`) ∈ `dim_oksm.numeric_code` (**failed**); нет файла ОКСМ → **warning**.
8. `domain.person_confidence` — ⊆ `{high, medium, low}` (**failed**).
9. `distribution.person_confidence` — доля `low` > 30% → **warning**.
10. `domain.sim_count` — `sim_count >= 1` (**failed**).
11. `domain.{msisdn,imsi,imei}_digits` — непустые значения только из цифр (**warning**).

### Шаг 4. Итог

`summary` и return dict со статусом прогона. CLI не падает при failed checks.

---

## Проверки

Формат лога: `{"tag":"DQ_FCT_PERSON","check":"...","status":"...","metrics":{...}}`.

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet за месяц не найден | Нет среза после [`build-fct-person`](../../fct/build_fct_person.md) |
| `dataset_basic` | **ok** | `row_count`, `column_count`, `distinct_person_id`, путь | Базовый объём для сравнения прогонов |
| `schema_columns` | **failed** / **warning** | `missing_columns` / `extra_columns` | Контракт совпадает с ETL и [`person.json`](../../../src/mobile/schema/fct/person.json) |
| `nulls.*` (критичные) | **failed** | null в ключе / подписке | Персона без ID, кластера или основной SIM бесполезна |
| `nulls.*` (демо) | **warning** | null в `gender`/`age`/`citizenship` | Допустимо `U`, но null — деградация профиля |
| `key.person_id_unique` | **failed** | дубликаты `person_id` | Одна строка = одна персона в месяце |
| `key.person_id_format` | **failed** | неверный префикс/длина hash | Согласованность с ETL `prs_` + 24 hex |
| `key.report_date_single` | **failed** | несколько `report_date` или не тот месяц | Месячный срез смешан с другим |
| `domain.gender` | **failed** | значение вне M/F/U | Контракт демографии |
| `domain.age` | **failed** | не число и не `U`, или вне 0–120 | Возраст на начало месяца |
| `domain.citizenship` | **failed** | пусто или не `U`/трёхзначный код | ОКСМ или unknown |
| `domain.citizenship_oksm` | **failed** / **warning** | неизвестный код / нет `dim_oksm` | Сверка со справочником [`build-dim-oksm`](../../dim/build_dim_oksm.md) |
| `domain.person_confidence` | **failed** | не high/medium/low | Сила идентификации кластера |
| `distribution.person_confidence` | **warning** | доля `low` > 30% | Много слабых кластеров — риск для аналитики |
| `domain.sim_count` | **failed** | `sim_count < 1` | Инвариант ETL после union-find |
| `domain.msisdn_digits` / `imsi` / `imei` | **warning** | не только цифры | Нормализация в ETL / binding |
| `summary` | **ok** | счётчики checks | Сводка прогона |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/stg/person.py`](../../../src/mobile/pipelines/dq/stg/person.py) |
| DQ notebook | [`pipelines/nb/15_fct_person.ipynb`](../../../src/mobile/pipelines/nb/15_fct_person.ipynb) |
| ETL build | [`pipelines/stg/person.py`](../../../src/mobile/pipelines/stg/person.py) |
| Пути layout | [`project_paths.py`](../../../src/mobile/project_paths.py) |
| CLI | [`cli.py`](../../../src/mobile/cli.py) |
| Схема | [`person.json`](../../../src/mobile/schema/fct/person.json) |
| Сборка (док) | [`build_fct_person.md`](../../fct/build_fct_person.md) |
| DQ binding IMEI | [`dq_fct_msisdn_imei.md`](./dq_fct_msisdn_imei.md) |
| DQ binding IMSI | [`dq_fct_msisdn_imsi_operator.md`](./dq_fct_msisdn_imsi_operator.md) |
| Справочник ОКСМ | [`build_dim_oksm.md`](../../dim/build_dim_oksm.md) |

Сквозная цепочка: `build-stg-geo-all` → `build-fct-msisdn-imei` → **`dq-fct-msisdn-imei`** → `build-fct-msisdn-imsi-operator` → **`dq-fct-msisdn-imsi-operator`** → **`build-fct-person`** → **`dq-fct-person`** → **`nb-fct-person`** → downstream.
