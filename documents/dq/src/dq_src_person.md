# dq-src-person

**Витрина:** `src_person` · **Команда:** `dq-src-person` · **Режим:** read-only DQ по календарному диапазону (процесс не падает при failed checks).

Референс: [`pipelines/dq/src/person.py`](../../../src/mobile/pipelines/dq/src/person.py). Сборка витрины: [`build_src_person.md`](../../src/build_src_person.md).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Обойти суточные каталоги `src_person` в периоде | Метрики покрытия `_SUCCESS` и объёма |
| 2 | Построить агрегаты `period.*` по всем дням диапазона | Распределения identity/client/operator |
| 3 | Выполнить глубокий профиль на одном выбранном дне | null/cardinality, доменные checks по фактическим колонкам |
| 4 | Сформировать `summary` | Счётчики checks и итоговый статус |

**Бизнес-назначение:** проверить качество синтетической витрины Person по дням и контракт трансформации в `stg_person`.

**В scope задач:** покрытие каталогов и `_SUCCESS`, period-агрегаты, профили полей, форматы MSISDN/IMSI/IMEI, temporal/FIO/stg_contract.

---

## TODO

1. При необходимости вынести пороги `stg_contract.*` в конфиг.

---

## Параметры запуска

Вызов: `run_dq(start_date, end_date, person_root)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-src-person`). Конец периода (`end_date`) **не передаётся в CLI** — вычисляется как последний день месяца из `start_date` ([`calendar_month_end`](../../../src/mobile/project_paths.py)).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `start_date` | date | Нет | `2025-01-01` | Начало периода DQ (`--start-date`) |
| `person_root` | path | Нет | `data/src/person` | Корень суточных каталогов (`--src-person-path`) |

Локальный запуск:

```bash
uv run mobile dq-src-person
uv run mobile dq-src-person --start-date 2025-01-01 --src-person-path data/src/person
uv run mobile nb-src-person
```

Логи: `data/logs/mobile.log` (тег `DQ_SRC_PERSON`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-src-person`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя | `person` |
| Формат | Parquet по суточным каталогам |
| Layout | `data/src/person/load_year={YYYY}/load_month={MM}/load_day={DD}/person.parquet` |
| Маркер full snapshot | `_SUCCESS` в каталоге дня |
| Набор полей | Все фактические колонки parquet выбранного дня |
| Ключевые домены | MSISDN/IMSI/IMEI, identity_type, temporal, FIO, operator |

**День глубокого профиля:** последний день с `_SUCCESS` в диапазоне, иначе последний день диапазона.

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | Суточные каталоги `src_person` | `data/src/person` (`--src-person-path`) | `person.parquet` и опционально `_SUCCESS` по дням; все метрики считаются по фактическим данным parquet |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Резолв `start_date` (по умолчанию `2025-01-01`) и `end_date` = последний день месяца `start_date`.
2. Резолв `person_root` (по умолчанию `data/src/person`).
3. Обход каталогов `load_year=*/load_month=*/load_day=*` под корнем.
4. Фильтрация дней по периоду → `dataset_filter`, `period.calendar_coverage`.

### Шаг 1. Покрытие каталогов (весь диапазон)

1. `day.coverage` — `has_parquet`, `has_success`, `row_count` по каждому дню.
2. Full snapshot определяется **только по наличию `_SUCCESS`** в каталоге дня; предрасчёт «ожидаемых» дат из параметров build не используется.
3. `success_days_presence`, `success_days_inventory`, `success_days_share`, `latest_success_day`, `latest_calendar_day_has_success`.
4. `period.volume`, `period.identity_aggregates`, `period.distribution.*`, `period.cross.identity_type_x_client_type`.

### Шаг 2. Глубокий профиль выбранного дня

1. Чтение `person.parquet` последнего дня с `_SUCCESS`, иначе последнего дня диапазона.
2. `dataset_presence`, `dataset_basic`, `dataset_columns`.
3. Для каждой фактической колонки: `nulls.*`, `cardinality.*`, `unique_values.*`, `numeric_profile.*`, `distribution.*`; сводка `field_profile_coverage`.
4. Помесячные профили temporal-полей: `distribution.{col}_month`.
5. Длины строк: `string_length.{service_list,last_geo,geo_json}`.

### Шаг 3. Доменные и контрактные проверки

1. **`stg_contract.*`:** критичные поля для трансформации в [`build-stg-person`](../../stg/build_stg_person.md); заполненность `isdn`/`imsi`/`imei`/FIO у физлиц.
2. **`identity_type.*`:** заполненность полей по типу identity; `identity_type.non_gsm_isdn_leak`.
3. **Ключи и форматы:** `key_integrity.operator_isdn`, `identity_duplicate_keys`, `isdn_format`, `imsi_format`, `imei_format`, `iccid_format`, `passport_format`.
4. **Temporal:** `temporal_consistency`, `actually_to_open_interval` (`ACTUALLY_TO_OPEN` = `2261-12-31 23:59:59`), `closed_contract_ratio`.
5. **Geo/услуги:** `lac_cell_by_last_location`, `service_list_format`.
6. **Демография:** `birth_day_quality`, `fio_quality_physical`, `fio_quality_corporate`, `citizenship_fields_presence`.
7. Каждый check логируется: `{"tag":"DQ_SRC_PERSON","check":"...","status":"...","metrics":{...}}`.

### Шаг 4. Итог

`summary` с `total_checks`, `warning_checks`, `failed_checks`; return dict со `status`, `success_days_count`, `day_dirs_in_range`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `dataset_filter` **warning** | Нет каталогов в периоде |
| `success_days_presence` **failed** | Ни один день без `_SUCCESS` |
| `dataset_presence` **failed** | Нет parquet на выбранном дне |
| `start_date` > `end_date` | `dataset_filter` **failed**, ранний выход |
| Битый parquet | исключение pandas/pyarrow |

---

## Проверки

Статусы: **ok** / **warning** / **failed** (`nulls.*`, `cardinality.*`, профили распределений — всегда **ok**).

### Каталоги и `_SUCCESS` (весь диапазон)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_filter` | **warning** | Нет каталогов в периоде | Фиксация объёма среза до профиля полей |
| `day.coverage` | ok | Parquet и `_SUCCESS` по каждому дню | Контроль полноты сборки `build-src-person` |
| `success_days_presence` | **failed** | Нет ни одного `_SUCCESS` | Full snapshot нужен для `build-src-mobile` и STG |
| `success_days_inventory` | ok | Список календарных дней с `_SUCCESS` | Фактическое покрытие full snapshot по флагу |
| `success_days_share` | ok | Доля дней с `_SUCCESS` в периоде | Описательная метрика плотности full snapshot |
| `latest_success_day` | **failed** | Нет дней с `_SUCCESS` | Контроль наличия актуального full snapshot |
| `latest_calendar_day_has_success` | **warning** | На последнем дне периода нет `_SUCCESS` | Хвост периода без full snapshot |

### Агрегаты по периоду (`period.*`)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `period.calendar_coverage` | **warning** | Пропуски календарных дней в layout | Дыры в суточной витрине ломают downstream-циклы |
| `period.volume` | ok | min/p50/p95/max `row_count` по дням; split success/partial | Профиль объёма и соотношение full vs partial дней |
| `period.identity_aggregates` | ok / **warning** | Скан лёгких колонок по всем дням периода | Агрегированные распределения без полного read всех parquet |
| `period.distribution.{dim}` | ok | Распределения `identity_type`, `client_type`, `operator_Id`, … | Стабильность доменов по всему периоду |
| `period.cross.identity_type_x_client_type` | ok | Кросс-таблица identity × client_type | Sanity mix типов абонентов |

### Базовые проверки выбранного дня

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet на выбранном дне не найден | Без файла невозможен глубокий профиль |
| `dataset_basic` | **warning** | Пустая витрина (`row_count=0`) | Фиксация объёма среза |
| `dataset_columns` | ok | Фактический список колонок parquet | Контракт DQ — по данным, без JSON-схемы |
| `field_profile_coverage` | ok | Число профилированных колонок | Контроль полноты DQ-прогона |

### Профили полей

| Check | Статус | Смысл | Обоснование |
|-------|--------|-------|-------------|
| `nulls.*` | **ok** | Null count/ratio по полю | Базовый профиль полноты для калибровки генератора |
| `cardinality.*` | **ok** | `nunique` и относительная кардинальность | Выбросы и неожиданная кардинальность |
| `unique_values.*` | **ok** | Таблица значений (низкая кардинальность) | Перечень редких категорий |
| `numeric_profile.*` | **ok** | min/p50/p95/max/mean/std + non-numeric | Статистический профиль числовых полей |
| `distribution.*` | **ok** | Top-N распределения и доли | Фактическое распределение категорий |
| `distribution.{col}_month` | **ok** | Помесячный профиль temporal-полей | Календарный профиль интервалов |
| `string_length.*` | **ok** | Длины `service_list`, `last_geo`, `geo_json` | Sanity длин текстовых blob-полей |

### STG-контракт (`stg_contract.*`)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `stg_contract.columns` | **failed** | Нет критичных полей для STG | `build-stg-person` ожидает фиксированный набор из `src_person` |
| `stg_contract.physical_rows` | **failed** | Нет строк `client_type=0` | Физлица — основной объём person-пайплайна |
| `stg_contract.physical.{isdn,imsi,imei}_present` | **failed/warning** | Полнота идентификаторов у ФЛ | GSM-поля обязательны для person identity graph |
| `stg_contract.physical.interval_order` | **failed/warning** | `actually_to >= actually_from` | Интервалы переносятся as-is в STG |
| `stg_contract.physical.fio_present` | **failed/warning** | ФИО у физлиц | Демографический слой person |

### Доменные проверки (выбранный день)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `identity_type.*.{col}_fill` | **ok** / **warning** / **failed** | Заполненность полей по типу identity | Каждый identity_type имеет свой набор обязательных полей |
| `identity_type.non_gsm_isdn_leak` | **warning** | `isdn` у non-GSM типов | Утечка MSISDN в PSTN/CDMA/VoIP |
| `key_integrity.operator_isdn` | **warning** | Дубликаты `(operator_Id, isdn)` | Бизнес-ключ абонента в срезе дня |
| `identity_duplicate_keys` | **warning** | Дубликаты `(operator_Id, isdn, imsi, actually_from)` | Ключ person identity |
| `isdn_format` | **warning** / **failed** | MSISDN: 11 цифр, префикс `7` | Формат номера для OSS-матчинга |
| `imsi_format`, `imei_format` | **warning** / **failed** | Длина 15 цифр | Стандартные идентификаторы SIM/ handset |
| `iccid_format` | **warning** / **failed** | Длина ICCID 18–20 | Идентификатор SIM-карты |
| `passport_format` | **warning** / **failed** | `dul_number` как `#### ######` | Формат паспорта РФ |
| `temporal_consistency` | **warning** | `actually_to >= actually_from`; профиль active_days | SCD-интервалы person |
| `actually_to_open_interval` | **warning** | Активные строки с `actually_to = ACTUALLY_TO_OPEN` | Открытый интервал активного абонента |
| `closed_contract_ratio` | ok | Доля закрытых договоров | Профиль churn/closed |
| `lac_cell_by_last_location` | **warning** / **failed** | LAC/Cell при `abonent_last_location=0` | Последняя локация на БС |
| `service_list_format` | ok | Разделитель `\x11`, число услуг | Формат списка услуг |
| `birth_day_quality` | **warning** | Возраст 14–110 лет | Sanity демографии |
| `fio_quality_physical` | **warning** / **failed** | ФИО у физлиц | Обязательные поля профиля ФЛ |
| `fio_quality_corporate` | **warning** | ФИО у юрлиц должно быть пустым | Разделение client_type |
| `citizenship_fields_presence` | **warning** | Наличие полей гражданства | Ожидаемо пусто для синтетики |
| `distribution.operator_Id` | ok | Распределение по операторам | Баланс операторов в срезе |

### Итог

| Check | Смысл | Обоснование |
|-------|-------|-------------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks`; итоговый статус run | Сводка прогона для мониторинга и CI |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/src/person.py`](../../../src/mobile/pipelines/dq/src/person.py) |
| ETL build `src_person` | [`pipelines/src/person.py`](../../../src/mobile/pipelines/src/person.py) |
| CLI wiring | [`cli.py`](../../../src/mobile/cli.py) |
| STG person | [`build_stg_person.md`](../../stg/build_stg_person.md) |
