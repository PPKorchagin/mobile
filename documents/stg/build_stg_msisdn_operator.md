# build-stg-msisdn-operator

**Витрина:** `stg_msisdn_operator` · **Команда:** `build-stg-msisdn-operator` · **Режим:** месячные интервалы MSISDN + `operator_id` (+ `imsi`) из всех срезов `src_person`.

Референс: [`pipelines/stg/msisdn_operator.py`](../../src/mobile/pipelines/stg/msisdn_operator.py). Схема: [`msisdn_operator.json`](../../src/mobile/schema/stg/msisdn_operator.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать **все** `src_person` с `_SUCCESS` за месяц | Конкатенация срезов |
| 2 | Отфильтровать ФЛ, пересечение с месяцем | Подмножество строк |
| 3 | Агрегировать `(msisdn, operator_id, imsi)` | `valid_from` / `valid_to` |
| 4 | Записать parquet | `data/stg/msisdn_operator/{YYYY-MM-01}.parquet` |

**Бизнес-назначение:** явные **наблюдения MNP** — один номер, разные операторы/IMSI на разных интервалах. Используется в [`build-stg-person`](./build_stg_person.md) как рёбра графа персон.

**В scope:** только `src_person` (не geo). Профиль персоны строится по **последнему** `load_day`; operator-витрина — по **всем** срезам месяца.

---

## TODO

1. DQ `dq-stg-msisdn-operator` (непересекающиеся интервалы на одном msisdn+operator).
2. Обогащение из `event_dds` (serving MNC) как независимая проверка.

---

## Параметры запуска

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | **`YYYY-MM-01`** |
| `src_person_path` | path | Нет | `data/src/person` | Корень layout |
| `output_path` | path | Нет | `data/stg/msisdn_operator/{YYYY-MM-01}.parquet` | Выход |

```bash
uv run mobile build-stg-msisdn-operator --report-date 2025-01-01
```

Внутри `build-stg-person` команда вызывается автоматически (`build_operator_vitrine=true`).

Логи: `command=build-stg-msisdn-operator`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Таблица | `stg_msisdn_operator` |
| Путь | `data/stg/msisdn_operator/{YYYY-MM-01}.parquet` |
| Сжатие | `snappy` |

### Поля витрины

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | Нормализованный MSISDN |
| 2 | `operator_id` | long | `operator_Id` из `src_person` |
| 3 | `imsi` | string | IMSI на интервале (может быть null) |
| 4 | `valid_from` | timestamp | min(`actually_from`) по группе |
| 5 | `valid_to` | timestamp | max(`actually_to`) по группе |

---

## Источники витрины

| # | Источник | Путь | Режим чтения |
|---|----------|------|--------------|
| 1 | `src_person` | `data/src/person/load_year=…/load_month=…/load_day=*/person.parquet` | `all_snapshots` |

Чтение: [`src_person_month.py`](../../src/mobile/pipelines/stg/src_person_month.py) → `read_src_person_month(..., mode="all_snapshots")`.

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. `report_date.day == 1`.
2. Период месяца `[01 .. последний день]`.
3. Обход всех `load_day` с `_SUCCESS` и `person.parquet`.

### Шаг 1. Чтение

`pd.concat` всех срезов месяца.

### Шаг 2. Фильтрация

1. `client_type == 0`.
2. Интервал пересекает отчётный месяц.
3. Нормализация `msisdn`, `imsi`; `operator_id` из `operator_Id`.

### Шаг 3. Агрегация

`groupby(msisdn, operator_id, imsi).agg(min actually_from, max actually_to)`.

### Шаг 4. Запись

Parquet по `output_path`.

### Типовые ошибки

| Ситуация | Поведение |
|----------|-----------|
| Нет срезов с `_SUCCESS` | `FileNotFoundError` |
| Пустой месяц без ФЛ | пустой parquet |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`msisdn_operator.json`](../../src/mobile/schema/stg/msisdn_operator.json) |
| ETL | [`msisdn_operator.py`](../../src/mobile/pipelines/stg/msisdn_operator.py) |
| Person | [`build_stg_person.md`](./build_stg_person.md) |
| src_person | [`build_src_person.md`](../src/build_src_person.md) |
