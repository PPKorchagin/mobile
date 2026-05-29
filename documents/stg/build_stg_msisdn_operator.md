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

Точка входа: `run_build` → `build_operator_intervals_from_src` в [`msisdn_operator.py`](../../src/mobile/pipelines/stg/msisdn_operator.py).

### Шаг 0. Инициализация

1. `_validate_report_month`: `report_date` = 1-е число месяца.
2. `period_start`, `period_end` = границы календарного месяца.
3. `output_path` → `data/stg/msisdn_operator/{YYYY-MM-01}.parquet`.
4. Старт `timed_stage` и `append_command_metrics`.

### Шаг 1. Чтение всех срезов `src_person`

1. `read_src_person_month(..., mode="all_snapshots")`:
   - для каждого `load_day` в `[period_start, period_end]` с `_SUCCESS`;
   - `pd.concat` всех `person.parquet` (в отличие от person-профиля, где берётся только последний срез).
2. Это нужно, чтобы увидеть **смену оператора** (MNP) на одном MSISDN в разные дни месяца.
3. При отсутствии срезов — `FileNotFoundError`.

### Шаг 2. Фильтрация бизнес-правил

1. `client_type == 0` (только физлица).
2. `actually_from` / `actually_to` как timestamp; пустой `actually_to` → `2999-12-31 23:59:59`.
3. Интервал подписки **пересекает** отчётный месяц:
   - `actually_from <= month_end` и `actually_to >= month_start`.
4. Нормализация:
   - `msisdn` ← `normalize_msisdn(isdn)`;
   - `imsi` ← `normalize_imsi(imsi)` (может быть null в группе);
   - `operator_id` ← `operator_Id`.
5. `dropna` по `msisdn`, `operator_id`, `actually_from`, `actually_to`.

### Шаг 3. Агрегация интервалов

1. `groupby(["msisdn", "operator_id", "imsi"], dropna=False)`.
2. Агрегаты:
   - `valid_from = min(actually_from)`;
   - `valid_to = max(actually_to)`.
3. Выходные колонки: `msisdn`, `operator_id`, `imsi`, `valid_from`, `valid_to`.
4. **Не** выполняется склейка смежных интервалов между разными срезами — только min/max внутри группы (достаточно для рёбер MNP в person).

### Шаг 4. Запись

1. `mkdir` родительского каталога.
2. `to_parquet(output_path, compression=snappy)`.
3. Метрики: `interval_rows`, `distinct_msisdn`, пути.

### Шаг 5. Использование в `build-stg-person`

1. `operator_observation_edges`: для каждой строки витрины с пересечением месяца — ребро `union(msisdn:…, imsi:…)` (если IMSI задан).
2. Один MSISDN с двумя `operator_id` на разных интервалах **не** объединяется только по номеру — только через общий IMSI или другие узлы графа.

### Типовые ошибки

| Ситуация | Поведение |
|----------|-----------|
| Нет срезов с `_SUCCESS` | `FileNotFoundError` |
| Пустой месяц без ФЛ | пустой parquet |
| `report_date` не 1-е число | `ValueError` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`msisdn_operator.json`](../../src/mobile/schema/stg/msisdn_operator.json) |
| ETL | [`msisdn_operator.py`](../../src/mobile/pipelines/stg/msisdn_operator.py) |
| Person | [`build_stg_person.md`](./build_stg_person.md) |
| src_person | [`build_src_person.md`](../src/build_src_person.md) |
