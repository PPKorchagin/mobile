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
| `build_bindings_month` | bool | Нет | `true` | Вызвать `build-stg-msisdn-*-month`, если нет файла |
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

### Шаг 0. Инициализация

1. `report_date.day == 1`.
2. Пути: `person`, `person_sim`, `person_id_ledger`, operator, monthly bindings.
3. При необходимости — `build-stg-msisdn-operator` и `refresh_month_bindings_from_geo` (если нет month parquet).

### Шаг 1. Чтение `src_person` (профиль)

`read_src_person_month(..., mode="latest_snapshot")`.

### Шаг 2. Исключение M2M

TAC = первые 8 цифр IMEI; исключить `is_m2m=true` из [`stg_tac`](build_stg_tac.md).

### Шаг 3. Operator / MNP

Из **всех** срезов месяца — интервалы `(msisdn, operator_id, imsi)` → `stg_msisdn_operator`. Один MSISDN может иметь несколько операторов на разных интервалах.

### Шаг 4. Месячные bindings

Загрузка месячных `stg_msisdn_imsi` / `stg_msisdn_imei`; при отсутствии — `refresh_month_bindings_from_geo` по всем дням с `stg_geo_all`.

### Шаг 5. Подписки и binding-fill

1. `client_type == 0`, пересечение с месяцем.
2. Нормализация ID; binding-fill на конец месяца.
3. Узлы строки: `bio:`, `contract:`, `iccid:`, `msisdn:`, `imsi:`, `imei:`.

### Шаг 6. Граф персон (union-find)

**Рёбра:**

- co-occurrence на строке `src_person`;
- пары из monthly `stg_msisdn_imsi` / `stg_msisdn_imei` (интервал ∩ месяц);
- пары из `stg_msisdn_operator` (MNP: не сливать только по `operator_id` без msisdn).

**`person_cluster_key`:** приоритет `bio:` → `contract:` → `iccid:` → min(технические узлы).

**`person_confidence`:** `high` (bio), `medium` (contract/iccid), `low` (только tech ID).

**`person_id`:** `prs_` + SHA256(`person_cluster_key`)[:24], с переопределением из ledger прошлого месяца при совпадении узлов ([`assign_person_ids_with_ledger`](../../src/mobile/pipelines/stg/person_identity.py)).

### Шаг 7. Выходные витрины

1. **`stg_person`:** одна строка на `person_id`; primary MSISDN/IMSI/IMEI по `is_primary`; `sim_count`.
2. **`stg_person_sim`:** все SIM-интервалы кластера; ровно одна `is_primary=true` на `person_id`.
3. **`stg_person_id_ledger`:** все узлы кластера для следующего месяца.

### Типовые ошибки

| Ситуация | Поведение |
|----------|-----------|
| `report_date` не 1-е число | `ValueError` / `SystemExit` |
| Нет `_SUCCESS` за месяц | `FileNotFoundError` |
| Нет `stg_tac` | warning, M2M не фильтруется |
| Нет суточных binding | пустые monthly / слабый fill |

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
