# dq-src-bs

**Витрина:** `src_bs` · **Команда:** `dq-src-bs` · **Режим:** read-only DQ (процесс не падает при failed checks).

Референс: [`pipelines/dq/src/bs.py`](../../../src/mobile/pipelines/dq/src/bs.py). Сборка витрины: [`build_src_bs.md`](../../src/build_src_bs.md).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать полный `src_bs` parquet | DataFrame по всей витрине |
| 2 | Построить профили и распределения по полям | JSON-метрики в лог `DQ_SRC_BS` |
| 3 | Выполнить доменные и контрактные проверки | `ok` / `warning` / `failed` по checks |
| 4 | Сформировать максимальный набор метрик качества | База для калибровки генератора |
| 5 | Сформировать `summary` | Счётчики checks и итоговый статус |

**Бизнес-назначение:** собрать максимально широкий профиль качества `src_bs`, чтобы затем по этим метрикам калибровать генерацию синтетики.

**В scope задач:** максимально полные метрики по фактическим колонкам витрины: null/cardinality, распределения, temporal/spatial checks, доменные контракты и диапазоны.

---

## TODO

1. При необходимости вынести пороги `contract.range.*` и `stg_contract.*` в конфиг.
2. Добавить notebook-визуализацию `DQ_SRC_BS` (динамика распределений между запусками).

---

## Параметры запуска

Вызов: `run_dq(parquet_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-src-bs`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `parquet_path` | path | Нет | `data/src/bs.parquet` | Входной parquet `src_bs` |

Локальный запуск:

```bash
uv run mobile dq-src-bs
uv run mobile dq-src-bs --src-bs-path data/src/bs.parquet
```

Логи: `data/logs/mobile.log` (тег `DQ_SRC_BS`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-src-bs`.

---

## Структура проверяемой витрины

| Свойство | Значение |
|----------|----------|
| Имя | `src_bs` |
| Формат | Parquet |
| Путь (по умолчанию) | `data/src/bs.parquet` |
| Набор полей | Все фактические колонки parquet |
| Ключевые домены | CGI/идентификаторы, temporal, координаты, радио-параметры |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | Витрина `src_bs` | `data/src/bs.parquet` | Объект DQ-проверок |
| 2 | Константы операторов/дат | `src/mobile/cli_defaults.py` | Доменные проверки (`MNC`, open sentinel) |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Резолв входного пути `parquet_path`.
2. Чтение фактического набора колонок из parquet.

### Шаг 1. Базовые проверки датасета

1. Проверка наличия parquet (`dataset_presence`).
2. Чтение полного DataFrame `src_bs`.
3. Проверка наличия `date_on` / `date_off` (`report_scope` failed при отсутствии колонок).
4. Базовые метрики `report_scope`, `dataset_basic` (`warning` при пустой витрине).

### Шаг 2. Профили полей

Для каждой фактической колонки DataFrame:

1. `nulls.{field}` и `cardinality.{field}`.
2. `unique_values.{field}` для низкой кардинальности.
3. `numeric_profile.{field}` для numeric-полей.
4. `distribution.{field}` для string/boolean и дискретных numeric; отдельно `distribution.date_on_month` и `distribution.date_off_month`.
5. Сводка покрытия профилей `field_profile_coverage` (по всем колонкам parquet).

### Шаг 3. Доменные/контрактные проверки

Полный проход по `src_bs` (без фильтра по дате):

1. **`key_integrity`:** уникальность `(mcc,mnc,lac,cell)` в смысле бизнес-ключа; дубликаты активных строк → **warning**/**failed**.
2. **`contract.cgi_duplicate_rows`:** число строк с одинаковым CGI и пересекающимися `[date_on, date_off]`.
3. **Temporal:**
   - `temporal_consistency` — `date_off >= date_on`;
   - `temporal_open_date_off` — доля открытых интервалов;
   - `contract.date_on_not_future`, `contract.date_off_after_on` — пороги на аномалии.
4. **Spatial:** `spatial_ranges` — `coord_x`/`coord_y` (lon/lat) в диапазонах; `contract.coords_null`, `contract.coords_out_of_range`.
5. **Радио:** `contract.range.*` — clip-диапазоны azimuth, power, height, …; `contract.generation_vocab`; список допустимых `frequency`.
6. **`stg_contract.*`:** сравнение имён/типов с ожидаемым маппингом в [`build-stg-bs`](../../stg/build_stg_bs.md) (покрытие полей, которые должны пережить трансформацию).
7. Каждый check логируется: `{"tag":"DQ_SRC_BS","check":"...","status":"...","metrics":{...}}`.

### Шаг 4. Итог

`summary` с `total_checks`, `warning_checks`, `failed_checks`; return dict со `status`, `row_count`, `source_row_count`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `dataset_presence` **failed** | Нет входного parquet |
| `report_scope` **failed** | Отсутствуют `date_on` или `date_off` |
| Много `contract.range.*` failed | Генератор выдаёт значения вне доменных диапазонов |

---

## Проверки

Статусы: **ok** / **warning** / **failed**.

### Базовые проверки датасета

| Check | Статус при сбое | Смысл |
|-------|------------------|-------|
| `dataset_presence` | **failed** | Нет parquet-файла |
| `report_scope` | **failed/warning** | **failed** если нет `date_on`/`date_off`; **warning** если витрина пуста |
| `dataset_basic` | **warning** | Пустая витрина (`row_count=0`) |

### Профили полей

| Check | Статус | Смысл |
|-------|--------|-------|
| `nulls.*` | **ok** | Null count/ratio по полю |
| `cardinality.*` | **ok** | `nunique` и относительная кардинальность |
| `unique_values.*` | **ok** | Таблица значений (для низкой кардинальности) |
| `numeric_profile.*` | **ok** | min/p50/p95/max/mean/std + non-numeric |
| `distribution.*` | **ok** | Top-N распределения и доли |
| `distribution.date_on_month`, `distribution.date_off_month` | **ok** | Помесячное распределение temporal-полей |
| `field_profile_coverage` | **ok** | Сколько полей профилировано |

### Ключи, temporal, spatial

| Check | Статус при сбое | Смысл |
|-------|------------------|-------|
| `key_integrity` | **warning** | Дубли по `(mcc,mnc,lac,cell,date_on)` |
| `temporal_consistency` | **warning** | Строки с `date_off < date_on` |
| `temporal_open_date_off` | **ok** | Доля открытых записей относительно open sentinel |
| `spatial_ranges` | **warning** | Координаты вне допустимых диапазонов |

### STG-контракт (`stg_contract.*`)

| Check | Статус при сбое | Условие |
|-------|------------------|---------|
| `stg_contract.columns` | **failed** | Нет критичных полей для STG-контракта |
| `stg_contract.lac_cell` | **failed/warning** | Доля валидных `lac/cell` ниже порогов |
| `stg_contract.coords` | **failed/warning** | Доля валидных координат ниже порогов |
| `stg_contract.temporal_order` | **failed/warning** | Доля корректного порядка дат ниже порогов |
| `stg_contract.generation_present` | **failed/warning** | Низкая доля непустой `generation` |

### Доменные контракты (`contract.*`)

| Check | Статус при сбое | Смысл |
|-------|------------------|-------|
| `contract.mcc_rf` | **failed/warning** | Доля `mcc=250` |
| `contract.mnc_valid` | **warning** | Негативные/пустые `mnc` |
| `contract.lac_cell_non_negative` | **failed/warning** | Неотрицательные `lac/cell` |
| `contract.cgi_duplicate_rows` | **warning** | Дубликаты CGI |
| `contract.date_off_present` | **failed** | Пустые `date_off` |
| `contract.active_vs_closed` | **ok** | Баланс открытых/закрытых интервалов |
| `contract.coords_present` | **failed/warning** | Полнота координат |
| `contract.cells_per_coordinate` | **ok** | Секторов на координату |
| `contract.generation_vocab` | **failed/warning** | Значения `generation` вне словаря |
| `contract.azimuth_semantics` | **failed/warning** | Семантика азимута и indoor/omni |
| `contract.range.*` | **failed/warning** | Диапазоны numeric/int полей |
| `contract.frequency_list.*` | **warning** | Формат списков частот |
| `contract.border_boolean` | **warning** | Значения `border` вне boolean-семантики |

### Итог

| Check | Смысл |
|-------|--------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks`; итоговый статус run |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/src/bs.py`](../../../src/mobile/pipelines/dq/src/bs.py) |
| ETL build `src_bs` | [`pipelines/src/bs.py`](../../../src/mobile/pipelines/src/bs.py) |
| CLI wiring | [`cli.py`](../../../src/mobile/cli.py) |
