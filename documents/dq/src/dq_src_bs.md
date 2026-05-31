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
| 1 | Витрина `src_bs` | `data/src/bs.parquet` (`--src-bs-path`) | Единственный вход DQ: все метрики считаются по фактическим данным parquet |

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
   - `temporal_consistency` — `date_off >= date_on`, min/max `date_on` / `date_off`, длительность интервалов;
   - `temporal_date_off_tail` — хвост `date_off`: max, p95, доля строк на максимальном `date_off`;
   - `distribution.date_on_month`, `distribution.date_off_month` — помесячные профили.
4. **Spatial:** `spatial_ranges` — `coord_x`/`coord_y` (lon/lat) в диапазонах; `contract.coords_null`, `contract.coords_out_of_range`.
5. **Радио:** `contract.range.*`, `contract.generation_vocab`, `contract.azimuth_semantics`, `contract.frequency_list.*`; профили `radio.profile.*` и полнота `radio.presence.*` по фактическим данным.
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

Статусы: **ok** / **warning** / **failed** (`nulls.*`, `cardinality.*`, профили распределений — всегда **ok**).

### Базовые проверки датасета

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `dataset_presence` | **failed** | Parquet по `--src-bs-path` не найден | Без файла витрины DQ и downstream (`build-stg-bs`, `build-src-mobile`) не имеют входа |
| `report_scope` | **failed/warning** | **failed** если нет `date_on`/`date_off`; **warning** если витрина пуста | Temporal-колонки обязательны для SCD-логики и трансформации в `stg_bs` |
| `dataset_basic` | **warning** | Пустая витрина (`row_count=0`) | Фиксация объёма среза; пустой справочник блокирует geo/mobile-пайплайны |

### Профили полей

| Check | Статус | Смысл | Обоснование |
|-------|--------|-------|-------------|
| `nulls.*` | **ok** | Null count/ratio по полю | Доля пропусков — базовый профиль полноты для калибровки генератора |
| `cardinality.*` | **ok** | `nunique` и относительная кардинальность | Число distinct значений — выбросы и неожиданная кардинальность |
| `unique_values.*` | **ok** | Таблица значений (низкая кардинальность) | Полный перечень редких категорий без отдельного эталона |
| `numeric_profile.*` | **ok** | min/p50/p95/max/mean/std + non-numeric | Статистический профиль числовых полей для сравнения прогонов |
| `distribution.*` | **ok** | Top-N распределения и доли | Фактическое распределение категорий и дискретных numeric |
| `distribution.date_on_month`, `distribution.date_off_month` | **ok** | Помесячное распределение temporal-полей | Профиль календарного покрытия без привязки к константам периода |
| `field_profile_coverage` | **ok** | Сколько колонок parquet профилировано | Контроль полноты DQ-прогона по всем полям витрины |

### Ключи, temporal, spatial

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `key_integrity` | **warning** | Дубли по `(mcc,mnc,lac,cell,date_on)` | Бизнес-ключ сектора; дубликаты ломают джойны и SCD в `stg_bs` |
| `temporal_consistency` | **warning** | Строки с `date_off < date_on`; min/max temporal-полей | Интервал активности БС должен быть логически согласован |
| `temporal_date_off_tail` | **ok** | Хвост `date_off`: max, p95, доля строк на max | Профиль «открытых» интервалов по факту данных, без внешнего sentinel |
| `spatial_ranges` | **warning** | Координаты вне lon/lat диапазонов | Невалидные координаты недопустимы для geo-джойнов и карт |

### STG-контракт (`stg_contract.*`)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `stg_contract.columns` | **failed** | Нет критичных полей для STG-контракта | `build-stg-bs` ожидает фиксированный набор колонок из `src_bs` |
| `stg_contract.lac_cell` | **failed/warning** | Доля валидных `lac/cell` ниже порогов | LAC/Cell — часть CGI; без них сектор не идентифицируется |
| `stg_contract.coords` | **failed/warning** | Доля валидных координат ниже порогов | Координаты нужны для `stg_bs` и последующих geo-витрин |
| `stg_contract.temporal_order` | **failed/warning** | Доля корректного порядка дат ниже порогов | STG переносит интервалы as-is; инверсия дат — дефект источника |
| `stg_contract.generation_present` | **failed/warning** | Низкая доля непустой `generation` | Поколение RAT используется в профиле и фильтрах downstream |

### Доменные контракты (`contract.*`)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `contract.mcc_rf` | **failed/warning** | Доля `mcc=250`; top MCC | RF — домашний MCC для российских БС; отклонения — профиль качества |
| `contract.mnc_valid` | **warning** | Негативные/пустые `mnc`; `distinct_mnc` | MNC идентифицирует оператора; распределение — в `distribution.mnc` |
| `contract.lac_cell_non_negative` | **failed/warning** | Неотрицательные `lac/cell` | Отрицательные идентификаторы недопустимы в сетевой модели |
| `contract.cgi_duplicate_rows` | **warning** | Дубликаты полного CGI | Один CGI не должен повторяться как независимая запись |
| `contract.date_off_present` | **failed** | Пустые `date_off` | Закрытие интервала обязательно для SCD и temporal-джойнов |
| `contract.coords_present` | **failed/warning** | Полнота координат | Пропуски координат снижают пригодность для geo-пайплайнов |
| `contract.cells_per_coordinate` | **ok** | Секторов на одну точку (p50/p95/max) | Профиль плотности секторов на площадке — sanity для генератора |
| `contract.generation_vocab` | **failed/warning** | `generation` вне `{2G,3G,4G,LTE,5G}`; `unknown_count` | Нормализованный словарь RAT; распределение — в `distribution.generation` |
| `contract.azimuth_semantics` | **failed/warning** | Азимут, omnidirectional, indoor | Семантика направления сектора влияет на radio-модель |
| `contract.range.*` | **failed/warning** | Numeric/int поля в доменных диапазонах | Clip-диапазоны из профиля OpenCellID; выход — аномалия генерации |
| `contract.frequency_list.*` | **warning** | Формат списков частот `frequency_in/out` | Частоты — списки band ID; битый формат ломает парсинг |
| `contract.border_boolean` | **warning** | `border` вне boolean-семантики | Признак приграничной БС должен быть нормализуемым bool |

### Радио-показатели (`radio.*`)

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `radio.profile.{field}` | **warning** | min/p50/p95/max/mean, null_count для `power`, `height`, `frequency`, `tilt`, `el_tilt`, `mech_tilt`, `amplification`, `polarization`, `raster`, `thickness` | Фактический профиль числовых radio-полей для калибровки генератора |
| `radio.profile.frequency` | **warning** | + `sentinel_minus_one_count`, `positive_count` | Несущая частота: sentinel `-1` и положительные значения |
| `radio.profile.azimuth` | **ok** | Профиль азимута + `omnidirectional_*`, `directional_*` | Доля секторов без направленности vs направленных |
| `radio.profile.power_height` | **ok** | Совместная полнота `power`/`height`; p50 обоих | Мощность и высота подвеса — базовая radio-модель |
| `radio.profile.generation_bs_type` | **ok** | Совместная полнота `generation`/`bs_type`; distinct обоих | Связка RAT и типа площадки для профиля сети |
| `radio.presence.{field}` | **ok** | Полнота `bs_type`, `location`, `rad_class`, `bcch`, `controllernum`, `frequency_in/out` | Фактическая доля пропусков категориальных radio-полей |

### Итог

| Check | Смысл | Обоснование |
|-------|-------|-------------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks`; итоговый статус run | Сводка прогона для мониторинга, сравнения прогонов и CI |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/src/bs.py`](../../../src/mobile/pipelines/dq/src/bs.py) |
| ETL build `src_bs` | [`pipelines/src/bs.py`](../../../src/mobile/pipelines/src/bs.py) |
| CLI wiring | [`cli.py`](../../../src/mobile/cli.py) |
