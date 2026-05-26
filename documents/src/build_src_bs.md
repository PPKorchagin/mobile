# build-src-bs

**Витрина:** `src_bs` · **Команда:** `build-src-bs` · **Режим:** полная перезапись одного Parquet-файла.

Референс: [`pipelines/src/bs.py`](../../src/mobile/pipelines/src/bs.py). Схема витрины: [`bs.json`](../../src/mobile/schema/src/bs.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Загрузить полигоны субъектов из `stg_oktmo` и профиль генерации | Контекст для размещения БС |
| 2 | Сгенерировать синтетические строки БС в полигонах субъектов | DataFrame целевой схемы |
| 3 | Записать витрину в Parquet | Файл `data/src/bs.parquet` (перезапись) |

**Бизнес-назначение:** справочник мобильных базовых станций (синтетика) с календарными окнами `date_on` / `date_off`.

**В scope задач:** геометрия из ОКТМО, генерация координат и радиопараметров, шум, запись Parquet.

---

## TODO

1. Обновить профиль генерации после сверки с prod/DQ.
2. При необходимости вынести период и субъекты в аргументы CLI.

---

## Параметры запуска

Вызов: `bs.run(oktmo_parquet_path, output_path, compression, params)` ([`cli.py`](../../src/mobile/cli.py)).

| Переменная / объект | Тип | Обязательность | Значение по умолчанию | Описание |
|---------------------|-----|----------------|----------------------|----------|
| `oktmo_parquet_path` | string (path) | Да | `data/stg/oktmo.parquet` | Полигоны субъектов (`resolve_oktmo_layout()`) |
| `output_path` | string (path) | Да | `data/src/bs.parquet` | Выходной Parquet (перезапись) |
| `compression` | string | Да | `snappy` | Сжатие Parquet (`DEFAULT_PARQUET_COMPRESSION`) |
| `params` | `BuildBsParams` | Да | `default_bs_params()` | Период, регионы, операторы, seed, профиль |

Флагов CLI **нет**.

**Поля `BuildBsParams`** ([`cli_defaults.py`](../../src/mobile/cli_defaults.py) → `default_bs_params()`):

| Параметр | По умолчанию | Смысл |
|----------|--------------|-------|
| `start_date` / `end_date` | `2024-12-25` … `2025-02-05` | Период генерации |
| `subjects` | 3 субъекта (`DEFAULT_REGION_SUBJECTS`) | Фильтр ОКТМО `level=1` по `name` |
| `operators` | 4 оператора | MNC в `mnc` |
| `seed` | `20250407` | Случайность генерации |
| `profile_path` | `src/mobile/raw_data/build_bs_profile_from_opencellid.json` | JSON-профиль OpenCellID |

**Константы ETL в коде** ([`bs.py`](../../src/mobile/pipelines/src/bs.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `SRC_BS_TABLE` | `bs` |
| `SRC_BS_FIELDS` | порядок и типы колонок (см. [`bs.json`](../../src/mobile/schema/src/bs.json)) |
| `OPEN_BS_DATE_OFF` | `2999-12-31 23:59:59` (активные БС на конец периода) |
| `NOISE_FIELD_PROBABILITY` | `0.35` |
| `LAC_CELL_NULL_PROBABILITY` / `ZERO` | `0.015` / `0.01` (OCC-013) |
| Диапазон строк на субъект | `2800` … `5200` |

Локальный запуск:

```bash
uv run mobile build-src-bs
```

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `bs` — [`bs.json`](../../src/mobile/schema/src/bs.json) → `table` |
| Описание | Справочник мобильных базовых станций |
| Формат хранения | Parquet |
| Партиционирование | Нет |
| Календарный срез / `load_date` | Нет (snapshot) |
| Сжатие | `snappy` (`DEFAULT_PARQUET_COMPRESSION` в CLI) |

**Бизнес-ключ:** `mcc` + `mnc` + `lac` + `cell` + `date_on`.

### Поля витрины

Контракт — [`bs.json`](../../src/mobile/schema/src/bs.json) → `fields` (**36** колонок). Ключевые группы:

| Группа | Примеры полей |
|--------|----------------|
| Идентификация сети | `mcc`, `mnc`, `lac`, `cell`, `id` |
| Время | `date_on`, `date_off` |
| Координаты / регион | `lat`, `lon`, `subject`, `border` |
| Радио | `generation`, `azimut`, `width`, `indoor`, `external`, `msc_num`, `bsc_num`, `bsc_rc`, `bsc_cell_num`, `lac_16`, `cell_16`, `sector_num`, `bs_class`, `bs_type`, `power`, `ta`, `capacity`, `software_version`, `address`, `comment` |

Полный перечень имён и типов — только в JSON.

### Ожидаемый объём (дефолтный CLI)

Порядка **8–16 тыс.** строк (3 субъекта × 2800–5200, seed `20250407`).

---

## Источники витрины

| # | Источник | Путь (по умолчанию) | Назначение |
|---|----------|---------------------|------------|
| 1 | ОКТМО STG | `data/stg/oktmo.parquet` | `level=1`, колонки `name`, `WKT` → полигоны |
| 2 | Профиль генерации | `src/mobile/raw_data/build_bs_profile_from_opencellid.json` | Доли операторов/поколений, диапазоны LAC/Cell, p50/p95 мощности |

---

## Алгоритм обработки данных

Точка входа: `run(oktmo_parquet_path, output_path, compression, params)` в [`bs.py`](../../src/mobile/pipelines/src/bs.py).

### Шаг 0. Подготовка

1. `fields = SRC_BS_FIELDS`, `output_path` и `compression` — аргументы job.
2. `rng = random.Random(params.seed)`.
4. Стадия `load_oktmo_sec`: `_load_subject_geometries(oktmo_parquet_path, params.subjects)`.
5. `profile = _load_build_profile(params.profile_path, params.operators)` — при отсутствии файла профиля возвращается `None` (дефолтные веса поколений и диапазоны LAC/Cell).

### Шаг 1. Загрузка ОКТМО (`_load_subject_geometries`)

1. `pd.read_parquet(oktmo_path)`; фильтр `level == 1`.
2. `name` → `string`, strip; оставить строки, где `name ∈ params.subjects`.
3. Если `set(subjects) - found` не пуст — `ValueError` с перечнем отсутствующих субъектов.
4. Для каждой строки: `wkt.loads(WKT)`; допустимы только `Polygon` / `MultiPolygon`, иначе `ValueError`.
5. Результат: `dict[subject_name → geometry]`.

### Шаг 2. Профиль OpenCellID (`_load_build_profile`, опционально)

При наличии JSON: веса операторов (`operator_distribution_pct` + алиасы `OPERATOR_PROFILE_ALIASES`), веса поколений глобально и по оператору, `id_ranges.lac` / `cell`, `samples.p50` / `p95` для мощности.

### Шаг 3. Генерация строк (`_generate_rows`, стадия `generate_rows_sec`)

**Календарь `date_on`:** `_weighted_dates(start_date, end_date)` — для каждого дня периода вес = `month_weight × weekday_weight` (месяцы 1/2/3 → 3/4/5, будни 2, выходные 1); день повторяется в списке столько раз, сколько вес.

**Цикл по субъектам:**

1. `subject_total = rng.randint(2800, 5200)`.
2. `_split_subject_total_by_operator`: доли по `profile.operator_weights` или равные; целые части + остаток раздаётся `rng.choices` по весам.
3. Для каждого `operator` и `target_count` раз:
   - `date_on = rng.choice(dates)`; `date_off = _sample_date_off(date_on, end_date)` (+3…+90 дней, cap `end_date`).
   - `point = _sample_point_in_geometry(geom, rng)` — rejection sampling в bounds (до 5000 попыток), для `MultiPolygon` выбор полигона по площади; fallback `representative_point()`.
   - `on_border = (point.distance(geom.boundary) <= 0.012°)`.
   - `_generate_row(...)` → словарь; в список попадают только ключи из `fields`.

**`_generate_row` (ядро одной БС):**

| Блок | Логика |
|------|--------|
| Сеть | `mcc=250`, `mnc=OPERATORS[operator]` |
| Поколение | `_sample_generation` — из профиля оператора / глобально / дефолт `[2G,3G,4G,LTE,5G]` с весами `[5,8,35,35,17]` |
| Развёртывание | `_pick_deploy_profile(tech)` — 7 профилей `_DEPLOY_PROFILES` (macro, micro, indoor, femto, …); веса корректируются для 2G/5G |
| Радио | `_coherent_radio_fields` + `_sample_radio_power` из профиля; поля в `_PROTECTED_RADIO_COORD_FIELDS` не трогаются на шаге шума |
| OCC-013 | `lac_cell_roll`: `<0.015` → `lac,cell=None`; `<0.025` → `0,0`; иначе `_sample_lac` / `_sample_cell` из диапазона профиля |
| Время | `date_on`/`date_off` → `_msk_wall_datetime` (Europe/Moscow, naive). Если `date_off >= period_end` → `date_off = OPEN_BS_DATE_OFF`. Иначе с вероятностью ~1.5% «старые» даты, ~1.5% «будущие», ~1% `date_off < date_on`, иначе нормализация в пределах периода |
| Прочее | `address`, `description`, частоты, `border`, `bsid`, … |

### Шаг 4. Шум и типы

1. `DataFrame(rows)`.
2. `_inject_noise`: для каждой строки с вероятностью `NOISE_ROW_PROBABILITY` (0.22) и для каждого поля (кроме `_PROTECTED_RADIO_COORD_FIELDS`) с вероятностью `NOISE_FIELD_PROBABILITY` (0.35) подставляется «грязное» значение (`_sample_noisy_value` по типу поля). DataFrame переводится в `object` до coercion.
3. `_coerce_types`: cast по `fields` (`int`→`Int32`, `smallint`→`Int16`, `timestamp`→`datetime64[ns]`, границы int, boolean из строк).
4. `_validate_dataset`: все колонки из `fields` присутствуют; датасет не пуст; неизвестные `subject` — только `logger.warning` (допускается шум).
5. `_collect_stats` — распределения mnc/generation, счётчики шума.

### Шаг 5. Запись (стадия `write_parquet_sec`)

1. `output_path.parent.mkdir(parents=True, exist_ok=True)`.
2. `data.to_parquet(output_path, compression=compression, index=False)` — перезапись.
3. `append_command_metrics(command="build-src-bs", metrics={stats + perf})`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет ОКТМО или профиля |
| `ValueError` | Субъект не найден; неподдерживаемая геометрия; пустой датасет |
| pandas / pyarrow | Запись parquet |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/src/bs.json`](../../src/mobile/schema/src/bs.json) |
| ETL | [`src/mobile/pipelines/src/bs.py`](../../src/mobile/pipelines/src/bs.py) |
| Пути по умолчанию | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
