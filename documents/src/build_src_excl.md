# build-src-excl

**Витрины:** `src_imsi`, `src_imei`, `src_msisdn` · **Команда:** `build-src-excl` · **Режим:** полная перезапись трёх Parquet-файлов.

Референс: [`pipelines/src/excl.py`](../../src/mobile/pipelines/src/excl.py). Схемы: [`imsi.json`](../../src/mobile/schema/src/imsi.json), [`imei.json`](../../src/mobile/schema/src/imei.json), [`msisdn.json`](../../src/mobile/schema/src/msisdn.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Найти последний полный срез `src_person` (каталог с `_SUCCESS`) | Источник троек imsi/imei/msisdn |
| 2 | Отобрать уникальные тройки и случайную выборку по доле АБ | DataFrame для трёх списков |
| 3 | Записать три Parquet в `data/src/excl/` | `src_imsi`, `src_imei`, `src_msisdn` |

**Бизнес-назначение:** списки идентификаторов для исключения из обработки (связанные imsi, imei, msisdn).

**В scope задач:** чтение person, выборка, запись одноколоночных витрин.

---

## TODO

1. При необходимости вынести выбор дня-источника в параметр (сейчас — последний `_SUCCESS`).

---

## Параметры запуска

Вызов: `excl.run_from_config(person_config, imsi_config, imei_config, msisdn_config, params)` ([`cli.py`](../../src/mobile/cli.py)).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `src_person_config_path` | string (path) | Да | `src/mobile/schema/src/person.json` | Layout и `success_flag` источника |
| `src_imsi_config_path` | string (path) | Да | `src/mobile/schema/src/imsi.json` | Выход IMSI |
| `src_imei_config_path` | string (path) | Да | `src/mobile/schema/src/imei.json` | Выход IMEI |
| `src_msisdn_config_path` | string (path) | Да | `src/mobile/schema/src/msisdn.json` | Выход MSISDN |
| `params` | `BuildSrcExclParams` | Да | `default_excl_params(...)` | Доля АБ и seed |

| Переменная CLI | Тип | По умолчанию | Описание |
|----------------|-----|--------------|----------|
| `--excl-pct-of-ab` | float | `0.7` | Процент строк **АБ** в исключениях |

**Поля `BuildSrcExclParams`:**

| Параметр | По умолчанию | Смысл |
|----------|--------------|-------|
| `pct_of_ab` | `0.7` (или флаг CLI) | % строк последнего full snapshot |
| `seed` | `20250407` | Детерминированная выборка |

Размер выборки: `sample_size = min(eligible_triples, max(1, round(ab_row_count × pct / 100)))`, где `eligible_triples` — уникальные строки с непустыми `imsi`, `imei`, `isdn` (`isdn` → msisdn).

Даты периода CLI **не задаёт**.

Локальный запуск:

```bash
uv run mobile build-src-excl
uv run mobile build-src-excl --excl-pct-of-ab 0.5
```

---

## Структура генерируемых витрин

| Витрина | JSON | Путь (по умолчанию) | Поля |
|---------|------|---------------------|------|
| `src_imsi` | [`imsi.json`](../../src/mobile/schema/src/imsi.json) | `data/src/excl/src_imsi.parquet` | `value` (string) |
| `src_imei` | [`imei.json`](../../src/mobile/schema/src/imei.json) | `data/src/excl/src_imei.parquet` | `value` (string) |
| `src_msisdn` | [`msisdn.json`](../../src/mobile/schema/src/msisdn.json) | `data/src/excl/src_msisdn.parquet` | `value` (string) |

| Свойство | Значение |
|----------|----------|
| Формат | Parquet |
| Партиционирование | Нет |
| Сжатие | `snappy` |

### Ожидаемый объём

При `pct_of_ab=0.7` и ~200k строк АБ — порядка **1.4k** значений в каждом файле (зависит от eligible triples).

---

## Источники витрины

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `src_person` | Последний `data/src/person/.../load_day=*/` с `_SUCCESS` | `person.parquet` |
| 2 | Layout person | из `person.json` → `readiness.s3_layout`, `success_flag` | Поиск среза |

**Предусловие:** выполнен `build-src-person` (есть каталог с `_SUCCESS`).

---

## Алгоритм обработки данных

Точка входа: `run_from_config(person_cfg, imsi_cfg, imei_cfg, msisdn_cfg, params)` в [`excl.py`](../../src/mobile/pipelines/src/excl.py).

### Шаг 0. Чтение конфигов

1. `_read_json` для четырёх путей; отсутствие файла → `FileNotFoundError`.

### Шаг 1. Поиск среза person (`_resolve_latest_success_day_dir`)

1. Из `person.json` → `readiness.s3_layout`, `success_flag` (default `_SUCCESS`).
2. `_resolve_person_layout_root(layout)`: корень до первого сегмента с `{placeholder}` (например `data/src/person`).
3. `glob("load_year=*/load_month=*/load_day=*")`, отфильтровать каталоги, где есть файл `success_flag`.
4. Взять **последний** по сортировке пути (`success_dirs[-1]`). Если пусто → `FileNotFoundError("No src_person day directory with _SUCCESS found")`.
5. Ожидаемый parquet: `{day_dir}/person.parquet`.

### Шаг 2. Отбор троек (`_eligible_exclusion_triples`)

1. `pd.read_parquet(src_parquet)` — все строки среза (без фильтра по оператору).
2. `ab_row_count = len(source)`.
3. Копия с колонками:
   - `imsi` ← `_norm_numeric_str(imsi)` (`to_numeric` → `Int64` → `string`);
   - `imei` — аналогично;
   - `msisdn` ← `_norm_numeric_str(isdn)` (поле person — `isdn`).
4. `dropna(subset=["imsi", "imei", "msisdn"])`.
5. `drop_duplicates(subset=["imsi", "imei", "msisdn"])` → `eligible_triple_count`.

### Шаг 3. Размер выборки (`BuildSrcExclParams.sample_size_for_ab`)

```
target = max(1, round(ab_row_count * pct_of_ab / 100))
sample_size = min(target, eligible_triple_count)
```

Если `sample_size <= 0` → `ValueError("No eligible rows with non-null isdn/imsi/imei in src_person")`.

### Шаг 4. Сэмплирование (`_sample_triples`)

`random.Random(seed).sample(list(eligible.index), k=sample_size)` → `eligible.loc[idx]`.

### Шаг 5. Запись трёх витрин (`_write_single_column`)

Для каждого из imsi / imei / msisdn:

1. DataFrame одной колонки `value` (переименование в имя из `fields[0].name`, обычно `value`).
2. `_coerce_types` по схеме JSON (`string` / `long` / `int`).
3. `output_path = PROJECT_ROOT / readiness.s3_layout` (или абсолютный путь).
4. `mkdir(parents=True)`; `to_parquet(..., compression=snappy, index=False)` — перезапись.

### Шаг 6. Метрики

`append_command_metrics(command="build-src-excl", metrics={source_parquet, source_day, ab_row_count, eligible_triple_count, pct_of_ab, sample_size, seed, пути трёх файлов, elapsed_total_sec})`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет конфига; нет `_SUCCESS` или `person.parquet` |
| `ValueError` | Нет строк с полной тройкой imsi/imei/msisdn |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схемы | [`imsi.json`](../../src/mobile/schema/src/imsi.json), [`imei.json`](../../src/mobile/schema/src/imei.json), [`msisdn.json`](../../src/mobile/schema/src/msisdn.json), [`person.json`](../../src/mobile/schema/src/person.json) |
| ETL | [`src/mobile/pipelines/src/excl.py`](../../src/mobile/pipelines/src/excl.py) |
| Пути | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
