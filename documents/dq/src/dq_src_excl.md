# dq-src-excl

**Витрины:** `src_imsi`, `src_imei`, `src_msisdn` · **Команда:** `dq-src-excl` · **Режим:** read-only DQ (процесс не падает при failed checks).

Референс: [`pipelines/dq/src/excl.py`](../../../src/mobile/pipelines/dq/src/excl.py). Сборка списков: [`build_src_excl.md`](../../src/build_src_excl.md).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать три Parquet списков исключений | DataFrame по каждой витрине |
| 2 | Проверить наличие файлов и колонку `value` | Checks с префиксом витрины |
| 3 | Посчитать базовые метрики по каждому списку | `row_count`, `unique_count`, `null_count` |
| 4 | Сформировать `summary` | Счётчики checks и итоговый статус |

**Бизнес-назначение:** контроль качества списков идентификаторов для исключения из обработки (IMSI, IMEI, MSISDN).

**В scope задач:** наличие parquet, одноколоночная структура `value`, базовые totals по каждой витрине.

---

## TODO

1. При необходимости добавить cross-mart checks (равенство `row_count` между тремя списками).

---

## Параметры запуска

Вызов: `run_dq(src_imsi_path, src_imei_path, src_msisdn_path)` ([`cli.py`](../../../src/mobile/cli.py) → `dq-src-excl`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `src_imsi_path` | path | Нет | `data/src/excl/src_imsi.parquet` | Parquet `src_imsi` (`--src-imsi-path`) |
| `src_imei_path` | path | Нет | `data/src/excl/src_imei.parquet` | Parquet `src_imei` (`--src-imei-path`) |
| `src_msisdn_path` | path | Нет | `data/src/excl/src_msisdn.parquet` | Parquet `src_msisdn` (`--src-msisdn-path`) |

Локальный запуск:

```bash
uv run mobile build-src-excl
uv run mobile dq-src-excl
uv run mobile dq-src-excl --src-imsi-path data/src/excl/src_imsi.parquet --src-imei-path data/src/excl/src_imei.parquet --src-msisdn-path data/src/excl/src_msisdn.parquet
uv run mobile nb-src-excl
```

Логи: `data/logs/mobile.log` (тег `DQ_SRC_EXCL`). Метрики времени: `data/qa/command_timing.jsonl`, `command=dq-src-excl`.

---

## Структура проверяемых витрин

| Витрина | JSON | Путь (по умолчанию) | Поля |
|---------|------|---------------------|------|
| `src_imsi` | [`imsi.json`](../../../src/mobile/schema/src/imsi.json) | `data/src/excl/src_imsi.parquet` | `value` (string) |
| `src_imei` | [`imei.json`](../../../src/mobile/schema/src/imei.json) | `data/src/excl/src_imei.parquet` | `value` (string) |
| `src_msisdn` | [`msisdn.json`](../../../src/mobile/schema/src/msisdn.json) | `data/src/excl/src_msisdn.parquet` | `value` (string) |

| Свойство | Значение |
|----------|----------|
| Формат | Parquet |
| Партиционирование | Нет |
| Набор полей | Одна колонка `value` на витрину |
| Источник сборки | Последний full snapshot `src_person` с `_SUCCESS` ([`build-src-excl`](../../src/build_src_excl.md)) |

---

## Источники

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | Parquet `src_imsi` | `data/src/excl/src_imsi.parquet` | Список IMSI для исключения |
| 2 | Parquet `src_imei` | `data/src/excl/src_imei.parquet` | Список IMEI для исключения |
| 3 | Parquet `src_msisdn` | `data/src/excl/src_msisdn.parquet` | Список MSISDN для исключения |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Резолв путей `src_imsi_path`, `src_imei_path`, `src_msisdn_path` (по умолчанию — `DEFAULT_SRC_EXCL_*_OUTPUT` из [`project_paths.py`](../../../src/mobile/project_paths.py)).
2. Формирование словаря витрин `{src_imsi, src_imei, src_msisdn}` → абсолютные пути parquet.

### Шаг 1. Проверки по каждой витрине

Для каждой витрины (`src_imsi`, `src_imei`, `src_msisdn`) последовательно:

1. **`{mart}.dataset_presence`** — проверка существования parquet; при отсутствии файла витрина пропускается, остальные проверяются дальше.
2. Чтение DataFrame из parquet.
3. **`{mart}.dataset_basic`** — `row_count`, `column_count`, `parquet_path` (в т.ч. при `row_count=0`).
4. **`{mart}.schema_columns`** — наличие колонки `value`; при отсутствии totals не считаются.
5. **`{mart}.totals`** — `row_count`, `unique_count`, `null_count` по колонке `value`.
6. Каждый check логируется: `{"tag":"DQ_SRC_EXCL","check":"...","status":"...","metrics":{...}}`.

### Шаг 2. Итог

`summary` с `total_checks`, `warning_checks`, `failed_checks`; return dict со `status`, `marts` (статус и метрики по каждой витрине).

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `{mart}.dataset_presence` **failed** | Нет входного parquet (не выполнен `build-src-excl`) |
| `{mart}.schema_columns` **failed** | В parquet нет колонки `value` |
| Битый parquet | исключение pandas/pyarrow |

---

## Проверки

Статусы: **ok** / **failed** (в текущей реализации warning-checks не используются).

### Базовые проверки по витрине

Checks с префиксом `src_imsi`, `src_imei` или `src_msisdn`:

| Check | Статус при сбое | Смысл | Обоснование |
|-------|-----------------|-------|-------------|
| `{mart}.dataset_presence` | **failed** | Parquet по пути витрины не найден | Без файла списка исключений downstream-фильтры не имеют входа |
| `{mart}.dataset_basic` | **ok** | `row_count`, `column_count` | Фиксация объёма и ширины витрины; пустой файл допустим как метрика |
| `{mart}.schema_columns` | **failed** | Нет колонки `value` | Контракт одноколоночной витрины excl; имя колонки зафиксировано сборкой |
| `{mart}.totals` | **ok** | `row_count`, `unique_count`, `null_count` по `value` | Базовый профиль полноты и уникальности идентификаторов в списке |

### Итог

| Check | Смысл | Обоснование |
|-------|-------|-------------|
| `summary` | `total_checks`, `warning_checks`, `failed_checks`; итоговый статус run | Сводка прогона для мониторинга, сравнения прогонов и CI |

CLI не завершается с ненулевым exit code при failed checks (read-only DQ).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| DQ pipeline | [`pipelines/dq/src/excl.py`](../../../src/mobile/pipelines/dq/src/excl.py) |
| ETL build excl | [`pipelines/src/excl.py`](../../../src/mobile/pipelines/src/excl.py) |
| DQ notebook | [`pipelines/nb/7_src_excl.ipynb`](../../../src/mobile/pipelines/nb/7_src_excl.ipynb) |
| CLI wiring | [`cli.py`](../../../src/mobile/cli.py) |
| JSON-схемы | [`schema/src/imsi.json`](../../../src/mobile/schema/src/imsi.json), [`imei.json`](../../../src/mobile/schema/src/imei.json), [`msisdn.json`](../../../src/mobile/schema/src/msisdn.json) |
