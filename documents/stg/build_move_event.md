# build-move-event

**Витрина:** `stg_event` (DDS-слой) · **Команда:** `build-move-event` · **Режим:** локальное копирование дневных Parquet по ЦОД в плоский layout.

Референс: [`pipelines/stg/move_event.py`](../../src/mobile/pipelines/stg/move_event.py). Схема данных — та же, что у [`stg_event`](./build_stg_event.md): [`event.json`](../../src/mobile/schema/stg/event.json).

> **Заглушка (stub):** команда имитирует перенос в DDS-layout в dev/тестовом контуре (`data/stg/event_dds/`). **На проде** доставку файлов в целевой каталог **выполняет вручную поставщик**; автоматический transfer в боевой контур **не** входит в scope пайплайна.

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Для каждого ЦОД найти `events.parquet` за `report_date` | Путь в `data/stg/event/…` |
| 2 | Скопировать файл в DDS-каталог (параллельно по ЦОД) | `data/stg/event_dds/{YYYY-MM-DD}/{source_id}.parquet` |
| 3 | Записать метрики переноса | `command_timing.jsonl`, лог |

**Бизнес-назначение:** плоский дневной срез событий по ЦОД для локального [`dq-stg-event`](../dq/stg/dq_stg_event.md) и downstream geo-слоя (один файл на дату × ЦОД).

**В scope задач:** быстрое копирование (`hardlink` на одном томе, иначе `copyfile`), создание каталога назначения, лог по каждому ЦОД. Трансформации данных **нет** — содержимое идентично [`build-stg-event`](./build_stg_event.md).

**Предусловие:** `uv run mobile build-stg-event` за ту же `report_date` (файлы `data/stg/event/{YYYY}/{MM}/{DD}/{source_id}/events.parquet`).

---

## TODO

1. При появлении `run-all` — встроить после `build-stg-event` в сквозную цепочку.

---

## Параметры запуска

Вызов pipeline: `run_move(report_date)` ([`cli.py`](../../src/mobile/cli.py) → `build-move-event`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | **Да** | `DEFAULT_SRC_*` (оркестратор) | Отчётный день (`--report-date`) |

Пути `src` / `dst` вычисляются в коде — `stg_event_output_path` / `stg_event_dds_output_path` в [`project_paths.py`](../../src/mobile/project_paths.py); на вход job **не передаются**.

**Константы ETL в коде** ([`move_event.py`](../../src/mobile/pipelines/stg/move_event.py), на вход job **не передаются**):

| Константа | Значение |
|-----------|----------|
| `_COPY_WORKERS` | `len(mobile_datacenter_ids())` — параллельное копирование по ЦОД |
| Стратегия копирования | `hardlink` (один том) → `copyfile` (разные тома) |

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)); на каждый день — timed-run `build-move-event-{YYYY-MM-DD}` (оба ЦОД в одном процессе) |
| `--report-date` | Один отчётный день, оба ЦОД |

Локальный запуск:

```bash
uv run mobile build-stg-event
uv run mobile build-move-event
uv run mobile build-move-event --report-date 2025-01-01
```

Логи: `data/logs/mobile.log` (`build-move-event source_id=… method=hardlink|copyfile`). Метрики: `data/qa/command_timing.jsonl`, `command=build-move-event` или `build-move-event-{date}`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя слоя | DDS-копия `stg_event` — [`event.json`](../../src/mobile/schema/stg/event.json) → `table` |
| Описание | Тот же контракт полей, что у `stg_event` — `description` в JSON |
| Формат хранения | Parquet (копия исходного файла) |
| Партиционирование | Один файл на `report_date` × `source_id` (ЦОД) |
| Календарный срез | `report_date` (`YYYY-MM-DD` в пути DDS) |
| Сжатие | Как у источника (`snappy` после `build-stg-event`) |

### Путь выхода

Шаблон `STG_EVENT_DDS_LAYOUT_TEMPLATE` в [`project_paths.py`](../../src/mobile/project_paths.py):

`data/stg/event_dds/{YYYY-MM-DD}/{source_id}.parquet`

Примеры:

- `data/stg/event_dds/2025-01-01/central.parquet`
- `data/stg/event_dds/2025-01-01/far-east.parquet`

### Поля витрины

Контракт — [`event.json`](../../src/mobile/schema/stg/event.json) → `fields`; идентичен [`build_stg_event.md`](./build_stg_event.md) → раздел «Поля витрины» (`event_timestamp`, `imsi`, `imei`, `msisdn`, `location`, `event`, `event_name`, `event_count`).

---

## Источники витрины

По одному готовому Parquet на ЦОД из [`build-stg-event`](./build_stg_event.md).

| ЦОД (`source_id`) | Вход | Выход |
|-------------------|------|-------|
| `central` | `data/stg/event/{YYYY}/{MM}/{DD}/central/events.parquet` | `data/stg/event_dds/{YYYY-MM-DD}/central.parquet` |
| `far-east` | `data/stg/event/{YYYY}/{MM}/{DD}/far-east/events.parquet` | `data/stg/event_dds/{YYYY-MM-DD}/far-east.parquet` |

Список ЦОД — `mobile_datacenter_ids()` в [`project_paths.py`](../../src/mobile/project_paths.py).

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Принять `report_date`.
2. Для каждого `source_id` из `mobile_datacenter_ids()` вычислить `src` (`stg_event_output_path`) и `dst` (`stg_event_dds_output_path`).

### Шаг 1. Перенос по ЦОД

Для `central` и `far-east` **параллельно** (`ThreadPoolExecutor`, I/O-bound):

1. Если `src` не существует:
   - в `moves` — `{source_id, status: "missing_source", …}`;
   - **warning** в лог; `files_written` не увеличивается;
   - следующий ЦОД без исключения.
2. Иначе:
   - `dst.parent.mkdir(parents=True, exist_ok=True)`;
   - при общем томе с каталогом назначения — **`os.link`** (`copy_method: hardlink`);
   - иначе — **`shutil.copyfile`** (`copy_method: copyfile`);
   - `files_written += 1`; лог: `bytes`, пути.
3. Исходный `data/stg/event/…` **не** удаляется.

### Шаг 2. Метрики

1. `append_command_metrics(command="build-move-event", …)` — `files_written`, массив `moves`, `elapsed_total_sec`.

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `SystemExit` | CLI: явный прогон без `--report-date` |
| Warning, `missing_source` | Нет `events.parquet` для ЦОД за день |
| `files_written=0` | Нет ни одного файла за день (процесс без исключения) |
| `OSError` | Диск / права при `link` / `copyfile` |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/stg/event.json`](../../src/mobile/schema/stg/event.json) |
| ETL | [`src/mobile/pipelines/stg/move_event.py`](../../src/mobile/pipelines/stg/move_event.py) |
| Пути layout | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
| Сборка событий | [`build_stg_event.md`](./build_stg_event.md) |
| DQ DDS | [`dq_stg_event.md`](../dq/stg/dq_stg_event.md) |

Сквозная цепочка (локально): `build-src-mobile` → `build-stg-event` → `build-move-event` (stub) → `dq-stg-event`. На **проде** между `build-stg-event` и `dq-stg-event` — ручной перенос поставщиком.
