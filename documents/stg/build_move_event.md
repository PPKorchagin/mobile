# build-move-event

**Витрина:** `stg_event` (DDS-слой) · **Команда:** `build-move-event` · **Режим:** копирование дневных Parquet по ЦОД в плоский layout.

Референс: [`pipelines/stg/move_event.py`](../../src/mobile/pipelines/stg/move_event.py). Схема данных — та же, что у [`stg_event`](./build_stg_event.md): [`event.json`](../../src/mobile/schema/stg/event.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Для каждого ЦОД найти `events.parquet` за `report_date` | Путь в `data/stg/event/…` |
| 2 | Скопировать файл в DDS-каталог | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet` |
| 3 | Записать метрики переноса | `command_timing.jsonl`, лог |

**Бизнес-назначение:** плоский дневной срез событий по ЦОД для потребителей DDS (один файл на дату × ЦОД, без вложенного `YYYY/MM/DD` в пути).

**В scope задач:** побайтовое копирование (`shutil.copy2`), создание каталога назначения, лог по каждому ЦОД. Трансформации данных **нет** — содержимое идентично `build-stg-event`.

---

## TODO

1. Включить в сквозную цепочку после `build-stg-event`, если появится `run-all`.

---

## Параметры запуска

Вызов: `run_move(report_date)` ([`cli.py`](../../src/mobile/cli.py) → `build-move-event`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | Отчётный день (тот же, что в `build-stg-event`) |

Пути в коде — `stg_event_output_path` / `stg_event_dds_output_path` в [`project_paths.py`](../../src/mobile/project_paths.py); на вход job **не передаются**.

Оркестратор (без `--report-date`): цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` из [`cli_defaults.py`](../../src/mobile/cli_defaults.py); на каждый день — отдельный timed-run `build-move-event-{YYYY-MM-DD}`.

**Предусловие:** `uv run mobile build-stg-event` за ту же `report_date` (файлы в `data/stg/event/…`).

Локальный запуск:

```bash
# Все дни периода build-src-mobile
uv run mobile build-move-event

# Один отчётный день (оба ЦОД в одном процессе)
uv run mobile build-move-event --report-date 2025-01-01
```

Логи: `data/logs/mobile.log` (`build-move-event source_id=…`). Метрики: `data/qa/command_timing.jsonl`, `command=build-move-event` или `build-move-event-{date}`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя слоя | DDS-копия `stg_event` (схема — [`event.json`](../../src/mobile/schema/stg/event.json)) |
| Формат хранения | Parquet (копия исходного файла) |
| Партиционирование | Один файл на `report_date` × ЦОД |
| Календарный срез | `report_date` (`YYYY-MM-DD` в пути DDS) |
| Сжатие | Как у источника (`snappy` после `build-stg-event`) |

### Путь выхода

Шаблон: `STG_EVENT_DDS_LAYOUT_TEMPLATE` в [`project_paths.py`](../../src/mobile/project_paths.py):

`data/stg/event_dds/{YYYY-MM-DD}/{source_id}.parquet`

Примеры:

- `data/stg/event_dds/2025-01-01/central.parquet`
- `data/stg/event_dds/2025-01-01/far-east.parquet`

### Поля витрины

Совпадают с [`build_stg_event.md`](./build_stg_event.md) → раздел «Поля витрины» (`event_timestamp`, `imsi`, `imei`, `msisdn`, `location`, `event`, `event_name`, `event_count`).

---

## Источники витрины

По одному готовому parquet на ЦОД из `build-stg-event`.

| ЦОД (`source_id`) | Вход | Выход |
|-------------------|------|-------|
| `central` | `data/stg/event/{YYYY}/{MM}/{DD}/central/events.parquet` | `data/stg/event_dds/{YYYY-MM-DD}/central.parquet` |
| `far-east` | `data/stg/event/{YYYY}/{MM}/{DD}/far-east/events.parquet` | `data/stg/event_dds/{YYYY-MM-DD}/far-east.parquet` |

Список ЦОД — `mobile_datacenter_ids()` в [`project_paths.py`](../../src/mobile/project_paths.py).

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Принять `report_date`.
2. Для каждого `source_id` из `DEFAULT_MOBILE_DATACENTERS` вычислить `src` и `dst` через `stg_event_output_path` / `stg_event_dds_output_path`.

### Шаг 1. Перенос по ЦОД

Для каждого ЦОД:

1. Если `src` не существует — `status=missing_source`, **warning** в лог, переход к следующему ЦОД.
2. Иначе `mkdir -p` для родителя `dst`.
3. `shutil.copy2(src, dst)` — копия с сохранением метаданных файла.
4. Лог: `source_id`, `report_date`, `src`, `dst`, `bytes`.

### Шаг 2. Метрики

1. `append_command_metrics(command="build-move-event", …)` — `files_written`, массив `moves` по ЦОД, `elapsed_total_sec`.

### Типовые ошибки

| Ситуация | Поведение |
|----------|-----------|
| Нет `events.parquet` для ЦОД | Warning, `files_written` не увеличивается для этого ЦОД |
| Нет ни одного файла за день | `files_written=0`, процесс завершается без исключения |
| Диск / права на запись | `OSError` / `PermissionError` при `copy2` |

---

## Место в пайплайне

```text
build-src-mobile → build-stg-event → build-move-event → dq-stg-event → (потребители DDS)
```

DQ после переноса: [`dq-stg-event`](../dq/stg/dq_stg_event.md) на `data/stg/event_dds/…`.

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`src/mobile/pipelines/stg/move_event.py`](../../src/mobile/pipelines/stg/move_event.py) |
| Пути | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
| Сборка событий | [`build_stg_event.md`](./build_stg_event.md) |
| DQ DDS | [`dq_stg_event.md`](../dq/stg/dq_stg_event.md) — `dq-stg-event` |
| Схема полей | [`src/mobile/schema/stg/event.json`](../../src/mobile/schema/stg/event.json) |
