# nb-perf-metrics

**Артефакт:** notebook · **Команда:** `nb-perf-metrics` · **Режим:** визуализация wall-time из `command_timing.jsonl`.

Референс: [`src/mobile/nb/perf_metrics.ipynb`](../../src/mobile/nb/perf_metrics.ipynb), [`pipelines/nb/perf_metrics.py`](../../src/mobile/pipelines/nb/perf_metrics.py).

---

## Задачи

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `data/qa/command_timing.jsonl` | DataFrame метрик |
| 2 | Сводный график последнего прогона по командам | PNG / display |
| 3 | Графики истории по каждой команде | Отдельные series |

**Бизнес-назначение:** сравнение длительности ETL/DQ-команд между прогонами и регрессии производительности.

**В scope:** только чтение JSONL и matplotlib; пайплайны данных не запускаются.

---

## Параметры запуска

| Переменная | Тип | По умолчанию | Описание |
|------------|-----|--------------|----------|
| — | — | — | Отдельных CLI-флагов нет |

```bash
uv run mobile nb-perf-metrics
```

Выход (по умолчанию): `data/notebooks/perf_metrics.executed.ipynb` (если настроено в `perf_metrics.run`).

Вход: [`command_timing.py`](../../src/mobile/command_timing.py) → `DEFAULT_METRICS_PATH` = `data/qa/command_timing.jsonl`.

---

## Источники

| # | Источник | Путь |
|---|----------|------|
| 1 | Метрики CLI | `data/qa/command_timing.jsonl` |
| 2 | Детальные этапы | поля `*_sec` из `COMMANDS_WITH_DETAILED_TIMING` |

Команды в фиксированном порядке — см. `COMMAND_ORDER` в notebook (build-stg-*, dq-*, src-*).

---

## Алгоритм обработки данных

Точка входа: `run()` в [`perf_metrics.py`](../../src/mobile/pipelines/nb/perf_metrics.py) → `run_notebook` для [`perf_metrics.ipynb`](../../src/mobile/nb/perf_metrics.ipynb).

### Шаг 0. Подготовка окружения

1. Удалить предыдущий `data/notebooks/perf_metrics.executed.ipynb` (если есть).
2. `run_notebook(source, executed, timeout_seconds=300)` — выполнение ячеек с `PROJECT_ROOT` в PYTHONPATH.
3. Входной путь: `ROOT / DEFAULT_METRICS_PATH` = `data/qa/command_timing.jsonl`.

### Шаг 1. Загрузка метрик

1. `load_command_metrics_df(METRICS)` ([`command_timing.py`](../../src/mobile/command_timing.py)):
   - построчное чтение JSONL;
   - парсинг в `DataFrame` с колонками `command`, `ts_utc`, `elapsed_total_sec`, `status`, `run_id`, опционально `*_sec` для детальных стадий.
2. Если `raw.empty` — вывод сообщения «выполните build-src…» и пропуск графиков.

### Шаг 2. Сводка последнего прогона по командам

1. `latest_idx = raw.groupby("command")["ts_utc"].idxmax()` — последняя запись на команду.
2. `latest_per_command = raw.loc[latest_idx]`.
3. Сортировка `_command_sort_key`: сначала команды из `COMMAND_ORDER`, остальные — лексикографически.
4. Таблица для display: `command`, `ts_utc`, `elapsed_total_sec`, `status`, `run_id`.
5. **Bar chart (horizontal):**
   - ось Y — команды;
   - ось X — `elapsed_total_sec`;
   - высота фигуры ~ `0.45 * n_commands`.

### Шаг 3. История по каждой команде

1. Для каждого уникального `command` в `raw`:
   - подмножество строк с этой командой;
   - сортировка по `ts_utc`;
   - line plot: время прогона vs `elapsed_total_sec`.
2. Позволяет увидеть регрессии после изменений ETL.

### Шаг 4. Детальные стадии (если есть в JSONL)

Для команд из `COMMANDS_WITH_DETAILED_TIMING` в метриках могут быть поля `read_*_sec`, `transform_sec`, `write_sec` — notebook может отображать их при наличии (см. ячейки notebook).

### Типовые ситуации

| Ситуация | Поведение |
|----------|-----------|
| Пустой JSONL | сообщение в stdout, без графиков |
| Команда не запускалась | нет на сводном bar chart |
| Несколько прогонов в день | в истории — несколько точек; в сводке — только последний по `ts_utc` |
| Timeout notebook | ошибка `run_notebook` после 300s |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Notebook | [`perf_metrics.ipynb`](../../src/mobile/nb/perf_metrics.ipynb) |
| Runner | [`perf_metrics.py`](../../src/mobile/pipelines/nb/perf_metrics.py) |
| Timing | [`command_timing.py`](../../src/mobile/command_timing.py) |
