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

## Алгоритм

### Шаг 1. Загрузка

`load_command_metrics_df(metrics_path)`.

### Шаг 2. Последний прогон

Для каждой `command` — последняя запись по `finished_at` / `run_id`; bar chart `elapsed_total_sec`.

### Шаг 3. История

Line plot по каждой команде: `elapsed_total_sec` vs время.

### Типовые ситуации

| Ситуация | Поведение |
|----------|-----------|
| Пустой JSONL | пустые графики / сообщение |
| Команда не запускалась | отсутствует на графике |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Notebook | [`perf_metrics.ipynb`](../../src/mobile/nb/perf_metrics.ipynb) |
| Runner | [`perf_metrics.py`](../../src/mobile/pipelines/nb/perf_metrics.py) |
| Timing | [`command_timing.py`](../../src/mobile/command_timing.py) |
