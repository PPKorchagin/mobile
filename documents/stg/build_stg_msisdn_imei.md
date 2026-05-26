# build-stg-msisdn-imei

**Витрина:** `stg_msisdn_imei` · **Команда:** `build-stg-msisdn-imei` · **Режим:** интервалы актуальности MSISDN↔IMEI за отчётный день (один Parquet на дату).

Референс: [`pipelines/stg/msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py). Схема витрины: [`msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `event_dds` за `report_date` (все ЦОД) | DataFrame событий |
| 2 | Нормализовать MSISDN и IMEI, отфильтровать валидные пары | События с `msisdn`, `imei`, `event_ts` |
| 3 | Построить интервалы по смене IMEI на MSISDN | `valid_from` / `valid_to` |
| 4 | Записать витрину в Parquet | Файл `output_path` |

**Бизнес-назначение:** для любого момента внутри суток знать, какое устройство (IMEI) наблюдалось у MSISDN по фактическим событиям.

**В scope задач:** чтение DDS, нормализация, интервалы, запись Parquet. Логика интервалов совпадает с [`build-stg-msisdn-imsi`](./build_stg_msisdn_imsi.md), вместо `imsi` используется `imei`.

---

## TODO

1. DQ-витрина `dq-stg-msisdn-imei`.
2. Сквозная цепочка `run-all`.

---

## Параметры запуска

Переменные, передаваемые в job (аргументы `run_build()`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да* | — | Отчётный день |
| `event_dds_path` | string (path) | Нет | `data/stg/event_dds` | Корень DDS, каталог дня или файл |
| `output_path` | string (path) | Нет | `data/stg/msisdn_imei/{report_date}.parquet` | Выходной Parquet (перезапись) |

\* Без `--report-date` — цикл дней из [`cli_defaults.py`](../../src/mobile/cli_defaults.py).

**Предусловие:** `build-move-event` за ту же `report_date`.

```bash
uv run mobile build-stg-msisdn-imei --report-date 2025-01-01
uv run mobile build-stg-msisdn-imei --report-date 2025-01-01 \
  --output-path data/stg/msisdn_imei/2025-01-01.parquet
```

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_msisdn_imei` — [`msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json) |
| Формат | Parquet, один файл на `report_date` |
| Сжатие | `snappy` |

### Поля витрины

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | string | MSISDN, E.164 |
| 2 | `imei` | string | IMEI, 14–16 цифр |
| 3 | `valid_from` | timestamp | Начало интервала |
| 4 | `valid_to` | timestamp | Конец интервала |

### Нормализация

| Поле | Правила ([`subscriber_ids.py`](../../src/mobile/pipelines/stg/subscriber_ids.py)) |
|------|--------|
| `msisdn` | Только цифры; RU 10→`7…`, `8XXXXXXXXXX`→`7…`; иностранные 7–15 цифр |
| `imei` | 14–16 цифр |

Время интервалов — локальное из `event_timestamp` (как в `event_dds`).

---

## Источники и алгоритм

Источник и шаги 1–4 — как в [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md) (разделы «Источники витрины» и «Алгоритм»), с заменой `imsi` → `imei` и `normalize_imei`.

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`src/mobile/schema/stg/msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json) |
| ETL | [`src/mobile/pipelines/stg/msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py) |
| MSISDN–IMSI | [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md) |
| event_dds | [`build_move_event.md`](./build_move_event.md) |
