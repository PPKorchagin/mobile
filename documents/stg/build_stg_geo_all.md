# build-stg-geo-all

**Витрина:** `stg_geo_all` · **Команда:** `build-stg-geo-all` · **Режим:** сборка дневного Parquet из `event_dds` с enrich по `stg_bs`.

Референс: [`pipelines/stg/geo_all.py`](../../src/mobile/pipelines/stg/geo_all.py). Схема витрины: [`geo_all.json`](../../src/mobile/schema/stg/geo_all.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `event_dds` за отчётную дату | DataFrame событий |
| 2 | Нормализовать поля (`msisdn`/`imsi`/`imei`), собрать `cgi` из `location` | Канон событий для join |
| 3 | Обогатить через `stg_bs` (координаты, тип БС, ОКТМО, timezone) | Гео-атрибуты в витрине |
| 4 | Провалидировать записи и выполнить 5m-агрегацию последовательностей | Уплотненный поток событий |
| 5 | Записать `stg_geo_all` | Дневной parquet-файл |

**Бизнес-назначение:** дневной геослой событий абонентов для downstream-аналитики и построения интервалов/трасс.

**В scope задач:** join `event_dds` + `stg_bs`, UTC-нормализация, отбор в витрину **только по UTC-дате**, валидация координат и ключей, 5m-схлопывание.

**Важно:** в этой реализации **не используется** дозаполнение через `stg_msisdn_imsi` и `stg_msisdn_imei`.

---

## TODO

1. Добавить DQ-команду `dq-stg-geo-all`.
2. При необходимости вынести `AGG_GAP_SECONDS=300` в конфиг CLI.

---

## Параметры запуска

Вызов: `run_build(report_date, event_dds_path, stg_bs_path, output_path)` ([`cli.py`](../../src/mobile/cli.py) → `build-stg-geo-all`).

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `report_date` | date | Да | — | Отчётный день |
| `event_dds_path` | path | Нет | `data/stg/event_dds` | Корень DDS, каталог дня или parquet-файл `{dc}.parquet` |
| `stg_bs_path` | path | Нет | `data/stg/bs.parquet` | Входной parquet витрины БС для enrich |
| `output_path` | path | Нет | `data/stg/geo_all/{report_date}.parquet` | Выходной parquet |

Parquet всегда пишется со сжатием **`snappy`** (`DEFAULT_PARQUET_COMPRESSION`).

Локальный запуск:

```bash
uv run mobile build-stg-geo-all --report-date 2025-01-01
uv run mobile build-stg-geo-all --report-date 2025-01-01 --event-dds-path data/stg/event_dds
uv run mobile build-stg-geo-all --report-date 2025-01-01 --stg-bs-path data/stg/bs.parquet
uv run mobile build-stg-geo-all --report-date 2025-01-01 --output-path data/stg/geo_all/2025-01-01.parquet
```

Логи: `data/logs/mobile.log` (строка `build-stg-geo-all completed`). Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-geo-all`.
Дополнительно пишутся технические счётчики нормализации:
`rows_norm_error_event_timestamp`, `rows_norm_error_location_parts`, `rows_norm_error_msisdn`,
`rows_norm_error_imsi`, `rows_norm_error_imei`, `rows_norm_error_event_count`,
`rows_norm_error_event_type`, `rows_norm_error_cgi`, `rows_cgi_imputed`.

---

## Структура генерируемой витрины

| Свойство | Значение |
|----------|----------|
| Имя таблицы | `stg_geo_all` — [`geo_all.json`](../../src/mobile/schema/stg/geo_all.json) |
| Формат хранения | Parquet |
| Календарный срез | `report_date` |
| Сжатие | `snappy` |

### Путь выхода

Шаблон: `STG_GEO_ALL_LAYOUT_TEMPLATE` в [`project_paths.py`](../../src/mobile/project_paths.py):

`data/stg/geo_all/{YYYY-MM-DD}.parquet`

### Поля витрины

Контракт — [`geo_all.json`](../../src/mobile/schema/stg/geo_all.json) → `fields`.

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `msisdn` | long | MSISDN из `event_dds` |
| 2 | `imsi` | long | IMSI из `event_dds` |
| 3 | `imei` | long | IMEI из `event_dds` |
| 4 | `start_time_utc` | timestamp | Время события в UTC |
| 5 | `end_time_utc` | timestamp | Конец интервала после агрегации (иначе `null`) |
| 6 | `utc_offset` | int | Смещение в часах (как `stg_bs.timezone`) |
| 7 | `lat` | float | Широта БС |
| 8 | `lon` | float | Долгота БС |
| 9 | `bs_type` | string | Тип БС |
| 10 | `cgi` | long | Ключ БС `mcc×10¹³ + mnc×10¹¹ + lac×10⁶ + cell` |
| 11 | `event_count` | int | Число событий в агрегированной группе |
| 12 | `source_event_type` | string | `cdr` / `sms` / `gprs` / `location` |
| 13 | `oktmo_code_1` | string | ОКТМО уровень 1 |
| 14 | `oktmo_code_2` | string | ОКТМО уровень 2 |

---

## Источники витрины

| # | Источник | Путь | Назначение |
|---|----------|------|------------|
| 1 | `event_dds` | `data/stg/event_dds/{YYYY-MM-DD}/{dc}.parquet` | События за день |
| 2 | `stg_bs` | `data/stg/bs.parquet` | Lookup БС (координаты, ОКТМО, timezone, интервалы активности) |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Разрешить пути `event_dds_path` и `output_path`.
2. Проверить существование `stg_bs`.

### Шаг 1. Чтение событий `event_dds`

1. Найти parquet-файлы за `report_date` (`discover_event_dds_parquet_paths`).
2. Прочитать только нужные поля: `event_timestamp`, `imsi`, `imei`, `msisdn`, `location`, `event`, `event_name`, `event_count`.
3. Отфильтровать только по формату `event_timestamp=YYYYMMDDhhmmss` (без day-фильтра в локальном времени).

### Шаг 2. Нормализация и enrich

1. Разобрать `location` (dict/tuple/string) в `mcc/mnc/lac/cell`.
2. Построить `cgi` только для строк с валидными `mcc/mnc/lac/cell`.
3. Нормализовать `msisdn` (формат 7XXXXXXXXXX), `imsi`, `imei`.
4. Определить `source_event_type` из `event_name` или кода `event`.
5. Для событий с битым/пустым `cgi` восстановить `cgi` между известными точками:
   - внутри последовательности одного `msisdn` по `start_time_local`,
   - только когда есть валидные БС-якоря до и после события,
   - выбрать промежуточную БС по интерполяции между координатами якорей и ближайшему `cgi` из lookup.
6. Join со `stg_bs` по `cgi`:
   - сначала матч по активному интервалу `date_on_bs <= start_time <= date_off_bs`,
   - затем fallback на ближайший `date_on_bs`.
7. Рассчитать UTC через `timezone` БС (`utc_offset` в часах, как в `stg_bs`).
8. Оставить в витрине только события, где `start_time_utc` попадает в интервал `[report_date 00:00:00, report_date+1d)` в UTC.

### Шаг 3. Валидация и агрегация

1. Оставить только записи с непустыми `msisdn`, `cgi`, `start_time_utc`.
2. Проверить диапазоны координат (`lat`, `lon`).
3. Сортировка по `msisdn`, `start_time_utc`, `source_event_type`, `cgi`.
4. 5m-схлопывание последовательностей:
   - одинаковые `msisdn + source_event_type + cgi + bucket_5m`,
   - разрыв по времени > 300 секунд начинает новую группу.
5. `event_count` = размер группы.

### Шаг 4. Запись

1. Записать итоговый DataFrame в parquet (`snappy`).
2. Записать метрики в `command_timing.jsonl` (`append_command_metrics`).

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError: stg_bs parquet not found` | Не подготовлен `stg_bs` |
| Пустой выходной файл/0 строк | Нет событий `event_dds` за день или все строки отфильтрованы |
| Сильное падение `rows_after_validate` | Невалидные `msisdn/cgi/coords` во входе |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема витрины | [`src/mobile/schema/stg/geo_all.json`](../../src/mobile/schema/stg/geo_all.json) |
| ETL | [`src/mobile/pipelines/stg/geo_all.py`](../../src/mobile/pipelines/stg/geo_all.py) |
| Пути/лейауты | [`src/mobile/project_paths.py`](../../src/mobile/project_paths.py) |
| CLI | [`src/mobile/cli.py`](../../src/mobile/cli.py) |
| Источник событий | [`build_stg_event.md`](./build_stg_event.md), [`build_move_event.md`](./build_move_event.md) |

