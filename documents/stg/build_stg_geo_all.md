# build-stg-geo-all

**Витрина:** `stg_geo_all` · **Команда:** `build-stg-geo-all` · **Режим:** сборка дневного Parquet из `event_dds` с enrich по `fct_bs`.

Референс: [`pipelines/stg/geo_all.py`](../../src/mobile/pipelines/stg/geo_all.py). Схема витрины: [`geo_all.json`](../../src/mobile/schema/stg/geo_all.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `event_dds` за отчётную дату | DataFrame событий |
| 2 | Нормализовать поля (`msisdn`/`imsi`/`imei`), собрать `cgi` из `location` | Канон событий для join |
| 3 | Обогатить через `fct_bs` (координаты, тип БС, ОКТМО, timezone) | Гео-атрибуты в витрине |
| 4 | Провалидировать записи и выполнить 5m-агрегацию последовательностей | Уплотненный поток событий |
| 5 | Записать `stg_geo_all` | Дневной parquet-файл |

**Бизнес-назначение:** дневной геослой событий абонентов для downstream-аналитики и построения интервалов/трасс.

**В scope задач:** join `event_dds` + `fct_bs`, UTC-нормализация, отбор в витрину **только по UTC-дате**, валидация координат и ключей, 5m-схлопывание.

**Важно:** в этой реализации **не используется** дозаполнение через `fct_msisdn_imsi` и `fct_msisdn_imei`.

---

## TODO

1. При необходимости вынести `AGG_GAP_SECONDS=300` в конфиг CLI.

---

## Параметры запуска

Вызов: `run_build(report_date, event_dds_path, fct_bs_path, output_path)` ([`cli.py`](../../src/mobile/cli.py) → `build-stg-geo-all`). **Все четыре обязательны** — pipeline не подставляет пути по умолчанию; их резолвит CLI-оркестратор или явный вызов.

| Переменная | Тип | Обязательность | Описание |
|------------|-----|----------------|----------|
| `report_date` | date | **Да** | Отчётный день (UTC-срез в витрине) |
| `event_dds_path` | path | **Да** | Корень DDS, каталог дня или parquet `{dc}.parquet` |
| `fct_bs_path` | path | **Да** | Входной parquet `fct_bs` для enrich |
| `output_path` | path | **Да** | Выходной parquet `stg_geo_all` |

Parquet всегда пишется со сжатием **`snappy`** (`DEFAULT_PARQUET_COMPRESSION`).

**Константы ETL в коде** (на вход job **не передаются**): `_AGG_GAP_SECONDS=300`, `_READ_COLUMNS`, `_OUTPUT_COLUMNS` — см. [`geo_all.py`](../../src/mobile/pipelines/stg/geo_all.py).

### CLI

| Режим | Поведение |
|-------|-----------|
| Без флагов | Цикл `DEFAULT_SRC_START_DATE` … `DEFAULT_SRC_END_DATE` ([`cli_defaults.py`](../../src/mobile/cli_defaults.py)); на каждый день — timed-run `build-stg-geo-all-{YYYY-MM-DD}` с путями `data/dds/event_dds`, `data/fct/bs.parquet`, `data/stg/geo_all/{date}.parquet` |
| Все 4 явно | `--report-date`, `--event-dds-path`, `--fct-bs-path`, `--output-path` (один прогон) |

**Предусловие:** `build-dds-move-event` (или готовый `event_dds`) и `build-fct-bs` за тот же период.

Локальный запуск:

```bash
uv run mobile build-dds-move-event
uv run mobile build-fct-bs
uv run mobile build-stg-geo-all
uv run mobile build-stg-geo-all --report-date 2025-01-01 \
  --event-dds-path data/dds/event_dds \
  --fct-bs-path data/fct/bs.parquet \
  --output-path data/stg/geo_all/2025-01-01.parquet
```

Логи: `data/logs/mobile.log` (строка `build-stg-geo-all completed`). Метрики: `data/qa/command_timing.jsonl`, `command=build-stg-geo-all` или `build-stg-geo-all-{date}`.
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
| 6 | `utc_offset` | int | Смещение в часах (как `fct_bs.timezone`) |
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
| 1 | `event_dds` | `data/dds/event_dds/{YYYY-MM-DD}/{dc}.parquet` | События за день |
| 2 | `fct_bs` | `data/fct/bs.parquet` | Lookup БС (координаты, ОКТМО, timezone, интервалы активности) |

---

## Алгоритм обработки данных

### Шаг 0. Инициализация

1. Разрешить пути `event_dds_path` и `output_path`.
2. Проверить существование `fct_bs`.

### Шаг 1. Чтение событий `event_dds`

1. Найти parquet-файлы за UTC-день (`_discover_event_dds_paths_for_utc_day` в [`geo_all.py`](../../src/mobile/pipelines/stg/geo_all.py)).
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
6. Join со `fct_bs` по `cgi`:
   - сначала матч по активному интервалу `date_on_bs <= start_time <= date_off_bs`,
   - затем fallback на ближайший `date_on_bs`.
7. Рассчитать UTC через `timezone` БС (`utc_offset` в часах, как в `fct_bs`).
8. Оставить в витрине только события, где `start_time_utc` попадает в интервал `[report_date 00:00:00, report_date+1d)` в UTC.

### Шаг 3. Валидация и агрегация

1. Отбор строк: `msisdn`, `cgi`, `start_time_utc` not null; координаты в допустимых диапазонах широты/долготы.
2. Метрики отсева: `rows_after_validate` vs `rows_read`.
3. Сортировка: `msisdn`, `start_time_utc`, `source_event_type`, `cgi` (детерминированный порядок группировки).
4. **5m-схлопывание** (`_collapse_5m`):
   - `bucket_5m = floor(start_time_local to 5 minutes)`;
   - ключ группы: `(msisdn, source_event_type, cgi, bucket_5m)`;
   - внутри группы: новая подгруппа, если `delta_sec > 300` между соседними событиями;
   - `start_time_utc` / `end_time_utc` группы = min/max по событиям;
   - `event_count` = число исходных событий в группе;
   - координаты и `oktmo_*` — из агрегированной строки (веса по `event_count` на этапе join с BS уже применены).
5. Итоговый DataFrame — контракт [`geo_all.json`](../../src/mobile/schema/stg/geo_all.json).

### Шаг 4. Запись

1. Записать итоговый DataFrame в parquet (`snappy`).
2. Записать метрики в `command_timing.jsonl` (`append_command_metrics`).

### Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError: fct_bs parquet not found` | Не подготовлен `fct_bs` |
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
| DQ | [`documents/dq/stg/dq_stg_geo_all.md`](../dq/stg/dq_stg_geo_all.md) |
| Источник событий | [`build_dds_event.md`](../dds/build_dds_event.md), [`build_dds_move_event.md`](../dds/build_dds_move_event.md) |

