# build-src-mobile

Команда генерирует четыре мобильные витрины ОСС — **CDR**, **SMS**, **GPRS**, **location** — по календарным дням и операторам. Абонентский пул берётся из **`src_person`** (месячные срезы с `_SUCCESS`), геометрия БС — из **`data/src/bs.parquet`**. Без разбиения по ЦОД: один parquet на витрину × оператор × день.

**Запуск** (из корня репозитория):

```bash
uv run mobile build-src-mobile
```

Период, операторы, seed и `movement_ratio` — константы в [`cli_defaults.py`](../../src/mobile/cli_defaults.py) → `default_mobile_params()`. Флагов CLI нет. Entry point: `mobile = "mobile.cli:main"` в [`pyproject.toml`](../../pyproject.toml).

**Предварительно:** `build-src-bs`, `build-src-person` (на каждый месяц периода нужен `_SUCCESS` в каталоге person).

---

## На вход

| № | Артефакт | Формат | Путь (по умолчанию) | Назначение |
|---|----------|--------|---------------------|------------|
| 1 | [`cdr.json`](../../src/mobile/schema/src/cdr.json) … [`location.json`](../../src/mobile/schema/src/location.json) | JSON | `src/mobile/schema/src/` | Схемы полей, `readiness.s3_layout` |
| 2 | [`person.json`](../../src/mobile/schema/src/person.json) | JSON | `src/mobile/schema/src/person.json` | Шаблон каталогов person для пула абонентов |
| 3 | `src_bs` | Parquet | `data/src/bs.parquet` | БС (весь справочник, без фильтра субъектов) |
| 4 | `src_person` | Parquet + `_SUCCESS` | `data/src/person/load_year={YYYY}/load_month={MM}/load_day={DD}/` | Последний успешный срез месяца |
| 5 | Часовые пояса (опционально) | CSV | `src/mobile/raw_data/time_zones.csv` | Локальное время событий по `subject` + координатам БС |

---

## На выходе

| Витрина | Event | Путь (шаблон `s3_layout`) |
|---------|-------|---------------------------|
| CDR | `10001` | `data/src/mobile/operator/cdr/{name_operator}/10001/{YYYY}/{MM}/{DD}/` |
| SMS | `10002` | `data/src/mobile/operator/sms/{name_operator}/10002/{YYYY}/{MM}/{DD}/` |
| GPRS | `10003` | `data/src/mobile/operator/gprs/{name_operator}/10003/{YYYY}/{MM}/{DD}/` |
| location | `10004` | `data/src/mobile/operator/location/{name_operator}/10004/{YYYY}/{MM}/{DD}/` |

`{name_operator}` — латиница: `beeline`, `megafon`, `mts`, `tele2` (`OPERATOR_SLUG` в [`mobile.py`](../../src/mobile/pipelines/src/mobile.py)).

Имя файла — из `readiness` JSON (обычно `*.parquet` в каталоге дня). Сжатие: `snappy`.

---

## Параметры → `BuildSrcMobileParams`

Задаются в [`cli_defaults.py`](../../src/mobile/cli_defaults.py) → `default_mobile_params()`, не в JSON.

| Параметр | Источник | По умолчанию | Смысл |
|----------|----------|--------------|-------|
| `start_date` / `end_date` | `DEFAULT_SRC_START_DATE` / `END` | `2024-12-25` … `2025-02-05` | Период генерации |
| `operators` | `OPERATORS` | 4 оператора | Ключи: `билайн`, `мегафон`, … |
| `seed` | `DEFAULT_BS_SEED` | `20250407` | Детерминированность активности и шума |
| `max_workers` | `len(OPERATORS)` | 4 | **Один процесс на оператора** |
| `movement_ratio` | `DEFAULT_SRC_MOBILE_MOVEMENT_RATIO` | `0.22` | Доп. вероятность смены БС в течение дня |
| `region_subjects` | `()` | без фильтра | Опционально ограничить BS по колонке `subject` |

---

## Логика сборки

1. Загрузка BS и person pool: для каждого месяца периода — **последний** parquet с `_SUCCESS` в каталоге месяца ([`build_person_pool_by_operator_month_slices`](../../src/mobile/pipelines/src/mobile.py)).
2. Подготовка BS по операторам (`prepare_bs_by_operator`), локальный UTC-offset по полигонам TZ.
3. **Один OS-процесс на оператора**: дни периода последовательно; прогресс — `tqdm` на оператора.
4. Активность по профилю (p50 событий/день/id, heavy tails, часть событий без привязки к БС).
5. Перед записью — [`inject_cross_mart_rows`](../../src/mobile/pipelines/src/mobile.py).
6. Запись — один parquet на витрину/день/оператор.
7. Метрики — `append_command_metrics(command="build-src-mobile", ...)`.

Код: [`pipelines/src/mobile.py`](../../src/mobile/pipelines/src/mobile.py) — `run_mobile_all()`.

---

## Синтетические отклонения (Q&A / DQ)

| ID | Доля / правило | Реализация |
|----|----------------|------------|
| **OCC-018** `Service` | Веса по справочнику | `SERVICE_*_WEIGHTS`, `pick_weighted_service` |
| **OCC-003 / GEN-018** cross-mart | **~2.5%** перенос между витринами; **~2%** неверный `Event` | `inject_cross_mart_rows` |

---

## Ошибки

| Исключение | Когда |
|------------|-------|
| `FileNotFoundError` | Нет `bs.parquet` или person parquet за месяц |
| `ValueError` | Нет `person_config_path`; пустой период; нет колонки `subject` при `region_subjects` |
| pandas / pyarrow | Запись parquet |
