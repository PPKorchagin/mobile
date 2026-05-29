# build-stg-msisdn-imei

**Витрина:** `stg_msisdn_imei` · **Команда:** `build-stg-msisdn-imei` · **Режим:** месячный parquet с **ежедневным** инкрементом из `stg_geo_all`.

Референс: [`msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py), [`binding_intervals.py`](../../src/mobile/pipelines/stg/binding_intervals.py). Схема: [`msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать `stg_geo_all` за отчётный день | События за сутки |
| 2 | Построить суточные интервалы MSISDN↔IMEI | Сегменты по смене устройства |
| 3 | Обновить месячный файл (снять вклад дня, merge) | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` |

**Бизнес-назначение:** месячная привязка номера к IMEI для person и geo-intervals.

Логика совпадает с [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md), колонка связи — `imei`.

---

## Параметры запуска

| Переменная | По умолчанию | Описание |
|------------|--------------|----------|
| `report_date` | — | Любой день месяца |
| `stg_geo_all_path` | `data/stg/geo_all/{YYYY-MM-DD}.parquet` | Вход за день |
| `output_path` | `data/stg/msisdn_imei/{YYYY-MM-01}.parquet` | Месячный выход |

```bash
uv run mobile build-stg-msisdn-imei --report-date 2025-01-15
```

---

## Структура витрины

Один Parquet на месяц: `msisdn`, `imei`, `valid_from`, `valid_to`. См. [`msisdn_imei.json`](../../src/mobile/schema/stg/msisdn_imei.json).

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`msisdn_imei.py`](../../src/mobile/pipelines/stg/msisdn_imei.py) |
| IMSI (аналог) | [`build_stg_msisdn_imsi.md`](./build_stg_msisdn_imsi.md) |
| Person | [`build_stg_person.md`](./build_stg_person.md) |
