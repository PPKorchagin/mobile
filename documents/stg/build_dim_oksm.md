# build-dim-oksm

**Витрина:** `dim_oksm` · **Команда:** `build-dim-oksm` · **Режим:** полная перезапись одного Parquet-файла.

Референс: [`pipelines/stg/oksm.py`](../../src/mobile/pipelines/stg/oksm.py). Схема витрины: [`oksm.json`](../../src/mobile/schema/dim/oksm.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Загрузить сырой CSV ОКСМ | DataFrame источника |
| 2 | Нормализовать коды и наименования | DataFrame целевой схемы `dim_oksm` |
| 3 | Записать витрину в Parquet с заданным сжатием | Файл `output_path` |

**Бизнес-назначение:** справочник стран (ОКСМ) для атрибутов с цифровым/буквенным кодом страны.

**В scope задач:** чтение CSV, нормализация `numeric_code` (3 цифры), `alpha2`/`alpha3` (uppercase), проверка уникальности ключей, запись Parquet.

---

## TODO

1. Проверить корректность кодов и наименований на соответствие официальному классификатору ОКСМ / ISO 3166.
2. Автоматизировать получение данных от внешнего поставщика с сохранением историчности и обновлением справочника.

---

## Параметры запуска

| Переменная    | Тип           | Обязательность | Значение по умолчанию                | Описание                               |
| ------------- | ------------- | -------------- | ------------------------------------ | -------------------------------------- |
| `csv_path`    | string (path) | Да             | `src/mobile/raw_data/oksm_v001.csv`  | Входной CSV (CLI `--csv-path`)         |
| `output_path` | string (path) | Да             | `data/dim/oksm.parquet`              | Выходной Parquet (CLI `--output-path`) |

Сжатие Parquet — константа `DEFAULT_PARQUET_COMPRESSION` в [`cli_defaults.py`](../../src/mobile/cli_defaults.py) (по умолчанию `snappy`); в job **не передаётся**.

**Константы ETL** ([`oksm.py`](../../src/mobile/pipelines/stg/oksm.py)):

| Константа | Значение |
|-----------|----------|
| `DIM_OKSM_TABLE` | `dim_oksm` |
| `CSV_SEP` | `;` |
| `CSV_ENCODING` | `utf-8-sig` |
| `SOURCE_MAPPING_COLUMNS` | колонки CSV (русские заголовки) → витрина |

```bash
uv run mobile build-dim-oksm
uv run mobile build-dim-oksm --csv-path src/mobile/raw_data/oksm_v001.csv --output-path data/dim/oksm.parquet
```

---

## Структура генерируемой витрины

| # | Поле | Тип | Смысл |
|---|------|-----|-------|
| 1 | `numeric_code` | string | Цифровой код страны, 3 цифры |
| 2 | `name_short` | string | Краткое наименование |
| 3 | `name_full` | string | Полное наименование |
| 4 | `alpha2` | string | ISO alpha-2 |
| 5 | `alpha3` | string | ISO alpha-3 |
| 6 | `autokey` | string | Ключ записи в источнике |

### Ожидаемый объём (эталон `oksm_v001.csv`)

~**253** строк, **6** колонок.

---

## Источники витрины

| Атрибут | Значение |
|---------|----------|
| Путь | `src/mobile/raw_data/oksm_v001.csv` |
| Формат | CSV: `;`, UTF-8 с BOM, заголовок в первой строке |

**Обязательные колонки CSV:** `Цифровой код`, `Наименование краткое`, `Наименование полное`, `Код альфа-2`, `Код альфа-3`, `autokey`.

---

## Алгоритм обработки данных

### Шаг 1. Чтение

`read_csv(csv_path, sep=';', encoding='utf-8-sig', keep_default_na=False, na_values=[""])` — иначе alpha2 `NA` (Намибия) читается pandas как null.

### Шаг 2. Нормализация

1. Rename колонок по `SOURCE_MAPPING_COLUMNS`.
2. **numeric_code:** только цифры → `zfill(3)` → последние 3 символа; валидация `^\d{3}$`.
3. **alpha2 / alpha3:** `strip`, замена кириллических омоглифов на латиницу (напр. `АХ` → `AX`), `upper`; валидация `^[A-Z]{2}$` / `^[A-Z]{3}$` для непустых.
4. **name_short / name_full / autokey:** `strip`; пустые наименования → `ValueError`.
5. Уникальность `numeric_code` и `autokey`.

### Шаг 3. Запись

`to_parquet(output_path, compression=DEFAULT_PARQUET_COMPRESSION, index=False)`.

---

## Типовые ошибки

| Ошибка | Причина |
|--------|---------|
| `FileNotFoundError` | Нет CSV |
| `ValueError` | Нет колонки CSV, невалидный `numeric_code` / `alpha2` / `alpha3`, пустые наименования, дубликат `numeric_code` или `autokey` |
| pandas / pyarrow | Битый CSV, сбой записи |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`src/mobile/schema/dim/oksm.json`](../../src/mobile/schema/dim/oksm.json) |
| ETL / lookup | [`src/mobile/pipelines/stg/oksm.py`](../../src/mobile/pipelines/stg/oksm.py) |
| DQ | [`documents/dq/stg/dq_dim_oksm.md`](../dq/stg/dq_dim_oksm.md) |
| Потребитель | [`build_stg_person.md`](./build_stg_person.md) |
