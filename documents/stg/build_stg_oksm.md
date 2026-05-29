# build-stg-oksm

**Витрина:** `stg_oksm` · **Команда:** `build-stg-oksm` · **Режим:** полная перезапись одного Parquet-файла.

Референс: [`pipelines/stg/oksm.py`](../../src/mobile/pipelines/stg/oksm.py). Схема витрины: [`oksm.json`](../../src/mobile/schema/stg/oksm.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Загрузить сырой CSV ОКСМ | DataFrame источника |
| 2 | Нормализовать коды и наименования | DataFrame целевой схемы `stg_oksm` |
| 3 | Записать витрину в Parquet с заданным сжатием | Файл `output_path` |

**Бизнес-назначение:** справочник стран (ОКСМ) для атрибутов с цифровым/буквенным кодом страны.

**В scope задач:** чтение CSV, нормализация `numeric_code` (3 цифры), `alpha2`/`alpha3` (uppercase), проверка уникальности ключей, запись Parquet.

---

## Параметры запуска

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `csv_path` | string (path) | Да | `src/mobile/raw_data/oksm_v001.csv` | Входной CSV |
| `output_path` | string (path) | Да | `data/stg/oksm.parquet` | Выходной Parquet (перезапись) |
| `compression` | string | Да | `snappy` | Сжатие Parquet |

**Константы ETL** ([`oksm.py`](../../src/mobile/pipelines/stg/oksm.py)):

| Константа | Значение |
|-----------|----------|
| `STG_OKSM_TABLE` | `stg_oksm` |
| `CSV_SEP` | `;` |
| `CSV_ENCODING` | `utf-8-sig` |
| `SOURCE_MAPPING_COLUMNS` | колонки CSV (русские заголовки) → витрина |

```bash
uv run mobile build-stg-oksm
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

`read_csv(csv_path, sep=';', encoding='utf-8-sig')`.

### Шаг 2. Нормализация

1. Rename колонок по `SOURCE_MAPPING_COLUMNS`.
2. **numeric_code:** только цифры → `zfill(3)` → последние 3 символа; валидация `^\d{3}$`.
3. **alpha2 / alpha3:** `strip`, замена кириллических омоглифов на латиницу (напр. `АХ` → `AX`), `upper`; валидация `^[A-Z]{2}$` / `^[A-Z]{3}$` для непустых.
4. **name_short / name_full / autokey:** `strip`; пустые наименования → `ValueError`.
5. Уникальность `numeric_code` и `autokey`.

### Шаг 3. Запись

`to_parquet(output_path, compression=compression, index=False)`.

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| Схема | [`src/mobile/schema/stg/oksm.json`](../../src/mobile/schema/stg/oksm.json) |
| ETL | [`src/mobile/pipelines/stg/oksm.py`](../../src/mobile/pipelines/stg/oksm.py) |
| DQ | [`documents/dq/stg/dq_stg_oksm.md`](../dq/stg/dq_stg_oksm.md) |
