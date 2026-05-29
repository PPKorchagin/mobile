# dq-stg-oksm

**Витрина:** `stg_oksm` · **Команда:** `dq-stg-oksm` · **Режим:** read-only проверки Parquet (процесс не падает при failed checks).

Референс: [`pipelines/dq/stg/oksm.py`](../../../src/mobile/pipelines/dq/stg/oksm.py). Контракт: [`oksm.json`](../../../src/mobile/schema/stg/oksm.json).

---

## Задачи pipeline

| # | Задача | Результат |
|---|--------|-----------|
| 1 | Прочитать parquet по пути CLI | DataFrame витрины |
| 2 | Проверить коды, имена, ключи | Логи `DQ_STG_OKSM` |
| 3 | Итог `summary` | Счётчики checks |

**Бизнес-назначение:** контроль качества справочника ОКСМ после `build-stg-oksm`.

---

## Параметры запуска

| Переменная | Тип | Обязательность | Значение по умолчанию | Описание |
|------------|-----|----------------|----------------------|----------|
| `parquet_path` | string (path) | Да | `data/stg/oksm.parquet` | `DEFAULT_STG_OKSM_OUTPUT_PATH` |

**Предусловие:** `uv run mobile build-stg-oksm`.

```bash
uv run mobile dq-stg-oksm
```

---

## Проверки

| Check | Уровень | Смысл |
|-------|---------|-------|
| `dataset_presence` | failed | Файл parquet существует |
| `dataset_basic` | ok | Число строк/колонок |
| `schema_columns` | failed | Все поля `STG_OKSM_FIELDS` |
| `nulls.*` | ok | Доля null по полю |
| `cardinality.*` | ok | Число уникальных значений |
| `numeric_code_integrity` | failed | Формат `^\d{3}$`, без дублей |
| `russia_presence` | warning | Есть запись с кодом `643` |
| `alpha2_integrity` | failed | Формат и уникальность alpha-2 |
| `alpha3_integrity` | failed | Формат и уникальность alpha-3 |
| `alpha_pair_cardinality` | ok | Число пар alpha2+alpha3 |
| `name_quality` | failed | Непустые `name_short` / `name_full` |
| `autokey_integrity` | failed | Уникальный непустой `autokey` |
| `summary` | ok | Итог warning/failed |

---

## Ссылки

| Артефакт | Путь |
|----------|------|
| ETL | [`src/mobile/pipelines/stg/oksm.py`](../../../src/mobile/pipelines/stg/oksm.py) |
| DQ | [`src/mobile/pipelines/dq/stg/oksm.py`](../../../src/mobile/pipelines/dq/stg/oksm.py) |
| Build doc | [`documents/stg/build_stg_oksm.md`](../../stg/build_stg_oksm.md) |
