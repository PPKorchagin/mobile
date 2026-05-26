from __future__ import annotations

import json
import logging
import random
import re
import shutil
import time
from calendar import monthrange
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from tqdm import tqdm

from mobile.cli_defaults import OPERATORS, stable_seed
from mobile.command_timing import append_command_metrics, timed_stage
from mobile.pipelines.src.schema_fields import SRC_PERSON_FIELDS
from mobile.project_paths import PROJECT_ROOT, SRC_PERSON_LAYOUT_TEMPLATE, SRC_PERSON_SUCCESS_FLAG


logger = logging.getLogger(__name__)

SRC_PERSON_TABLE = "person"
ACTUALLY_TO_OPEN = pd.Timestamp("2261-12-31 23:59:59")
DEFAULT_EXTRA_FULL_SNAPSHOT_RANDOM_DAYS = 7
PERSON_CHUNK_SIZE = 250_000
OPERATOR_PROCESS_WORKERS = 4
DAY_PARALLELISM_CAP = 3

_OPERATOR_WORKER_FAKER_POOL: dict[str, list[str]] | None = None

# Доли «грязных» строк под DQ/Q&A (PER-002, PER-007, PER-008, GEN-012, PER-015).
SCD2_OVERLAP_ROW_FRACTION = 0.04
INVALID_ISDN_PROBABILITY = 0.02
IDENTITY_FIELD_LEAK_PROBABILITY = 0.03
SCD2_CONCURRENT_OPEN_PROBABILITY = 0.35


@dataclass(frozen=True)
class BuildSrcPersonParams:
    start_date: date
    end_date: date
    operators: list[str]
    target_active_subscribers_per_operator: int
    daily_active_ratio_min: float
    daily_active_ratio_max: float
    closed_contract_ratio: float
    inactive_ratio: float
    corporate_ratio: float
    inter_operator_transition_ratio: float
    movement_ratio: float
    foreign_subscriber_ratio: float
    extra_random_full_snapshot_days: int
    seed: int
    max_workers: int

    def __post_init__(self) -> None:
        if int(self.extra_random_full_snapshot_days) < 0:
            raise ValueError("extra_random_full_snapshot_days must be >= 0")


def run(
    *,
    output_layout: str = SRC_PERSON_LAYOUT_TEMPLATE,
    compression: str,
    success_flag: str = SRC_PERSON_SUCCESS_FLAG,
    params: BuildSrcPersonParams,
) -> dict[str, Any]:
    perf_metrics: dict[str, Any] = {}
    fields = SRC_PERSON_FIELDS
    out_template = output_layout
    fake = Faker("ru_RU")
    fake.seed_instance(stable_seed("faker_pool", params.seed))
    faker_pool = _build_faker_pool(fake, random.Random(stable_seed("faker_pool_py", params.seed)))

    tasks: list[date] = []
    current = params.start_date
    while current <= params.end_date:
        tasks.append(current)
        current += timedelta(days=1)
    if not tasks:
        raise ValueError("No generation tasks for src_person")

    full_snapshot_days = select_full_snapshot_days(
        tasks,
        extra_random_day_count=params.extra_random_full_snapshot_days,
        seed=params.seed,
    )
    target = int(params.target_active_subscribers_per_operator)
    month_total = len(params.operators) * target
    logger.info(
        "Starting build-src-person: days=%s, full_snapshot_days=%s (_SUCCESS), operators=%s, "
        "monthly_active_pool_per_operator=%s (~%s total)",
        len(tasks),
        len(full_snapshot_days),
        len(params.operators),
        target,
        month_total,
    )
    logger.info(
        "build-src-person _SUCCESS calendar days: %s",
        ", ".join(sorted(d.isoformat() for d in full_snapshot_days)),
    )

    started_at = time.perf_counter()
    generated_rows = 0
    full_days = 0
    # Дни — потоки; внутри дня операторы — отдельные процессы (см. OPERATOR_PROCESS_WORKERS).
    day_parallelism = max(
        1,
        min(
            len(tasks),
            DAY_PARALLELISM_CAP,
            max(1, int(params.max_workers) // OPERATOR_PROCESS_WORKERS),
        ),
    )

    def _run_task(day: date) -> dict[str, Any]:
        return _generate_and_write_day(
            fields=fields,
            day=day,
            out_template=out_template,
            compression=compression,
            success_flag=success_flag,
            params=params,
            faker_pool=faker_pool,
            full_snapshot_days=full_snapshot_days,
        )

    with timed_stage("execution_sec", perf_metrics):
        with ThreadPoolExecutor(max_workers=day_parallelism) as executor:
            futures = [executor.submit(_run_task, day) for day in tasks]
            with tqdm(
                total=len(tasks),
                desc="build-src-person",
                unit="day",
                dynamic_ncols=True,
                smoothing=0.1,
            ) as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    generated_rows += int(result["row_count"])
                    full_days += int(result["is_full_snapshot"])
                    pbar.update(1)
                    elapsed_now = max(0.001, time.perf_counter() - started_at)
                    rows_per_sec = int(generated_rows / elapsed_now)
                    pbar.set_postfix(
                        day=result["day"],
                        day_rows=result["row_count"],
                        full_days=full_days,
                        total_rows=generated_rows,
                        rows_s=rows_per_sec,
                        refresh=False,
                    )
                    logger.info(
                        "build-src-person progress: %s/%s | day=%s | rows=%s | full=%s | rows_per_sec=%s",
                        pbar.n,
                        len(tasks),
                        result["day"],
                        result["row_count"],
                        result["is_full_snapshot"],
                        rows_per_sec,
                    )

    elapsed = round(time.perf_counter() - started_at, 2)
    logger.info(
        "build-src-person completed: rows=%s, files=%s, full_days=%s, elapsed_sec=%s",
        generated_rows,
        len(tasks),
        full_days,
        elapsed,
    )
    perf_metrics["elapsed_total_sec"] = elapsed
    perf_metrics["rows"] = int(generated_rows)
    perf_metrics["files"] = int(len(tasks))
    perf_metrics["day_workers"] = int(day_parallelism)
    perf_metrics["operator_process_workers"] = int(
        min(OPERATOR_PROCESS_WORKERS, len(params.operators)),
    )
    perf_metrics["full_days"] = int(full_days)
    perf_metrics["full_snapshot_days_expected"] = int(len(full_snapshot_days))
    append_command_metrics(command="build-src-person", metrics=perf_metrics)
    return {
        "row_count": int(generated_rows),
        "file_count": int(len(tasks)),
        "full_days": int(full_days),
        "full_snapshot_days_expected": int(len(full_snapshot_days)),
        "elapsed_sec": elapsed,
        "max_workers": int(day_parallelism),
        "operator_process_workers": int(min(OPERATOR_PROCESS_WORKERS, len(params.operators))),
    }


def _month_end_snapshot_days(start: date, end: date) -> set[date]:
    selected: set[date] = set()
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        last_dom = monthrange(cursor.year, cursor.month)[1]
        month_last = date(cursor.year, cursor.month, last_dom)
        snapshot_day = min(month_last, end)
        if snapshot_day >= start:
            selected.add(snapshot_day)
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return selected


def select_full_snapshot_days(
    tasks: list[date],
    *,
    extra_random_day_count: int = DEFAULT_EXTRA_FULL_SNAPSHOT_RANDOM_DAYS,
    seed: int = 0,
) -> frozenset[date]:
    """Концы месяцев в периоде + ``extra_random_day_count`` случайных дней (полный срез + ``_SUCCESS``)."""
    if not tasks:
        return frozenset()
    start = min(tasks)
    end = max(tasks)
    task_set = set(tasks)
    selected = _month_end_snapshot_days(start, end) & task_set

    pool = sorted(task_set - selected)
    n_extra = max(0, min(int(extra_random_day_count), len(pool)))
    if n_extra > 0:
        rng = np.random.default_rng(
            stable_seed("full_snapshot_random", seed, start.isoformat(), end.isoformat(), n_extra),
        )
        for picked in rng.choice(pool, size=n_extra, replace=False):
            selected.add(picked)

    return frozenset(selected)


def _generate_and_write_day(
    *,
    fields: list[dict[str, Any]],
    day: date,
    out_template: str,
    compression: str,
    success_flag: str,
    params: BuildSrcPersonParams,
    faker_pool: dict[str, list[str]],
    full_snapshot_days: frozenset[date],
) -> dict[str, Any]:
    output_path = _resolve_output_path(out_template, day)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    field_order = [f["name"] for f in fields]
    arrow_schema = _build_arrow_schema(fields)

    is_full_snapshot = day in full_snapshot_days
    if is_full_snapshot:
        daily_ratio = 1.0
    else:
        rng = np.random.default_rng(stable_seed("src_person_daily_ratio", day.isoformat(), params.seed))
        lo = max(0.05, min(1.0, float(params.daily_active_ratio_min)))
        hi = max(lo, min(1.0, float(params.daily_active_ratio_max)))
        daily_ratio = float(rng.uniform(lo, hi))
    target_count = int(params.target_active_subscribers_per_operator)
    per_operator_count = target_count if is_full_snapshot else max(1, int(target_count * daily_ratio))

    operator_workers = min(OPERATOR_PROCESS_WORKERS, len(params.operators))
    use_operator_processes = operator_workers > 1
    chunk_size = PERSON_CHUNK_SIZE
    total_rows = 0

    if not use_operator_processes:
        with pq.ParquetWriter(output_path, schema=arrow_schema, compression=compression) as writer:
            for operator in params.operators:
                operator_rows = 0
                for offset in range(0, per_operator_count, chunk_size):
                    current_chunk_size = min(chunk_size, per_operator_count - offset)
                    data = _generate_operator_slice(
                        day=day,
                        serving_operator=operator,
                        params=params,
                        faker_pool=faker_pool,
                        local_id_offset=offset,
                        count=current_chunk_size,
                    )
                    data = _align_columns_for_schema(data, field_order)
                    table = pa.Table.from_pandas(data, schema=arrow_schema, preserve_index=False, safe=False)
                    writer.write_table(table)
                    chunk_rows = int(len(data))
                    total_rows += chunk_rows
                    operator_rows += chunk_rows
                logger.info(
                    "build-src-person day=%s operator=%s rows=%s full=%s",
                    day.isoformat(),
                    operator,
                    operator_rows,
                    is_full_snapshot,
                )
    else:
        tmp_dir = output_path.parent / f".tmp_person_{day.strftime('%Y%m%d')}_{int(time.time() * 1000)}"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_files: list[Path] = []
        try:
            with ProcessPoolExecutor(
                max_workers=operator_workers,
                initializer=_init_operator_worker,
                initargs=(int(params.seed),),
            ) as executor:
                futures = []
                for idx, operator in enumerate(params.operators):
                    tmp_file = tmp_dir / f"operator_{idx}.parquet"
                    tmp_files.append(tmp_file)
                    futures.append(
                        executor.submit(
                            _write_operator_temp_file,
                            tmp_file=tmp_file,
                            fields=fields,
                            day=day,
                            operator=operator,
                            params=params,
                            field_order=field_order,
                            arrow_schema=arrow_schema,
                            compression=compression,
                            chunk_size=chunk_size,
                            per_operator_count=per_operator_count,
                        )
                    )
                for future in as_completed(futures):
                    total_rows += int(future.result())

            with pq.ParquetWriter(output_path, schema=arrow_schema, compression=compression) as writer:
                for tmp_file in tmp_files:
                    parquet = pq.ParquetFile(tmp_file)
                    for batch in parquet.iter_batches(batch_size=chunk_size):
                        writer.write_batch(batch)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    success_path = output_path.parent / success_flag
    if is_full_snapshot:
        success_path.write_text("", encoding="utf-8")
    elif success_path.exists():
        success_path.unlink()
    return {
        "day": day.isoformat(),
        "row_count": int(total_rows),
        "output_path": str(output_path),
        "is_full_snapshot": bool(is_full_snapshot),
    }


def _init_operator_worker(seed: int) -> None:
    """Инициализация пула Faker в дочернем процессе (детерминированно по ``seed``)."""
    global _OPERATOR_WORKER_FAKER_POOL
    fake = Faker("ru_RU")
    fake.seed_instance(stable_seed("faker_pool", seed))
    _OPERATOR_WORKER_FAKER_POOL = _build_faker_pool(
        fake,
        random.Random(stable_seed("faker_pool_py", seed)),
    )


def _resolve_faker_pool(faker_pool: dict[str, list[str]] | None) -> dict[str, list[str]]:
    if faker_pool is not None:
        return faker_pool
    if _OPERATOR_WORKER_FAKER_POOL is None:
        raise RuntimeError("operator worker faker pool is not initialized")
    return _OPERATOR_WORKER_FAKER_POOL


def _write_operator_temp_file(
    *,
    tmp_file: Path,
    fields: list[dict[str, Any]],
    day: date,
    operator: str,
    params: BuildSrcPersonParams,
    field_order: list[str],
    arrow_schema: pa.Schema,
    compression: str,
    chunk_size: int,
    per_operator_count: int,
    faker_pool: dict[str, list[str]] | None = None,
) -> int:
    pool = _resolve_faker_pool(faker_pool)
    rows = 0
    with pq.ParquetWriter(tmp_file, schema=arrow_schema, compression=compression) as writer:
        for offset in range(0, per_operator_count, chunk_size):
            current_chunk_size = min(chunk_size, per_operator_count - offset)
            data = _generate_operator_slice(
                day=day,
                serving_operator=operator,
                params=params,
                faker_pool=pool,
                local_id_offset=offset,
                count=current_chunk_size,
            )
            data = _align_columns_for_schema(data, field_order)
            table = pa.Table.from_pandas(data, schema=arrow_schema, preserve_index=False, safe=False)
            writer.write_table(table)
            rows += int(len(data))
    return rows


def _generate_operator_slice(
    *,
    day: date,
    serving_operator: str,
    params: BuildSrcPersonParams,
    faker_pool: dict[str, list[str]],
    local_id_offset: int,
    count: int,
) -> pd.DataFrame:
    sid = np.arange(local_id_offset, local_id_offset + count, dtype=np.int64)
    rng = np.random.default_rng(stable_seed("src_person", serving_operator, day.isoformat(), params.seed, local_id_offset, count))

    closed_ratio = max(0.0, min(0.95, float(params.closed_contract_ratio)))
    inactive_ratio = max(0.0, min(0.95, float(params.inactive_ratio)))
    corporate_ratio = max(0.0, min(0.5, float(params.corporate_ratio)))
    churn_ratio = max(0.0, min(1.0, float(params.movement_ratio)))
    transition_ratio = max(0.0, min(0.5, float(params.inter_operator_transition_ratio)))

    client_type = rng.choice([0, 1], size=count, p=[1.0 - corporate_ratio, corporate_ratio])
    home_operator = np.full(count, serving_operator, dtype=object)
    churn_mask = rng.random(count) < churn_ratio
    if np.any(churn_mask):
        home_operator[churn_mask] = _neighbor_operator_vectorized(serving_operator, sid[churn_mask])
    transition_mask = rng.random(count) < transition_ratio
    home_for_ids = home_operator.copy()
    if np.any(transition_mask):
        transitioned = [_neighbor_operator(str(home_operator[idx]), int(sid[idx]) + 1) for idx in np.where(transition_mask)[0]]
        home_for_ids[transition_mask] = np.array(transitioned, dtype=object)

    ids = [_identity_triplet(str(home_for_ids[i]), int(sid[i])) for i in range(count)]
    msisdn_digits = [v[0] for v in ids]
    imsi_digits = [v[1] for v in ids]
    imei_digits = [v[2] for v in ids]
    invalid_isdn = rng.random(count) < INVALID_ISDN_PROBABILITY
    for idx in np.where(invalid_isdn)[0]:
        msisdn_digits[int(idx)] = _sample_invalid_isdn_digits(rng)
    msisdn_int = _to_int_series(msisdn_digits)
    identity_leak = rng.random(count) < IDENTITY_FIELD_LEAK_PROBABILITY
    imsi_int = _to_int_series(imsi_digits)
    imei_int = _to_int_series(imei_digits)
    operator_id = np.full(count, OPERATORS[serving_operator], dtype=np.int64)
    pool = faker_pool
    pool_choice = lambda key: rng.choice(pool[key], size=count)  # noqa: E731

    contract_date = pd.Series(pd.to_datetime(pool_choice("contract_date"), errors="coerce")).dt.normalize()
    birth_day = pd.Series(pd.to_datetime(pool_choice("birth_date"), errors="coerce")).dt.normalize()
    closed_contract = rng.random(count) < closed_ratio
    inactive_flag = rng.random(count) < inactive_ratio
    active_now = ~(closed_contract | inactive_flag)

    is_individual = client_type == 0
    citizenship_codes = _assign_citizenship_codes(
        rng,
        count=count,
        is_individual=is_individual,
        foreign_ratio=float(params.foreign_subscriber_ratio),
    )
    ru_first = np.where(is_individual, pool_choice("first_name"), "")
    ru_middle = np.where(is_individual, pool_choice("middle_name"), "")
    ru_last = np.where(is_individual, pool_choice("last_name"), "")
    first_name, second_name, last_name = _fill_person_names_for_citizenship(
        rng,
        citizenship=citizenship_codes,
        is_individual=is_individual,
        ru_first=ru_first,
        ru_middle=ru_middle,
        ru_last=ru_last,
    )
    first_name = _normalize_person_name_array(first_name)
    second_name = _normalize_person_name_array(second_name)
    last_name = _normalize_person_name_array(last_name)
    abonent = np.where(
        is_individual,
        np.char.add(
            np.char.add(np.char.add(last_name.astype(str), " "), first_name.astype(str)),
            np.where(second_name.astype(str) != "", " " + second_name.astype(str), ""),
        ),
        "",
    )
    org_name = np.where(client_type == 1, pool_choice("company"), "")
    inn = np.where(
        client_type == 1,
        pd.Series(pd.to_numeric(pool_choice("business_inn"), errors="coerce"), dtype="Int64").to_numpy(),
        pd.NA,
    )
    contact = np.where(client_type == 1, pool_choice("fio_short"), "")
    identity_type = rng.choice([2, 4, 5, 3, 1], size=count, p=[0.74, 0.12, 0.06, 0.05, 0.03])
    is_gsm = identity_type == 2
    is_data = identity_type == 4
    is_voip = identity_type == 5
    ip_type = rng.choice([0, 1], size=count, p=[0.8, 0.2])
    ip_type_voip = rng.choice([0, 1], size=count, p=[0.72, 0.28])
    iccid_values = _iccid_array(rng, count)
    imsi_values = np.where(is_gsm | (identity_leak & np.isin(identity_type, [1, 4, 5])), imsi_int, pd.NA)
    imei_values = np.where(is_gsm | (identity_leak & np.isin(identity_type, [4, 5])), imei_int, pd.NA)
    iccid_out = np.where(is_gsm | (identity_leak & (identity_type == 5)), iccid_values, "")
    login_out = np.where(is_data | (identity_leak & is_gsm), pool_choice("login"), "")
    mac_out = np.where(is_data | (identity_leak & is_gsm), pool_choice("mac"), "")
    voip_calling_out = np.where(
        is_voip | (identity_leak & np.isin(identity_type, [2, 3])),
        msisdn_int,
        pd.NA,
    )

    main_service_start_ts = contract_date + pd.to_timedelta(rng.integers(0, 90, size=count), unit="D")
    closed_days_ago = pd.to_timedelta(rng.integers(1, 180, size=count), unit="D")
    main_service_end_ts = np.where(closed_contract, pd.Timestamp(day) - closed_days_ago, pd.NaT)
    end_contract_date = np.where(
        closed_contract,
        pd.Series(pd.Timestamp(day) - pd.to_timedelta(rng.integers(0, 60, size=count), unit="D")).dt.normalize(),
        pd.NaT,
    )

    abonent_last_location = rng.choice([0, 1, 2, 3], size=count, p=[0.52, 0.18, 0.16, 0.14])
    is_mobile_loc = abonent_last_location == 0
    lac_values = rng.integers(1000, 65000, size=count)
    cell_values = rng.integers(10000, 900000, size=count)

    data = pd.DataFrame(
        {
            "identity_type": identity_type,
            "operator_Id": operator_id,
            "network_pager_id": np.where(identity_type == 0, _pager_id_array(rng, count), ""),
            "isdn_world_pstn": np.where(identity_type == 1, msisdn_int, pd.NA),
            "additional_isdn_pstn": np.where(identity_type == 1, rng.integers(1000, 9999, size=count), pd.NA),
            "isdn": msisdn_int,
            "imsi": imsi_values,
            "imei": imei_values,
            "iccid": iccid_out,
            "isdn_cdma": np.where(identity_type == 3, msisdn_int, pd.NA),
            "cdma_imsi_a": np.where(identity_type == 3, imsi_int, pd.NA),
            "cdma_Imei_a": np.where(identity_type == 3, imei_int, pd.NA),
            "cdma_imsi_b": np.where(identity_type == 3, imsi_int, pd.NA),
            "icc_cdma": np.where(identity_type == 3, rng.integers(10**15, 10**16, size=count), pd.NA),
            "hardware_type": np.where(is_data, rng.choice([0, 1], size=count), pd.NA),
            "mac": mac_out,
            "atm_vpi_network": np.where(is_data, rng.integers(0, 255, size=count).astype(str), ""),
            "atm_vci_network": np.where(is_data, rng.integers(32, 65535, size=count).astype(str), ""),
            "login": login_out,
            "ip_type": np.where(is_data, ip_type, pd.NA),
            "ip4": np.where(is_data, np.where(ip_type == 0, pool_choice("ipv4"), ""), ""),
            "ip6": np.where(is_data, np.where(ip_type == 1, pool_choice("ipv6"), ""), ""),
            "email": np.where(is_data, pool_choice("email"), ""),
            "pin": np.where(is_data, rng.integers(1000, 9999, size=count).astype(str), ""),
            "isdn_date_network": np.where(is_data, msisdn_int, pd.NA),
            "user_domain": np.where(is_data, pool_choice("domain"), ""),
            "reserved": "",
            "ip_mask_type": np.where(is_data, ip_type, pd.NA),
            "ip4_mask": np.where(
                is_data,
                np.where(ip_type == 0, rng.choice(["255.255.255.0", "255.255.0.0"], size=count), ""),
                "",
            ),
            "ip6_mask": np.where(
                is_data,
                np.where(ip_type == 1, rng.choice(["ffff:ffff:ffff:ffff::", "ffff:ffff:ffff::"], size=count), ""),
                "",
            ),
            "ip_type_voip": np.where(is_voip, ip_type_voip, pd.NA),
            "ip4_voip": np.where(is_voip, np.where(ip_type_voip == 0, pool_choice("ipv4"), ""), ""),
            "ip6_voip": np.where(is_voip, np.where(ip_type_voip == 1, pool_choice("ipv6"), ""), ""),
            "voip_starter_name": np.where(is_voip, pool_choice("fio_short"), ""),
            "voip_calling_isdn": voip_calling_out,
            "contract_date": contract_date,
            "contract_number": _contract_number_array(serving_operator, sid),
            "actually_from": pd.Timestamp(day),
            "actually_to": _build_actually_to(day, active_now),
            "client_type": client_type,
            "struct_fio_type": np.where(client_type == 0, rng.choice([0, 1], size=count, p=[0.82, 0.18]), pd.NA),
            "first_name": first_name,
            "second_name": second_name,
            "last_name": last_name,
            "abonent": abonent,
            "birth_day": np.where(client_type == 0, birth_day, pd.NaT),
            "struct_dul_type": np.where(client_type == 0, rng.choice([0, 1], size=count, p=[0.84, 0.16]), pd.NA),
            "dul_serial": np.where(client_type == 0, pool_choice("passport_serial"), ""),
            "doc_number": np.where(client_type == 0, pool_choice("passport_number"), ""),
            "document": _fill_document_for_citizenship(
                rng,
                citizenship=citizenship_codes,
                is_individual=is_individual,
                ru_documents=np.where(is_individual, pool_choice("document"), ""),
            ),
            "dul_department": _fill_department_for_citizenship(
                rng,
                citizenship=citizenship_codes,
                is_individual=is_individual,
                ru_departments=np.where(is_individual, pool_choice("department"), ""),
            ),
            "unstruct_dul": np.where(client_type == 0, pool_choice("passport_full"), ""),
            "doc_type": np.where(client_type == 0, rng.choice([1, 2, 3], size=count, p=[0.86, 0.1, 0.04]), pd.NA),
            "client_bank": np.where(client_type == 0, pool_choice("bank"), ""),
            "client_bank_account": np.where(client_type == 0, pool_choice("bank_account"), ""),
            "org_name": org_name,
            "inn": inn,
            "contact": contact,
            "contact_info": np.where(client_type == 1, pool_choice("phone"), ""),
            "inner_users_list": np.where(client_type == 1, _inner_users_array(rng, pool, count), ""),
            "org_bank": np.where(client_type == 1, pool_choice("bank"), ""),
            "org_bank_account": np.where(client_type == 1, pool_choice("bank_account"), ""),
            "abonent_status": np.where(active_now, 0, 1),
            "main_service_start_ts": main_service_start_ts,
            "main_service_end_ts": main_service_end_ts,
            "abonent_last_location": abonent_last_location,
            "lac": np.where(is_mobile_loc, lac_values, pd.NA),
            "cell": np.where(is_mobile_loc, cell_values, pd.NA),
            "timing_advance": rng.integers(0, 64, size=count).astype(str),
            "network_location_id": _network_location_id_array(rng, count),
            "network_mac": pool_choice("mac"),
            "latitude": pd.to_numeric(pool_choice("latitude"), errors="coerce"),
            "longitude": pd.to_numeric(pool_choice("longitude"), errors="coerce"),
            "coordinates": "",
            "geo_json": "",
            "geo_projection_type": rng.choice([0, 1, 2], size=count, p=[0.93, 0.05, 0.02]),
            "ip_location_type": rng.choice(["0", "1"], size=count, p=[0.78, 0.22]),
            "ip4_location": pool_choice("ipv4"),
            "ip6_location": pool_choice("ipv6"),
            "ip_port_location": rng.integers(1024, 65535, size=count).astype(str),
            "object_conn_desc": pool_choice("object_desc"),
            "cross_desc": pool_choice("cross_desc"),
            "block_desc": pool_choice("block_desc"),
            "pair_desc": pool_choice("pair_desc"),
            "reserve": "",
            "network_conn_type": rng.choice([1, 2, 3, 4, 6, 7, 8, 10], size=count, p=[0.54, 0.08, 0.12, 0.08, 0.1, 0.04, 0.01, 0.03]),
            "address_registration": pool_choice("address"),
            "abonent_postal_address": pool_choice("address"),
            "address_invoice": pool_choice("address"),
            "address_installation": pool_choice("address"),
            "abonent_reserved_address": "",
            "service_list": _service_list_array(rng, count, day),
            "COL84": "",
            "COL85": "",
            "COL86": "",
            "last_change_date": pd.Timestamp(day),
            "end_contract_date": end_contract_date,
            "tariff": pool_choice("tariff"),
            "dealer": pool_choice("dealer"),
        }
    )

    end_ts = pd.to_datetime(data["main_service_end_ts"], errors="coerce")
    start_ts = pd.to_datetime(data["main_service_start_ts"], errors="coerce")
    invalid_end = end_ts.notna() & start_ts.notna() & (end_ts < start_ts)
    if invalid_end.any():
        data.loc[invalid_end, "main_service_end_ts"] = start_ts[invalid_end]

    data["coordinates"] = data["latitude"].round(6).astype(str) + "," + data["longitude"].round(6).astype(str)
    data["geo_json"] = (
        '{"type":"Point","coordinates":['
        + data["longitude"].round(6).astype(str)
        + ","
        + data["latitude"].round(6).astype(str)
        + "]}"
    )
    return _append_scd2_overlap_rows(data, day, rng)


def _resolve_output_path(template: str, day: date) -> Path:
    resolved = template.format(YYYY=day.strftime("%Y"), MM=day.strftime("%m"), DD=day.strftime("%d"))
    path = Path(resolved)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if path.suffix.lower() == ".parquet":
        return path
    return path / "person.parquet"


def _neighbor_operator(operator: str, sid: int) -> str:
    if operator not in OPERATORS:
        return operator
    names = list(OPERATORS)
    idx = names.index(operator)
    shift = 1 if sid % 2 == 0 else -1
    return names[(idx + shift) % len(names)]


def _neighbor_operator_vectorized(operator: str, sid: np.ndarray) -> np.ndarray:
    if operator not in OPERATORS:
        return np.full(len(sid), operator, dtype=object)
    names = list(OPERATORS)
    idx = names.index(operator)
    plus = names[(idx + 1) % len(names)]
    minus = names[(idx - 1) % len(names)]
    return np.where((sid % 2) == 0, plus, minus).astype(object)


def _msisdn_digits(operator: str, sid: int) -> str:
    names = list(OPERATORS)
    op_code = names.index(operator) + 1 if operator in OPERATORS else 9
    base = 10_000_000 + sid
    return f"79{op_code}{base:08d}"[:11]


def _imsi_digits(operator: str, sid: int) -> str:
    mnc = OPERATORS.get(operator, 99)
    return f"250{mnc:02d}{(1_000_000_000 + sid) % 10_000_000_000:010d}"


def _imei_digits(operator: str, sid: int) -> str:
    op = OPERATORS.get(operator, 99)
    return f"{35_000_000_000_000 + op * 1_000_000_000 + sid:015d}"[:15]


def _identity_triplet(operator: str, sid: int) -> tuple[str, str, str]:
    return (_msisdn_digits(operator, sid), _imsi_digits(operator, sid), _imei_digits(operator, sid))


def _to_int_series(values: list[str]) -> pd.Series:
    return pd.to_numeric(pd.Series(values, dtype="string"), errors="coerce").astype("Int64")


def _build_actually_to(day: date, active_now: np.ndarray) -> pd.Series:
    closed_to = pd.Timestamp(day) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    values = pd.Series(closed_to, index=np.arange(len(active_now)))
    values.loc[active_now] = ACTUALLY_TO_OPEN
    return values


def _pager_id_array(rng: np.random.Generator, count: int) -> np.ndarray:
    return np.array([f"PG{rng.integers(10**10, 10**12)}" for _ in range(count)], dtype=object)


def _iccid_array(rng: np.random.Generator, count: int) -> np.ndarray:
    return np.array([f"89{int(rng.integers(0, 10**18)):018d}" for _ in range(count)], dtype=object)


def _contract_number_array(serving_operator: str, sid: np.ndarray) -> np.ndarray:
    prefix = serving_operator.upper()[:3]
    return np.array([f"{prefix}-{int(v):09d}" for v in sid], dtype=object)


def _inner_users_array(rng: np.random.Generator, pool: dict[str, list[str]], count: int) -> np.ndarray:
    a = rng.choice(pool["fio_short"], size=count)
    b = rng.choice(pool["fio_short"], size=count)
    return np.array([f"{x};{y}" for x, y in zip(a, b, strict=False)], dtype=object)


def _network_location_id_array(rng: np.random.Generator, count: int) -> np.ndarray:
    return np.array([f"NL-{rng.integers(1000, 9999)}-{rng.integers(10000, 99999)}" for _ in range(count)], dtype=object)


def _service_list_array(rng: np.random.Generator, count: int, day: date) -> np.ndarray:
    services = np.array(["1", "2", "3", "5", "11", "12", "20"], dtype=object)
    range_start = (day - timedelta(days=365)).strftime("%y%m%d000000")
    range_end = day.strftime("%y%m%d235959")
    base = f"{range_start}-{range_end}"
    first = rng.choice(services, size=count)
    second = rng.choice(services, size=count)
    third = rng.choice(services, size=count)
    n = rng.integers(1, 4, size=count)
    out = np.array([f"{s};{base}" for s in first], dtype=object)
    mask2 = n >= 2
    out[mask2] = out[mask2] + "\x11" + np.array([f"{s};{base}" for s in second[mask2]], dtype=object)
    mask3 = n == 3
    out[mask3] = out[mask3] + "\x11" + np.array([f"{s};{base}" for s in third[mask3]], dtype=object)
    return out


def _normalize_isdn_digits(value: str) -> str:
    """E.164 без «+» (GEN-012, PER-008): только цифры, префикс 7 для РФ."""
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) >= 11 and digits.startswith("7"):
        return digits[:11]
    return digits


def _normalize_phone(value: str) -> str:
    digits = _normalize_isdn_digits(value)
    if len(digits) == 11 and digits.startswith("7"):
        return digits
    return value.strip()


def _sample_invalid_isdn_digits(rng: np.random.Generator) -> str:
    return str(
        rng.choice(
            [
                "790012345",
                "89001234567",
                "12345",
                "",
                "0079012345678",
            ],
        ),
    )


def _append_scd2_overlap_rows(data: pd.DataFrame, day: date, rng: np.random.Generator) -> pd.DataFrame:
    """PER-002: дополнительные интервалы SCD2 с пересечением по (operator_Id, isdn)."""
    if data.empty or "isdn" not in data.columns or "operator_Id" not in data.columns:
        return data
    candidates = data.index[data["isdn"].notna()]
    if len(candidates) == 0:
        return data

    n_extra = max(1, int(len(data) * SCD2_OVERLAP_ROW_FRACTION))
    n_extra = min(n_extra, len(candidates))
    picked = rng.choice(candidates.to_numpy(), size=n_extra, replace=False)
    extra = data.loc[picked].copy()

    day_ts = pd.Timestamp(day)
    extra["actually_from"] = day_ts - pd.to_timedelta(rng.integers(45, 200, size=n_extra), unit="D")
    extra["actually_to"] = day_ts + pd.to_timedelta(rng.integers(30, 400, size=n_extra), unit="D")
    concurrent_open = rng.random(n_extra) < SCD2_CONCURRENT_OPEN_PROBABILITY
    extra.loc[concurrent_open, "actually_to"] = ACTUALLY_TO_OPEN

    return pd.concat([data, extra], ignore_index=True)


def _normalize_person_name(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    for dash in ("-", "–", "—"):
        text = text.replace(dash, " ")
    text = re.sub(r"[$&+=?@#|'<>^*()%!_№`~{}\[\]]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return " ".join(part[:1].upper() + part[1:].lower() if part else "" for part in text.split(" "))


def _normalize_person_name_array(values: np.ndarray) -> np.ndarray:
    return np.array([_normalize_person_name(v) for v in values], dtype=object)


def _format_ru_passport(serial: str, number: str) -> tuple[str, str, str]:
    serial_digits = "".join(ch for ch in str(serial) if ch.isdigit())
    number_digits = "".join(ch for ch in str(number) if ch.isdigit())
    if not serial_digits and len(number_digits) >= 10:
        serial_digits, number_digits = number_digits[:4], number_digits[4:10]
    if len(serial_digits) > 4:
        serial_digits = serial_digits[-4:]
    if len(number_digits) > 6:
        number_digits = number_digits[-6:]
    serial_fmt = f"{int(serial_digits):04d}" if serial_digits else "0000"
    number_fmt = f"{int(number_digits):06d}" if number_digits else "000000"
    full = f"{serial_fmt} {number_fmt}"
    return serial_fmt, number_fmt, full


def _parse_ru_passport(raw: str) -> tuple[str, str, str]:
    parts = [p for p in re.split(r"\s+", str(raw).strip()) if p]
    if len(parts) >= 3:
        return _format_ru_passport(f"{parts[0]} {parts[1]}", parts[2])
    if len(parts) == 2:
        return _format_ru_passport(parts[0], parts[1])
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 10:
        return _format_ru_passport(digits[:4], digits[4:10])
    return _format_ru_passport(raw, "")


def _sample_dul_bundle(fake: Faker, rng: random.Random) -> tuple[str, str, str]:
    """PER-015: паспорт РФ и прочие ДУЛ на базе Faker + шаблонов из Q&A."""
    kind = rng.choices(
        ["ru", "ru", "birth", "foreign", "vnzh", "seaman"],
        weights=[58, 12, 8, 10, 7, 5],
        k=1,
    )[0]
    if kind == "ru":
        return _parse_ru_passport(fake.passport_number())
    if kind == "birth":
        series = rng.choice(["I", "II", "III", "IV", "V"])
        letters = "".join(rng.choice("АБВГДЕЖЗИКЛМНОПРСТУФХЦЧШЩЭЮЯ") for _ in range(2))
        number = f"{rng.randint(0, 999999):06d}"
        return "", number, f"{series}-{letters} {number}"
    if kind == "foreign":
        series = rng.choice(["17", "26", "27", "12"])
        number = f"{rng.randint(0, 9999999):07d}"
        return series, number, f"{series} {number}"
    if kind == "vnzh":
        series = rng.choice(["81", "82"])
        number = f"{rng.randint(0, 99999999):08d}"
        return series, number, f"{series} {number}"
    number = f"{rng.randint(0, 9999999):07d}"
    return "RUS", number, f"RUS {number}"


def _build_faker_pool(fake: Faker, rng: random.Random) -> dict[str, list[str]]:
    def pick_pool(generator, size: int) -> list[str]:
        values = [generator() for _ in range(size)]
        rng.shuffle(values)
        return values

    def pick_passport_parts(size: int) -> tuple[list[str], list[str], list[str]]:
        serials: list[str] = []
        numbers: list[str] = []
        full: list[str] = []
        for _ in range(size):
            s, n, f = _sample_dul_bundle(fake, rng)
            serials.append(s)
            numbers.append(n)
            full.append(f)
        rng.shuffle(serials)
        rng.shuffle(numbers)
        rng.shuffle(full)
        return serials, numbers, full

    passport_serial, passport_number, passport_full = pick_passport_parts(1024)

    return {
        "first_name": pick_pool(fake.first_name, 1024),
        "middle_name": pick_pool(fake.middle_name, 1024),
        "last_name": pick_pool(fake.last_name, 1024),
        "fio_short": pick_pool(lambda: f"{fake.last_name()} {fake.first_name()}", 2048),
        "company": pick_pool(fake.company, 1024),
        "address": pick_pool(lambda: fake.address().replace("\n", ", "), 4096),
        "bank": pick_pool(fake.bank, 512),
        "email": pick_pool(fake.email, 4096),
        "phone": pick_pool(lambda: _normalize_isdn_digits(fake.phone_number()), 4096),
        "login": pick_pool(fake.user_name, 2048),
        "ipv4": pick_pool(fake.ipv4, 4096),
        "ipv6": pick_pool(fake.ipv6, 2048),
        "mac": pick_pool(fake.mac_address, 4096),
        "domain": pick_pool(fake.domain_name, 256),
        "birth_date": pick_pool(
            lambda: fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(),
            4096,
        ),
        "contract_date": pick_pool(
            lambda: fake.date_between(start_date="-10y", end_date="today").isoformat(),
            4096,
        ),
        "business_inn": pick_pool(fake.businesses_inn, 1024),
        "bank_account": pick_pool(fake.checking_account, 4096),
        "passport_serial": passport_serial,
        "passport_number": passport_number,
        "passport_full": passport_full,
        "latitude": pick_pool(lambda: f"{fake.latitude():.6f}", 4096),
        "longitude": pick_pool(lambda: f"{fake.longitude():.6f}", 4096),
        "document": pick_pool(
            lambda: rng.choices(
                ["Паспорт РФ", "Паспорт РФ", "Паспорт РФ", "Загранпаспорт", "Вид на жительство"],
                k=1,
            )[0],
            512,
        ),
        "department": pick_pool(
            lambda: f"ОУФМС {fake.region()} {fake.city()} {rng.randint(100, 999)}-{rng.randint(100, 999)}",
            1024,
        ),
        "tariff": ["Базовый", "Семейный", "Безлимит", "Бизнес", "IoT", "Социальный", "Премиум"],
        "dealer": ["retail", "digital", "partner", "corporate", "regional", "federal"],
        "object_desc": ["узел доступа", "распределительный узел", "магистральный порт", "VoIP-шлюз", "AGW", "BSC"],
        "cross_desc": ["кросс-1", "кросс-2", "кросс-магистраль", "кросс-подъезд", "кросс-резерв"],
        "block_desc": ["блок A", "блок B", "блок C", "блок D"],
        "pair_desc": ["пара 1-2", "пара 3-4", "пара 5-6", "пара 7-8"],
    }


def _align_columns_for_schema(data: pd.DataFrame, ordered_cols: list[str]) -> pd.DataFrame:
    for name in ordered_cols:
        if name not in data.columns:
            data[name] = pd.NA
    return data[ordered_cols]


def _chunks_per_operator(aab_per_operator: int, chunk_size: int) -> int:
    aab = int(aab_per_operator)
    return max(1, (aab + chunk_size - 1) // chunk_size)


def _build_arrow_schema(fields: list[dict[str, Any]]) -> pa.Schema:
    type_map: dict[str, pa.DataType] = {
        "string": pa.string(),
        "int": pa.int32(),
        "long": pa.int64(),
        "double": pa.float64(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("ns"),
    }
    return pa.schema([pa.field(field["name"], type_map.get(str(field["type"]), pa.string())) for field in fields])


@dataclass(frozen=True)
class _ForeignCitizenshipProfile:
    code: str
    weight: float
    first_names: tuple[str, ...]
    last_names: tuple[str, ...]
    patronymics: tuple[str, ...]
    departments: tuple[str, ...]
    documents: tuple[str, ...]


_FOREIGN_CITIZENSHIP_PROFILES: tuple[_ForeignCitizenshipProfile, ...] = (
    _ForeignCitizenshipProfile(
        "KZ",
        0.22,
        ("Айдар", "Нурлан", "Алма", "Динара", "Ерлан", "Гульнара"),
        ("Назарбаев", "Касымов", "Сериков", "Жумабекова", "Омаров"),
        ("ович", "овна", "улы", "кызы"),
        ("МВД Республики Казахстан", "CONSULATE KZ MOSCOW", "Embassy of Kazakhstan"),
        ("Паспорт Казахстана", "ID card Republic of Kazakhstan"),
    ),
    _ForeignCitizenshipProfile(
        "UZ",
        0.18,
        ("Bobur", "Dilnoza", "Javohir", "Malika", "Sardor", "Zarina"),
        ("Karimov", "Rakhimov", "Tursunov", "Yuldashev", "Nazarova"),
        ("o'g'li", "qizi", "угли", "кизи"),
        ("МВД Республики Узбекистан", "OVIR Uzbekistan", "Embassy of Uzbekistan"),
        ("Паспорт Узбекистана", "Foreign passport UZB"),
    ),
    _ForeignCitizenshipProfile(
        "TJ",
        0.10,
        ("Farhod", "Gulnora", "Jamol", "Parvina", "Rustam", "Zulfiya"),
        ("Rakhimov", "Saidov", "Nazarov", "Khojiev", "Sharipova"),
        ("ович", "овна", "зода", "зода"),
        ("МВД Республики Таджикистан", "OVIR Tajikistan", "CONSULATE TJ"),
        ("Паспорт Таджикистана", "National passport TJ"),
    ),
    _ForeignCitizenshipProfile(
        "KG",
        0.08,
        ("Nurlan", "Aizada", "Bakyt", "Cholpon", "Temir", "Ainura"),
        ("Abdykadyrov", "Osmonov", "Jeenbekov", "Sydykov", "Mambetova"),
        ("ович", "овна", "уулу", "кызы"),
        ("МВД Кыргызской Республики", "OVIR Kyrgyzstan", "Embassy KG Bishkek"),
        ("Паспорт Кыргызстана", "ID KG"),
    ),
    _ForeignCitizenshipProfile(
        "BY",
        0.10,
        ("Aliaksandr", "Hanna", "Pavel", "Volha", "Siarhei", "Natallia"),
        ("Ivanou", "Kazlou", "Savitski", "Martsinkevich", "Karpenka"),
        ("ович", "овна", "аўіч", "аўна"),
        ("МВД Республики Беларусь", "OVIR Belarus Minsk", "CONSULATE BLR"),
        ("Паспорт Беларуси", "ID card Belarus"),
    ),
    _ForeignCitizenshipProfile(
        "AM",
        0.06,
        ("Armen", "Lusine", "Gor", "Narine", "Tigran", "Ani"),
        ("Hakobyan", "Sargsyan", "Grigoryan", "Petrosyan", "Avetisyan"),
        ("ович", "овна", "ի", ""),
        ("МВД Республики Армения", "Embassy of Armenia", "CONSULATE AM"),
        ("Паспорт Армении", "Foreign passport ARM"),
    ),
    _ForeignCitizenshipProfile(
        "AZ",
        0.06,
        ("Elchin", "Leyla", "Orkhan", "Sevinc", "Rashad", "Gunel"),
        ("Aliyev", "Mammadov", "Hasanov", "Huseynov", "Quliyeva"),
        ("оглы", "кызы", "ович", "овна"),
        ("МВД Азербайджанской Республики", "Embassy of Azerbaijan", "OVIR AZ"),
        ("Паспорт Азербайджана", "Foreign passport AZE"),
    ),
    _ForeignCitizenshipProfile(
        "UA",
        0.08,
        ("Oleksandr", "Olena", "Andrii", "Iryna", "Mykhailo", "Yulia"),
        ("Shevchenko", "Kovalenko", "Bondarenko", "Melnyk", "Tkachenko"),
        ("ович", "івна", "ович", "овна"),
        ("ГУ МВД Украины", "Embassy of Ukraine", "CONSULATE UA"),
        ("Паспорт Украины", "Foreign passport UKR"),
    ),
    _ForeignCitizenshipProfile(
        "CN",
        0.06,
        ("Wei", "Li", "Ming", "Xiao", "Jun", "Yan"),
        ("Wang", "Zhang", "Liu", "Chen", "Huang"),
        ("", ""),
        ("Embassy of China", "Chinese Consulate General", "Exit-Entry Administration CN"),
        ("Паспорт КНР", "Chinese passport"),
    ),
    _ForeignCitizenshipProfile(
        "DE",
        0.04,
        ("Thomas", "Anna", "Stefan", "Julia", "Michael", "Laura"),
        ("Müller", "Schmidt", "Schneider", "Fischer", "Weber"),
        ("", ""),
        ("German Embassy Moscow", "Auswärtiges Amt", "Botschaft Deutschland"),
        ("Reisepass", "German passport"),
    ),
    _ForeignCitizenshipProfile(
        "US",
        0.02,
        ("James", "Emily", "Michael", "Sarah", "David", "Jessica"),
        ("Smith", "Johnson", "Williams", "Brown", "Jones"),
        ("", ""),
        ("US Embassy Moscow", "Department of State USA", "American Citizen Services"),
        ("US Passport", "American passport"),
    ),
)


def _pick_foreign_citizenship_codes(rng: np.random.Generator, size: int) -> np.ndarray:
    if size <= 0:
        return np.array([], dtype=object)
    weights = np.array([p.weight for p in _FOREIGN_CITIZENSHIP_PROFILES], dtype=np.float64)
    weights /= weights.sum()
    codes = [p.code for p in _FOREIGN_CITIZENSHIP_PROFILES]
    return rng.choice(codes, size=size, p=weights)


def _assign_citizenship_codes(
    rng: np.random.Generator,
    *,
    count: int,
    is_individual: np.ndarray,
    foreign_ratio: float,
) -> np.ndarray:
    codes = np.full(count, "", dtype=object)
    fl_idx = np.flatnonzero(is_individual)
    if fl_idx.size == 0:
        return codes
    codes[fl_idx] = "RU"
    ratio = max(0.0, min(0.45, float(foreign_ratio)))
    n_foreign = int(round(fl_idx.size * ratio))
    if n_foreign > 0:
        pick = rng.choice(fl_idx, size=n_foreign, replace=False)
        codes[pick] = _pick_foreign_citizenship_codes(rng, n_foreign)
    return codes


def _fill_person_names_for_citizenship(
    rng: np.random.Generator,
    *,
    citizenship: np.ndarray,
    is_individual: np.ndarray,
    ru_first: np.ndarray,
    ru_middle: np.ndarray,
    ru_last: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    first = ru_first.copy()
    middle = ru_middle.copy()
    last = ru_last.copy()
    for profile in _FOREIGN_CITIZENSHIP_PROFILES:
        mask = is_individual & (citizenship == profile.code)
        n = int(mask.sum())
        if n == 0:
            continue
        first[mask] = rng.choice(profile.first_names, size=n)
        last[mask] = rng.choice(profile.last_names, size=n)
        if profile.patronymics and profile.patronymics[0]:
            middle[mask] = rng.choice(profile.patronymics, size=n)
        else:
            middle[mask] = ""
    return first, middle, last


def _fill_department_for_citizenship(
    rng: np.random.Generator,
    *,
    citizenship: np.ndarray,
    is_individual: np.ndarray,
    ru_departments: np.ndarray,
) -> np.ndarray:
    out = ru_departments.copy()
    for profile in _FOREIGN_CITIZENSHIP_PROFILES:
        mask = is_individual & (citizenship == profile.code)
        n = int(mask.sum())
        if n > 0:
            out[mask] = rng.choice(profile.departments, size=n)
    return out


def _fill_document_for_citizenship(
    rng: np.random.Generator,
    *,
    citizenship: np.ndarray,
    is_individual: np.ndarray,
    ru_documents: np.ndarray,
) -> np.ndarray:
    out = ru_documents.copy()
    for profile in _FOREIGN_CITIZENSHIP_PROFILES:
        mask = is_individual & (citizenship == profile.code)
        n = int(mask.sum())
        if n > 0:
            out[mask] = rng.choice(profile.documents, size=n)
    return out
