"""Нормализация и валидация IMSI / MSISDN / IMEI для binding-витрин."""

from __future__ import annotations

import pandas as pd

# ITU-T: IMSI до 15 цифр; MSISDN E.164 до 15; IMEI обычно 15 (14–16 встречается).
IMSI_MIN_LEN = 14
IMSI_MAX_LEN = 15
MSISDN_MIN_LEN = 7
MSISDN_MAX_LEN = 15
IMEI_MIN_LEN = 14
IMEI_MAX_LEN = 16


def normalize_imsi(series: pd.Series | None) -> pd.Series:
    """IMSI: только цифры, длина 14–15 (MCC+MNC+MSIN, в т.ч. иностранные)."""
    if series is None:
        return pd.Series(dtype="string")
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    digits = digits.mask(digits == "", pd.NA)
    ok = digits.notna() & digits.str.len().ge(IMSI_MIN_LEN) & digits.str.len().le(IMSI_MAX_LEN)
    return digits.where(ok)


def normalize_msisdn(series: pd.Series | None) -> pd.Series:
    """MSISDN: E.164-цифры; RU 10/11 → 7XXXXXXXXXX; иностранные 7–15 цифр без принудительной «7»."""
    if series is None:
        return pd.Series(dtype="string")
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    digits = digits.mask(digits == "", pd.NA)

    # Российский мобильный без кода страны
    ru_10 = digits.str.len() == 10
    digits = digits.mask(ru_10, "7" + digits)

    ru_11_8 = digits.str.len() == 11
    starts_8 = digits.str.startswith("8", na=False)
    digits = digits.mask(ru_11_8 & starts_8, "7" + digits.str.slice(1))

    ok = digits.notna() & digits.str.len().ge(MSISDN_MIN_LEN) & digits.str.len().le(MSISDN_MAX_LEN)
    return digits.where(ok)


def normalize_imei(series: pd.Series | None) -> pd.Series:
    """IMEI: 14–16 цифр (TAC + SNR + check), без Luhn на этапе binding."""
    if series is None:
        return pd.Series(dtype="string")
    digits = series.astype("string").str.replace(r"\D+", "", regex=True)
    digits = digits.mask(digits == "", pd.NA)
    ok = digits.notna() & digits.str.len().ge(IMEI_MIN_LEN) & digits.str.len().le(IMEI_MAX_LEN)
    return digits.where(ok)


def to_digit_string_series(series: pd.Series | None) -> pd.Series:
    """Числовые идентификаторы из parquet → string (без дробной части)."""
    if series is None:
        return pd.Series(dtype="string")
    num = pd.to_numeric(series, errors="coerce")
    out = num.astype("Int64").astype("string")
    return out.mask(out == "<NA>", pd.NA)
