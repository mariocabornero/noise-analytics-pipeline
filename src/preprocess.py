from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PreprocessConfig:
    time_col: str = "timestamp"
    group_col: str = "site"
    laeq_col: str = "LAeq_dB"
    lamax_col: str = "LAmax_dB"
    flag_col: str = "quality_flag"

    # Reglas simples para outliers (puedes afinarlas luego)
    laeq_min: float = 20.0
    laeq_max: float = 110.0
    lamax_min: float = 20.0
    lamax_max: float = 120.0

    # Qué hacer con outliers: "nan" (recomendado) o "clip"
    outlier_strategy: str = "nan"  # "nan" | "clip"

    # Frecuencia esperada para reindexar (tu generador es 1min)
    expected_freq: str = "1min"


def load_raw_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(path)


def save_csv(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def preprocess_noise_data(df: pd.DataFrame, cfg: Optional[PreprocessConfig] = None) -> pd.DataFrame:
    cfg = cfg or PreprocessConfig()
    out = df.copy()

    # 1) timestamp a datetime y orden
    out[cfg.time_col] = pd.to_datetime(out[cfg.time_col], errors="coerce")
    out = out.dropna(subset=[cfg.time_col])
    out = out.sort_values([cfg.group_col, cfg.time_col])

    # 2) Coherencia de tipos numéricos
    out[cfg.laeq_col] = pd.to_numeric(out[cfg.laeq_col], errors="coerce")
    out[cfg.lamax_col] = pd.to_numeric(out[cfg.lamax_col], errors="coerce")

    # 3) Aplicar flags MISSING (si existen)
    if cfg.flag_col in out.columns:
        missing_mask = out[cfg.flag_col].astype(str).str.upper().eq("MISSING")
        out.loc[missing_mask, [cfg.laeq_col, cfg.lamax_col]] = np.nan

    # 4) Detectar outliers por rango físico razonable
    outlier_mask = (
        (out[cfg.laeq_col].notna() & ((out[cfg.laeq_col] < cfg.laeq_min) | (out[cfg.laeq_col] > cfg.laeq_max)))
        | (out[cfg.lamax_col].notna() & ((out[cfg.lamax_col] < cfg.lamax_min) | (out[cfg.lamax_col] > cfg.lamax_max)))
    )

    # 5) Tratamiento de outliers
    if cfg.outlier_strategy.lower() == "clip":
        out[cfg.laeq_col] = out[cfg.laeq_col].clip(cfg.laeq_min, cfg.laeq_max)
        out[cfg.lamax_col] = out[cfg.lamax_col].clip(cfg.lamax_min, cfg.lamax_max)
    else:
        out.loc[outlier_mask, [cfg.laeq_col, cfg.lamax_col]] = np.nan

    # 6) Reindexar a frecuencia regular por site (para series temporales)
    # Esto crea filas faltantes con NaN si hay huecos => perfecto para rolling/resample.
    parts = []
    for site, g in out.groupby(cfg.group_col):
        g = g.set_index(cfg.time_col).sort_index()
        full_idx = pd.date_range(g.index.min(), g.index.max(), freq=cfg.expected_freq)
        gg = g.reindex(full_idx)
        gg[cfg.group_col] = site
        parts.append(gg.reset_index().rename(columns={"index": cfg.time_col}))

    clean = pd.concat(parts, ignore_index=True).sort_values([cfg.group_col, cfg.time_col])

    # 7) Marcar una columna sencilla de “data_quality”
    clean["data_quality"] = np.where(clean[cfg.laeq_col].isna(), "MISSING_OR_OUTLIER", "OK")

    return clean


def preprocess_file(
    input_csv: str | Path = "data/raw/noise_example.csv",
    output_csv: str | Path = "data/processed/noise_clean.csv",
) -> None:
    df = load_raw_csv(input_csv)
    clean = preprocess_noise_data(df)
    save_csv(clean, output_csv)

    # Mini resumen por consola (útil para ti)
    total = len(clean)
    ok = (clean["data_quality"] == "OK").sum()
    print(f"✅ Preprocesado completo: {output_csv}")
    print(f"Filas: {total} | OK: {ok} | Con NaN: {total - ok}")


if __name__ == "__main__":
    preprocess_file()
