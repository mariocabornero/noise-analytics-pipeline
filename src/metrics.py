from __future__ import annotations

import pandas as pd


def load_clean_data(path: str = "data/processed/noise_clean.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def add_rolling_stats(
    df: pd.DataFrame,
    window_min: int = 15,
) -> pd.DataFrame:
    """
    Añade estadísticas móviles (rolling) sobre LAeq.
    """
    out = df.copy()
    out = out.sort_values(["site", "timestamp"])

    def _rolling(g):
        r = g["LAeq_dB"].rolling(
            window=window_min,
            min_periods=max(3, window_min // 3)
        )
        g[f"LAeq_roll_mean_{window_min}min"] = r.mean()
        g[f"LAeq_roll_std_{window_min}min"] = r.std()
        return g

    out = out.groupby("site", group_keys=False).apply(_rolling)
    return out


def compute_window_metrics(
    df: pd.DataFrame,
    window: str = "1h",
) -> pd.DataFrame:
    """
    Calcula métricas agregadas por ventana temporal.
    """
    df = df.set_index("timestamp")

    results = []

    for site, g in df.groupby("site"):
        m = g.resample(window).agg(
            LAeq_mean=("LAeq_dB", "mean"),
            LAeq_p95=("LAeq_dB", lambda s: s.quantile(0.95)),
            LAmax_max=("LAmax_dB", "max"),
            n_samples=("LAeq_dB", "count"),
        )
        m["site"] = site
        results.append(m)

    return (
        pd.concat(results)
        .reset_index()
        .sort_values(["site", "timestamp"])
    )


if __name__ == "__main__":
    df = load_clean_data()
    df_roll = add_rolling_stats(df, window_min=15)
    metrics_1h = compute_window_metrics(df_roll, window="1h")

    df_roll.to_csv("data/processed/noise_with_rolling.csv", index=False)
    metrics_1h.to_csv("data/processed/noise_metrics_1h.csv", index=False)

    print("✅ Procesado temporal completo")
