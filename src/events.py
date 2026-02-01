from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True)
class EventConfig:
    k_sigma: float = 2.0          # umbral = mean + k * std
    min_duration_min: int = 3     # duración mínima del evento (min)
    site_col: str = "site"
    time_col: str = "timestamp"
    laeq_col: str = "LAeq_dB"
    roll_mean_col: str = "LAeq_roll_mean_15min"
    roll_std_col: str = "LAeq_roll_std_15min"


def load_with_rolling(path: str = "data/processed/noise_with_rolling.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def detect_events(df: pd.DataFrame, cfg: EventConfig = EventConfig()) -> pd.DataFrame:
    out = df.copy().sort_values([cfg.site_col, cfg.time_col])

    # Condición de evento punto a punto
    threshold = out[cfg.roll_mean_col] + cfg.k_sigma * out[cfg.roll_std_col]
    out["is_event_point"] = out[cfg.laeq_col] > threshold

    events = []

    for site, g in out.groupby(cfg.site_col):
        g = g.sort_values(cfg.time_col)

        # Identificar cambios en el estado evento/no-evento
        g["event_block"] = (g["is_event_point"] != g["is_event_point"].shift()).cumsum()

        for _, block in g.groupby("event_block"):
            if not block["is_event_point"].iloc[0]:
                continue

            start = block[cfg.time_col].iloc[0]
            end = block[cfg.time_col].iloc[-1]
            duration = (end - start).total_seconds() / 60 + 1

            if duration < cfg.min_duration_min:
                continue

            event = {
                "site": site,
                "start_time": start,
                "end_time": end,
                "duration_min": int(duration),
                "LAeq_peak": block[cfg.laeq_col].max(),
                "LAeq_mean": block[cfg.laeq_col].mean(),
            }
            events.append(event)

    return pd.DataFrame(events)


if __name__ == "__main__":
    df = load_with_rolling()
    events = detect_events(df)

    events.to_csv("data/processed/detected_events.csv", index=False)

    print("✅ Detección de eventos completada")
    print(f"Eventos detectados: {len(events)}")
