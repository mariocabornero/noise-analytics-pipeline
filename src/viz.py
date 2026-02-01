from __future__ import annotations

import pandas as pd
import matplotlib.pyplot as plt


def load_data():
    df = pd.read_csv("data/processed/noise_with_rolling.csv")
    events = pd.read_csv("data/processed/detected_events.csv")

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    events["start_time"] = pd.to_datetime(events["start_time"])
    events["end_time"] = pd.to_datetime(events["end_time"])

    return df, events


def plot_time_series_with_events(
    df: pd.DataFrame,
    events: pd.DataFrame,
    site: str = "P1",
):
    df_site = df[df["site"] == site]
    events_site = events[events["site"] == site]

    plt.figure(figsize=(14, 6))

    # Serie original
    plt.plot(
        df_site["timestamp"],
        df_site["LAeq_dB"],
        label="LAeq (1 min)",
        alpha=0.5,
    )

    # Rolling mean
    plt.plot(
        df_site["timestamp"],
        df_site["LAeq_roll_mean_15min"],
        label="LAeq rolling mean (15 min)",
        linewidth=2,
    )

    # Eventos
    for i, ev in events_site.iterrows():
        plt.axvspan(
            ev["start_time"],
            ev["end_time"],
            color="tab:red",
            alpha=0.25,
            label="Evento" if i == events_site.index[0] else None,
        )

    plt.title(f"Serie temporal de ruido con eventos detectados — {site}")
    plt.xlabel("Tiempo")
    plt.ylabel("Nivel sonoro [dB(A)]")
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_event_scatter(events: pd.DataFrame):
    plt.figure(figsize=(8, 6))

    for site, g in events.groupby("site"):
        plt.scatter(
            g["duration_min"],
            g["LAeq_peak"],
            label=site,
            alpha=0.7
        )

    plt.xlabel("Duración del evento [min]")
    plt.ylabel("LAeq pico [dB(A)]")
    plt.title("Eventos: duración vs pico")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


def plot_heatmap_hour_day(df: pd.DataFrame, site: str = "P1"):
    df_site = df[df["site"] == site].copy()
    df_site["date"] = df_site["timestamp"].dt.date
    df_site["hour"] = df_site["timestamp"].dt.hour

    pivot = (
        df_site
        .pivot_table(
            index="date",
            columns="hour",
            values="LAeq_dB",
            aggfunc="mean"
        )
    )

    plt.figure(figsize=(14, 6))
    plt.imshow(pivot, aspect="auto", origin="lower")
    plt.colorbar(label="LAeq medio [dB(A)]")

    plt.xlabel("Hora del día")
    plt.title(f"Heatmap ruido — hora (día analizado, {site})")

    # 🔥 CAMBIO CLAVE: eliminar eje Y por completo
    ax = plt.gca()
    ax.yaxis.set_visible(False)

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    df, events = load_data()

    # 1️⃣ Serie temporal con eventos
    plot_time_series_with_events(df, events, site="P1")

    # 2️⃣ Heatmap hora (sin eje Y)
    plot_heatmap_hour_day(df, site="P1")

    # 3️⃣ Scatter duración vs pico
    plot_event_scatter(events)
