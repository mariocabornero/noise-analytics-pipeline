import sys
from pathlib import Path

# =========================================================
# Asegurar que src/ está en el PYTHONPATH
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

# =========================================================
# Imports del pipeline
# =========================================================
from preprocess import preprocess_file
from metrics import load_clean_data, add_rolling_stats, compute_window_metrics
from events import load_with_rolling, detect_events
from viz import (
    load_data,
    plot_time_series_with_events,
    plot_heatmap_hour_day,
    plot_event_scatter,
)


def run_pipeline(site: str = "P1"):
    print("Iniciando pipeline de análisis de ruido ambiental\n")

    # 1) Preprocesado
    print("1) Preprocesando datos...")
    preprocess_file()
    print("   Preprocesado completado\n")

    # 2) Procesado de series temporales
    print("2) Procesando series temporales...")
    df_clean = load_clean_data("data/processed/noise_clean.csv")
    df_roll = add_rolling_stats(df_clean, window_min=15)
    metrics_1h = compute_window_metrics(df_roll, window="1h")

    df_roll.to_csv("data/processed/noise_with_rolling.csv", index=False)
    metrics_1h.to_csv("data/processed/noise_metrics_1h.csv", index=False)
    print("   Rolling y métricas calculadas\n")

    # 3) Detección de eventos
    print("3) Detectando eventos...")
    df_with_roll = load_with_rolling("data/processed/noise_with_rolling.csv")
    events = detect_events(df_with_roll)
    events.to_csv("data/processed/detected_events.csv", index=False)
    print(f"   Eventos detectados: {len(events)}\n")

    # 4) Visualización
    print("4) Generando visualizaciones...")
    df_viz, events_viz = load_data()

    plot_time_series_with_events(df_viz, events_viz, site=site)
    plot_heatmap_hour_day(df_viz, site=site)
    plot_event_scatter(events_viz)

    print("\nPipeline completado con éxito")


if __name__ == "__main__":
    run_pipeline(site="P1")
