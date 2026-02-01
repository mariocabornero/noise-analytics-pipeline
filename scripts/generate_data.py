import numpy as np
import pandas as pd

def generate_noise_data(
    start="2026-01-01 00:00:00",
    minutes=24*60,
    freq="1min",
    sites=("P1", "P2"),
    seed=42
):
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start=start, periods=minutes, freq=freq)

    all_rows = []

    for site in sites:
        # Baseline distinto por punto (ej: P1 más ruidoso que P2)
        site_offset = 3.0 if site == "P1" else 0.0

        # Patrón día/noche suave (más ruido de día, menos de noche)
        # Usamos una senoide: pico sobre las 15:00 aprox.
        hours = (idx.hour + idx.minute / 60).to_numpy ()
        circadian = 6 * np.sin ((hours - 9) / 24 * 2 * np.pi)

        # Ruido base (fondo) + variación aleatoria
        base = 52 + site_offset + circadian
        noise = rng.normal (0, 1.5, size=len (idx))
        laeq = (base + noise).astype (float)

        # Eventos: picos (camión/moto/gritos) con duración de varios minutos
        # Generamos N eventos por día
        n_events = 10 if site == "P1" else 7
        event_positions = rng.integers(0, len(idx), size=n_events)

        event_boost = np.zeros(len(idx))
        for pos in event_positions:
            dur = int(rng.integers(2, 8))  # 2 a 7 min
            amp = float(rng.uniform(6, 18))  # +6 a +18 dB sobre fondo
            end = min(pos + dur, len(idx))
            # Forma del evento: sube, se mantiene, baja (suave)
            shape = np.linspace(0.3, 1.0, end-pos)
            event_boost[pos:end] += amp * shape

        laeq = laeq + event_boost

        # LAmax: normalmente por encima de LAeq (máximo del intervalo)
        # Lo hacemos depender del evento y un término aleatorio
        lamax = laeq + rng.uniform(2.0, 8.0, size=len(idx))

        # Outliers artificiales (lecturas imposibles o picos raros)
        outlier_mask = rng.random(len(idx)) < 0.01  # 1%
        laeq[outlier_mask] += rng.uniform(15, 30, size=outlier_mask.sum())
        lamax[outlier_mask] += rng.uniform(10, 20, size=outlier_mask.sum())

        # Huecos de datos (missing)
        missing_mask = rng.random(len(idx)) < 0.01  # 1%
        laeq[missing_mask] = np.nan
        lamax[missing_mask] = np.nan

        # Flags de calidad
        quality = np.full(len(idx), "OK", dtype=object)
        quality[missing_mask] = "MISSING"
        quality[outlier_mask] = "OUTLIER"

        df_site = pd.DataFrame({
            "timestamp": idx,
            "site": site,
            "LAeq_dB": np.round(laeq, 1),
            "LAmax_dB": np.round(lamax, 1),
            "quality_flag": quality
        })

        all_rows.append(df_site)

    df = pd.concat(all_rows, ignore_index=True)
    return df

if __name__ == "__main__":
    df = generate_noise_data()
    df.to_csv("data/raw/noise_example.csv", index=False)
    print("✅ Generado: data/raw/noise_example.csv")
    print(df.head())
