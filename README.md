# Noise Analytics Pipeline

Pipeline de análisis acústico orientado a la detección automática de eventos de ruido
y a la generación de visualizaciones temporales a partir de medidas de nivel sonoro.

El proyecto transforma datos acústicos crudos en información interpretable,
facilitando el análisis ambiental y la identificación de patrones relevantes.

---

## 📌 Funcionalidades principales

- Análisis temporal de niveles sonoros (LAeq)
- Cálculo de medias móviles
- Detección automática de eventos de ruido
- Visualización:
  - Series temporales con eventos
  - Heatmap horario
  - Relación duración vs nivel pico de eventos

---

## 🗂️ Estructura del proyecto

```text
noise-analytics-pipeline/
│
├─ src/                # Lógica principal del pipeline
│├─ preprocess.py
│├─ events.py
│├─ metrics.py
│└─ viz.py
│
├─ notebooks/          # Notebooks de demostración
│└─ 01_noise_analytics_demo.ipynb
│
├─ data/               # Datos de entrada / procesados
├─ outputs/            # Resultados generados
├─ main.py             # Punto de entrada del proyecto
├─ requirements.txt
└─ README.md
