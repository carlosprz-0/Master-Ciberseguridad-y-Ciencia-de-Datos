"""
Dashboard Dash para comparar resultados de ASCON-AEAD128 y AES-CCM128
en PC personal y Raspberry Pi 4.

Uso:
1) Instalar dependencias:
   pip install dash pandas plotly

2) Colocar este archivo en la misma carpeta donde están los CSV, o indicar la ruta:
   python dashboard_resultados.py
   RESULTS_DIR=/ruta/a/csv python dashboard_resultados.py

3) Abrir en el navegador:
   http://127.0.0.1:8050

Convención de nombres esperada:
- Raspberry:
  results_attack_ascon-aead128_512.csv
  results_attack_aes-ccm128_512.csv

- PC personal:
  results_attack_ascon-aead128_512_pc_personal.csv
  results_attack_aes-ccm128_512_pc_personal.csv
"""

import os
import re
from pathlib import Path

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, dash_table, Input, Output


DATA_DIR = Path(os.environ.get("RESULTS_DIR", Path(__file__).resolve().parent))

EXPECTED_COLUMNS = {
    "iteration",
    "total_ms",
    "setup_ms",
    "backdoor_keygen_ms",
    "recovery_ms",
    "success",
}


def parse_filename(path: Path):
    """
    Extrae variante, nivel y plataforma a partir del nombre del CSV.
    """
    name = path.name

    platform = "PC personal" if "pc_personal" in name else "Raspberry Pi 4"

    if "ascon-aead128" in name:
        variant = "ASCON-AEAD128"
    elif "aes-ccm128" in name:
        variant = "AES-CCM128"
    else:
        variant = "Desconocida"

    match = re.search(r"_(512|768|1024)(?:_pc_personal)?\.csv$", name)
    level = int(match.group(1)) if match else None

    return platform, variant, level


def load_all_results(data_dir: Path):
    """
    Carga todos los CSV y añade metadatos de plataforma, variante y nivel.
    """
    frames = []

    for path in sorted(data_dir.glob("results_attack_*.csv")):
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            print(f"No se pudo leer {path}: {exc}")
            continue

        if not EXPECTED_COLUMNS.issubset(df.columns):
            print(f"CSV ignorado por columnas inesperadas: {path.name}")
            continue

        platform, variant, level = parse_filename(path)

        if level is None or variant == "Desconocida":
            print(f"CSV ignorado por nombre no reconocido: {path.name}")
            continue

        df = df.copy()
        df["platform"] = platform
        df["variant"] = variant
        df["level"] = level
        df["source_file"] = path.name

        frames.append(df)

    if not frames:
        raise FileNotFoundError(
            f"No se encontraron CSV válidos en {data_dir}. "
            "Coloca los results_attack_*.csv junto al script o usa RESULTS_DIR."
        )

    return pd.concat(frames, ignore_index=True)


def add_clean_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Marca mediciones válidas y posibles anomalías.
    La limpieza elimina tiempos negativos y valores extremos por IQR dentro
    de cada plataforma, variante y nivel.
    """
    df = df.copy()

    base_valid = (
        (df["total_ms"] >= 0)
        & (df["setup_ms"] >= 0)
        & (df["backdoor_keygen_ms"] >= 0)
        & (df["recovery_ms"] >= 0)
    )

    df["valid_basic"] = base_valid
    df["valid_iqr"] = True

    metric_cols = ["total_ms", "setup_ms", "backdoor_keygen_ms", "recovery_ms"]

    for _, idx in df.groupby(["platform", "variant", "level"]).groups.items():
        group = df.loc[idx]

        for col in metric_cols:
            q1 = group[col].quantile(0.25)
            q3 = group[col].quantile(0.75)
            iqr = q3 - q1

            if iqr == 0:
                continue

            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr

            df.loc[idx, "valid_iqr"] &= group[col].between(lower, upper)

    df["valid_clean"] = df["valid_basic"] & df["valid_iqr"]
    return df


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula medias, medianas, desviación típica, éxito e iteraciones.
    """
    summary = (
        df.groupby(["platform", "variant", "level"], as_index=False)
        .agg(
            iterations=("iteration", "count"),
            success_count=("success", "sum"),
            success_rate=("success", "mean"),
            total_mean_ms=("total_ms", "mean"),
            total_median_ms=("total_ms", "median"),
            total_std_ms=("total_ms", "std"),
            setup_mean_ms=("setup_ms", "mean"),
            keygen_mean_ms=("backdoor_keygen_ms", "mean"),
            recovery_mean_ms=("recovery_ms", "mean"),
        )
    )

    summary["success"] = (
        summary["success_count"].astype(int).astype(str)
        + "/"
        + summary["iterations"].astype(int).astype(str)
    )

    numeric_cols = [
        "success_rate",
        "total_mean_ms",
        "total_median_ms",
        "total_std_ms",
        "setup_mean_ms",
        "keygen_mean_ms",
        "recovery_mean_ms",
    ]

    summary[numeric_cols] = summary[numeric_cols].round(6)

    return summary.sort_values(["platform", "level", "variant"])


raw_df = add_clean_flag(load_all_results(DATA_DIR))
summary_raw = summarize(raw_df)
summary_clean = summarize(raw_df[raw_df["valid_clean"]].copy())

app = Dash(__name__)
app.title = "Comparativa ASCON vs AES-CCM"

app.layout = html.Div(
    style={"fontFamily": "Arial, sans-serif", "margin": "30px"},
    children=[
        html.H1("Comparativa de resultados: ASCON-AEAD128 vs AES-CCM128"),
        html.P(
            "Dashboard para comparar tiempos medios del ataque en PC personal y Raspberry Pi 4. "
            "Los datos se calculan a partir de los CSV generados por la implementación."
        ),

        html.Div(
            style={
                "padding": "12px",
                "border": "1px solid #ddd",
                "borderRadius": "8px",
                "marginBottom": "20px",
            },
            children=[
                html.Label("Tratamiento de datos:"),
                dcc.RadioItems(
                    id="clean-mode",
                    options=[
                        {
                            "label": "Usar todas las mediciones",
                            "value": "raw",
                        },
                        {
                            "label": "Excluir anomalías temporales",
                            "value": "clean",
                        },
                    ],
                    value="clean",
                    inline=True,
                    style={"marginTop": "8px"},
                ),
                html.P(
                    "La opción de anomalías elimina tiempos negativos y valores extremos por IQR. "
                    "Es útil si aparece alguna medición temporal imposible, como una recuperación negativa.",
                    style={"fontSize": "0.9em", "color": "#555"},
                ),
            ],
        ),

        dcc.Tabs(
            children=[
                dcc.Tab(
                    label="Tiempo total medio",
                    children=[
                        dcc.Graph(id="bar-total"),
                        dcc.Graph(id="line-platform"),
                    ],
                ),
                dcc.Tab(
                    label="Desglose por fases",
                    children=[
                        dcc.Graph(id="bar-phases"),
                    ],
                ),
                dcc.Tab(
                    label="Media global",
                    children=[
                        dcc.Graph(id="bar-global"),
                    ],
                ),
                dcc.Tab(
                    label="Tabla resumen",
                    children=[
                        html.H3("Resumen calculado"),
                        dash_table.DataTable(
                            id="summary-table",
                            page_size=20,
                            sort_action="native",
                            filter_action="native",
                            style_table={"overflowX": "auto"},
                            style_cell={
                                "textAlign": "center",
                                "padding": "6px",
                                "fontFamily": "Arial",
                                "fontSize": "13px",
                            },
                            style_header={
                                "fontWeight": "bold",
                                "backgroundColor": "#f2f2f2",
                            },
                        ),
                    ],
                ),
            ]
        ),
    ],
)


@app.callback(
    Output("bar-total", "figure"),
    Output("line-platform", "figure"),
    Output("bar-phases", "figure"),
    Output("bar-global", "figure"),
    Output("summary-table", "data"),
    Output("summary-table", "columns"),
    Input("clean-mode", "value"),
)
def update_figures(clean_mode):
    data = raw_df[raw_df["valid_clean"]].copy() if clean_mode == "clean" else raw_df.copy()
    summary = summarize(data)

    fig_total = px.bar(
        summary,
        x="level",
        y="total_mean_ms",
        color="variant",
        barmode="group",
        facet_col="platform",
        text="total_mean_ms",
        labels={
            "level": "Nivel Kyber/ML-KEM",
            "total_mean_ms": "Tiempo total medio (ms)",
            "variant": "Variante",
            "platform": "Plataforma",
        },
        title="Tiempo total medio por plataforma, variante y nivel",
    )
    fig_total.update_traces(texttemplate="%{text:.3f}", textposition="outside")
    fig_total.update_layout(yaxis_title="Tiempo medio (ms)")

    fig_line = px.line(
        summary,
        x="level",
        y="total_mean_ms",
        color="platform",
        line_dash="variant",
        markers=True,
        labels={
            "level": "Nivel Kyber/ML-KEM",
            "total_mean_ms": "Tiempo total medio (ms)",
            "platform": "Plataforma",
            "variant": "Variante",
        },
        title="Evolución del tiempo total medio según nivel de seguridad",
    )

    phases = summary.melt(
        id_vars=["platform", "variant", "level"],
        value_vars=["setup_mean_ms", "keygen_mean_ms", "recovery_mean_ms"],
        var_name="phase",
        value_name="mean_ms",
    )

    phase_names = {
        "setup_mean_ms": "Inicialización",
        "keygen_mean_ms": "KeyGen con puerta trasera",
        "recovery_mean_ms": "Recuperación",
    }
    phases["phase"] = phases["phase"].map(phase_names)
    phases["config"] = (
        phases["platform"]
        + " | "
        + phases["variant"]
        + " | "
        + phases["level"].astype(str)
    )

    fig_phases = px.bar(
        phases,
        x="config",
        y="mean_ms",
        color="phase",
        labels={
            "config": "Configuración",
            "mean_ms": "Tiempo medio (ms)",
            "phase": "Fase",
        },
        title="Desglose medio por fases",
    )
    fig_phases.update_layout(xaxis_tickangle=-35)

    global_summary = (
        summary.groupby(["platform", "variant"], as_index=False)
        .agg(global_total_mean_ms=("total_mean_ms", "mean"))
    )
    global_summary["global_total_mean_ms"] = global_summary["global_total_mean_ms"].round(6)

    fig_global = px.bar(
        global_summary,
        x="platform",
        y="global_total_mean_ms",
        color="variant",
        barmode="group",
        text="global_total_mean_ms",
        labels={
            "platform": "Plataforma",
            "global_total_mean_ms": "Media global del tiempo total (ms)",
            "variant": "Variante",
        },
        title="Media global del tiempo total por plataforma y variante",
    )
    fig_global.update_traces(texttemplate="%{text:.3f}", textposition="outside")

    table = summary.copy()
    table = table[
        [
            "platform",
            "variant",
            "level",
            "iterations",
            "success",
            "total_mean_ms",
            "setup_mean_ms",
            "keygen_mean_ms",
            "recovery_mean_ms",
            "total_median_ms",
            "total_std_ms",
        ]
    ]

    table.columns = [
        "Plataforma",
        "Variante",
        "Nivel",
        "Iteraciones",
        "Éxito",
        "T.Total media",
        "T.Setup media",
        "T.KeyGen media",
        "T.Recuperación media",
        "T.Total mediana",
        "T.Total desv.",
    ]

    columns = [{"name": col, "id": col} for col in table.columns]

    return (
        fig_total,
        fig_line,
        fig_phases,
        fig_global,
        table.to_dict("records"),
        columns,
    )


if __name__ == "__main__":
    print(f"Leyendo CSV desde: {DATA_DIR}")
    print("Archivos encontrados:")
    for file in sorted(DATA_DIR.glob("results_attack_*.csv")):
        print(f" - {file.name}")
    app.run(debug=True, host="127.0.0.1", port=8050)
