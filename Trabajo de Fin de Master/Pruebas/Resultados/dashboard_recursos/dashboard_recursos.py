import os
import glob
import re

import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, dash_table, Input, Output


# -----------------------------------------------------------------------------
# Dashboard de recursos para comparar ASCON-AEAD128 y AES-CCM128
# Métricas esperadas en los CSV:
# platform,cipher,level,user_time_s,system_time_s,cpu_percent,elapsed_time,max_rss_kb
# -----------------------------------------------------------------------------


def find_csv_files():
    """Busca los CSV de recursos en la carpeta actual y, si existe, en /mnt/data."""
    candidates = []
    candidates.extend(glob.glob("resource_metrics_*.csv"))
    candidates.extend(glob.glob("/mnt/data/resource_metrics_*.csv"))
    # Evita duplicados manteniendo orden
    seen = set()
    files = []
    for path in candidates:
        real = os.path.abspath(path)
        if real not in seen:
            seen.add(real)
            files.append(path)
    return files


def parse_elapsed_to_seconds(value):
    """Convierte elapsed_time de /usr/bin/time -v a segundos.

    Formatos habituales:
      M:SS.ss      -> 0:29.58
      H:MM:SS.ss   -> 1:02:03.45
      SS.ss        -> 12.34
    """
    if pd.isna(value):
        return None
    text = str(value).strip()
    parts = text.split(":")
    try:
        if len(parts) == 1:
            return float(parts[0])
        if len(parts) == 2:
            minutes = float(parts[0])
            seconds = float(parts[1])
            return minutes * 60 + seconds
        if len(parts) == 3:
            hours = float(parts[0])
            minutes = float(parts[1])
            seconds = float(parts[2])
            return hours * 3600 + minutes * 60 + seconds
    except ValueError:
        return None
    return None


def normalize_label(value):
    mapping = {
        "pc_personal": "PC personal",
        "raspberry_pi4": "Raspberry Pi 4",
        "ascon-aead128": "ASCON-AEAD128",
        "aes-ccm128": "AES-CCM128",
    }
    return mapping.get(str(value), str(value))


def load_data():
    files = find_csv_files()
    if not files:
        raise FileNotFoundError(
            "No se encontraron archivos resource_metrics_*.csv. "
            "Coloca resource_metrics_pc_personal.csv y resource_metrics_raspberry_pi4.csv "
            "en la misma carpeta que este script."
        )

    frames = []
    for file in files:
        df = pd.read_csv(file)
        df["source_file"] = os.path.basename(file)
        frames.append(df)

    df = pd.concat(frames, ignore_index=True)

    # Tipos numéricos
    numeric_cols = ["level", "user_time_s", "system_time_s", "cpu_percent", "max_rss_kb"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["elapsed_seconds"] = df["elapsed_time"].apply(parse_elapsed_to_seconds)
    df["max_rss_mb"] = df["max_rss_kb"] / 1024.0
    df["platform_label"] = df["platform"].apply(normalize_label)
    df["cipher_label"] = df["cipher"].apply(normalize_label)
    df["level_label"] = df["level"].astype(str)

    # Orden estable para las gráficas
    platform_order = ["PC personal", "Raspberry Pi 4"]
    cipher_order = ["ASCON-AEAD128", "AES-CCM128"]
    level_order = ["512", "768", "1024"]

    df["platform_label"] = pd.Categorical(df["platform_label"], categories=platform_order, ordered=True)
    df["cipher_label"] = pd.Categorical(df["cipher_label"], categories=cipher_order, ordered=True)
    df["level_label"] = pd.Categorical(df["level_label"], categories=level_order, ordered=True)
    df = df.sort_values(["platform_label", "cipher_label", "level_label"]).reset_index(drop=True)

    return df


DATA = load_data()

METRICS = {
    "elapsed_seconds": "Tiempo real transcurrido (s)",
    "user_time_s": "Tiempo de CPU en usuario (s)",
    "system_time_s": "Tiempo de CPU en sistema (s)",
    "cpu_percent": "Uso medio de CPU (%)",
    "max_rss_mb": "Memoria máxima residente (MB)",
}

app = Dash(__name__)
app.title = "Recursos ASCON vs AES"

app.layout = html.Div(
    style={"fontFamily": "Arial, sans-serif", "margin": "30px", "maxWidth": "1300px"},
    children=[
        html.H1("Comparación de recursos: ASCON-AEAD128 vs AES-CCM128"),
        html.P(
            "Dashboard para comparar memoria máxima residente, tiempos de CPU, "
            "porcentaje de CPU y tiempo real entre PC personal y Raspberry Pi 4."
        ),

        html.Div(
            style={"display": "flex", "gap": "20px", "alignItems": "center", "marginBottom": "20px"},
            children=[
                html.Div(
                    style={"width": "360px"},
                    children=[
                        html.Label("Métrica principal"),
                        dcc.Dropdown(
                            id="metric-dropdown",
                            options=[{"label": label, "value": key} for key, label in METRICS.items()],
                            value="max_rss_mb",
                            clearable=False,
                        ),
                    ],
                ),
                html.Div(
                    style={"width": "300px"},
                    children=[
                        html.Label("Plataforma"),
                        dcc.Dropdown(
                            id="platform-dropdown",
                            options=[
                                {"label": "Todas", "value": "all"},
                                {"label": "PC personal", "value": "PC personal"},
                                {"label": "Raspberry Pi 4", "value": "Raspberry Pi 4"},
                            ],
                            value="all",
                            clearable=False,
                        ),
                    ],
                ),
            ],
        ),

        dcc.Graph(id="metric-chart"),
        dcc.Graph(id="memory-chart"),
        dcc.Graph(id="elapsed-chart"),
        dcc.Graph(id="cpu-chart"),

        html.H2("Tabla de métricas registradas"),
        dash_table.DataTable(
            id="data-table",
            columns=[
                {"name": "Plataforma", "id": "platform_label"},
                {"name": "Variante", "id": "cipher_label"},
                {"name": "Nivel", "id": "level"},
                {"name": "User time (s)", "id": "user_time_s", "type": "numeric", "format": {"specifier": ".2f"}},
                {"name": "System time (s)", "id": "system_time_s", "type": "numeric", "format": {"specifier": ".2f"}},
                {"name": "CPU (%)", "id": "cpu_percent", "type": "numeric", "format": {"specifier": ".0f"}},
                {"name": "Elapsed", "id": "elapsed_time"},
                {"name": "Elapsed (s)", "id": "elapsed_seconds", "type": "numeric", "format": {"specifier": ".2f"}},
                {"name": "Max RSS (KB)", "id": "max_rss_kb", "type": "numeric", "format": {"specifier": ".0f"}},
                {"name": "Max RSS (MB)", "id": "max_rss_mb", "type": "numeric", "format": {"specifier": ".2f"}},
            ],
            data=DATA.to_dict("records"),
            sort_action="native",
            page_size=12,
            style_table={"overflowX": "auto"},
            style_cell={"textAlign": "center", "padding": "8px"},
            style_header={"fontWeight": "bold"},
        ),

        html.P(
            "Nota: el porcentaje de CPU puede superar el 100% si el proceso aprovecha más de un núcleo "
            "o si el sistema contabiliza el uso respecto a un único núcleo lógico.",
            style={"marginTop": "20px", "fontStyle": "italic"},
        ),
    ],
)


def filtered_data(platform_value):
    if platform_value == "all":
        return DATA.copy()
    return DATA[DATA["platform_label"].astype(str) == platform_value].copy()


@app.callback(
    Output("metric-chart", "figure"),
    Input("metric-dropdown", "value"),
    Input("platform-dropdown", "value"),
)
def update_metric_chart(metric, platform_value):
    df = filtered_data(platform_value)
    title = f"{METRICS[metric]} por variante y nivel"
    fig = px.bar(
        df,
        x="level_label",
        y=metric,
        color="cipher_label",
        barmode="group",
        facet_col=None if platform_value != "all" else "platform_label",
        category_orders={
            "level_label": ["512", "768", "1024"],
            "cipher_label": ["ASCON-AEAD128", "AES-CCM128"],
            "platform_label": ["PC personal", "Raspberry Pi 4"],
        },
        labels={
            "level_label": "Nivel de seguridad",
            metric: METRICS[metric],
            "cipher_label": "Variante",
            "platform_label": "Plataforma",
        },
        title=title,
        text_auto=".2f",
    )
    fig.update_layout(legend_title_text="Variante", title_x=0.02)
    fig.update_yaxes(matches=None, showticklabels=True)
    return fig


@app.callback(
    Output("memory-chart", "figure"),
    Input("platform-dropdown", "value"),
)
def update_memory_chart(platform_value):
    df = filtered_data(platform_value)
    fig = px.bar(
        df,
        x="level_label",
        y="max_rss_mb",
        color="cipher_label",
        barmode="group",
        facet_col=None if platform_value != "all" else "platform_label",
        category_orders={
            "level_label": ["512", "768", "1024"],
            "cipher_label": ["ASCON-AEAD128", "AES-CCM128"],
            "platform_label": ["PC personal", "Raspberry Pi 4"],
        },
        labels={
            "level_label": "Nivel de seguridad",
            "max_rss_mb": "Memoria máxima residente (MB)",
            "cipher_label": "Variante",
            "platform_label": "Plataforma",
        },
        title="Memoria máxima residente por plataforma",
        text_auto=".2f",
    )
    fig.update_layout(legend_title_text="Variante", title_x=0.02)
    fig.update_yaxes(matches=None, showticklabels=True)
    return fig


@app.callback(
    Output("elapsed-chart", "figure"),
    Input("platform-dropdown", "value"),
)
def update_elapsed_chart(platform_value):
    df = filtered_data(platform_value)
    fig = px.bar(
        df,
        x="level_label",
        y="elapsed_seconds",
        color="cipher_label",
        barmode="group",
        facet_col=None if platform_value != "all" else "platform_label",
        category_orders={
            "level_label": ["512", "768", "1024"],
            "cipher_label": ["ASCON-AEAD128", "AES-CCM128"],
            "platform_label": ["PC personal", "Raspberry Pi 4"],
        },
        labels={
            "level_label": "Nivel de seguridad",
            "elapsed_seconds": "Tiempo real transcurrido (s)",
            "cipher_label": "Variante",
            "platform_label": "Plataforma",
        },
        title="Tiempo real transcurrido",
        text_auto=".2f",
    )
    fig.update_layout(legend_title_text="Variante", title_x=0.02)
    fig.update_yaxes(matches=None, showticklabels=True)
    return fig


@app.callback(
    Output("cpu-chart", "figure"),
    Input("platform-dropdown", "value"),
)
def update_cpu_chart(platform_value):
    df = filtered_data(platform_value)
    fig = px.bar(
        df,
        x="level_label",
        y="cpu_percent",
        color="cipher_label",
        barmode="group",
        facet_col=None if platform_value != "all" else "platform_label",
        category_orders={
            "level_label": ["512", "768", "1024"],
            "cipher_label": ["ASCON-AEAD128", "AES-CCM128"],
            "platform_label": ["PC personal", "Raspberry Pi 4"],
        },
        labels={
            "level_label": "Nivel de seguridad",
            "cpu_percent": "Uso medio de CPU (%)",
            "cipher_label": "Variante",
            "platform_label": "Plataforma",
        },
        title="Uso medio de CPU",
        text_auto=".0f",
    )
    fig.update_layout(legend_title_text="Variante", title_x=0.02)
    fig.update_yaxes(matches=None, showticklabels=True)
    return fig


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=8051)
