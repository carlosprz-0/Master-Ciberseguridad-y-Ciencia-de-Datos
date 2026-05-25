# ============================================================
# Proyecto final de Visualización
# Historia:
# Desigualdad territorial en la provincia de Santa Cruz de Tenerife: renta, fuentes de ingresos
# y estructura laboral por sección censal entre 2021 y 2023
#
# Autor: Carlos Pérez Fino
#
# Objetivo:
# - Construir un pipeline DataOps con Dagster.
# - Usar datos de renta, fuentes de ingresos, ocupación y actividad.
# - Generar visualizaciones basadas en gramática de gráficos.
# - Incluir mapas a nivel de sección censal usando los GeoJSON.
# - Aplicar checks de calidad sobre datos, transformaciones y salidas.
# ============================================================

from pathlib import Path
import hashlib
import os

import numpy as np
import pandas as pd
import geopandas as gpd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from plotnine import (
    ggplot,
    aes,
    geom_point,
    geom_line,
    geom_col,
    labs,
    theme_minimal,
    theme,
    element_text,
    coord_flip,
    scale_y_continuous,
)

from dagster import (
    Definitions,
    MetadataValue,
    AssetCheckResult,
    AssetSelection,
    RunRequest,
    SkipReason,
    asset,
    asset_check,
    define_asset_job,
    sensor,
)


# ============================================================
# Configuración de rutas
# ============================================================

ROOT_DIR = Path(__file__).resolve().parent


def encontrar_directorio_datos() -> Path:
    """
    Busca automáticamente la carpeta donde están los CSV y geoJSON.
    Así el proyecto funciona aunque el ZIP esté descomprimido
    directamente en la raíz o dentro de una subcarpeta.
    """
    candidatos = [
        ROOT_DIR / "datos-proyecto-viz-2526",
        ROOT_DIR / "data" / "datos-proyecto-viz-2526",
        ROOT_DIR,
    ]

    for carpeta in candidatos:
        if (carpeta / "rentamedia-sc-3.csv").exists():
            return carpeta

    raise FileNotFoundError(
        "No se encuentra la carpeta de datos. "
        "Coloca los CSV dentro de datos-proyecto-viz-2526/"
    )


DATA_DIR = encontrar_directorio_datos()
CARTO_DIR = DATA_DIR / "cartografia-secciones"

OUTPUT_DIR = ROOT_DIR / "outputs"
FIG_DIR = OUTPUT_DIR / "figures"
TABLE_DIR = OUTPUT_DIR / "tables"

FIG_DIR.mkdir(parents=True, exist_ok=True)
TABLE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# Funciones auxiliares de limpieza
# ============================================================

def leer_csv_limpio(path: Path) -> pd.DataFrame:
    """
    Lee un CSV eliminando problemas habituales de codificación y espacios en blanco en los nombres de columnas.
    """
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [c.replace("\ufeff", "").strip() for c in df.columns]
    return df


def convertir_numero_es(serie: pd.Series) -> pd.Series:
    """
    Convierte números con formato 1,0 a 1.0:
    También respeta columnas que ya vienen como numéricas.
    """
    if pd.api.types.is_numeric_dtype(serie):
        return pd.to_numeric(serie, errors="coerce")

    def parsear(valor):
        if pd.isna(valor):
            return np.nan

        texto = str(valor).strip().replace("%", "")
        if texto == "":
            return np.nan

        if "," in texto:
            texto = texto.replace(".", "").replace(",", ".")

        return pd.to_numeric(texto, errors="coerce")

    return serie.map(parsear)


def clave_seccion_desde_geocode(serie: pd.Series) -> pd.Series:
    """
    Crea una clave estable de sección eliminando la fecha del geocode.

    Ejemplo:
    20240101_38001_D01_S001 -> 38001_D01_S001

    Esto permite comparar años aunque el mapa de renta use:
    2021 -> cartografía 2022
    2022 -> cartografía 2023
    2023 -> cartografía 2024
    """
    return serie.astype(str).str.replace(r"^\d{8}_", "", regex=True)


def cargar_geojson_secciones(anio_cartografia: int) -> gpd.GeoDataFrame:
    """
    Carga el GeoJSON de secciones de Tenerife correspondiente al año indicado.
    Se simplifica ligeramente la geometría para que los mapas se generen rápido.
    """
    path = CARTO_DIR / f"secciones_{anio_cartografia}0101_tenerife.json"

    if not path.exists():
        raise FileNotFoundError(f"No existe el GeoJSON: {path}")

    gdf = gpd.read_file(path).to_crs(epsg=4326)

    gdf["geometry"] = gdf.geometry.simplify(
        tolerance=0.0002,
        preserve_topology=True,
    )

    gdf["section_key"] = clave_seccion_desde_geocode(gdf["geocode"])

    return gdf


def guardar_mapa_coropletico(
    gdf: gpd.GeoDataFrame,
    columna: str,
    path: Path,
    titulo: str,
    etiqueta_leyenda: str,
    cmap: str = "viridis",
) -> str:
    """
    Genera un mapa coroplético:
    - Dataset: GeoDataFrame con geometría y variable.
    - Geometría: polígonos de secciones.
    - Estética: fill/color según la variable.
    - Escala: continua.
    - Etiquetas: título, leyenda y fuente.
    """
    fig, ax = plt.subplots(figsize=(10, 8))

    gdf.plot(
        column=columna,
        ax=ax,
        cmap=cmap,
        linewidth=0.08,
        edgecolor="white",
        legend=True,
        legend_kwds={
            "label": etiqueta_leyenda,
            "shrink": 0.65,
        },
        missing_kwds={
            "color": "lightgrey",
            "label": "Sin dato",
        },
    )

    ax.set_axis_off()
    ax.set_title(titulo, fontsize=15, weight="bold", loc="left")
    ax.text(
        0.01,
        0.02,
        "Fuente: ISTAC / INE. Elaboración propia por secciones censales.",
        transform=ax.transAxes,
        fontsize=8,
    )

    plt.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)

    return str(path)


def guardar_plotnine(plot, path: Path, width: int = 9, height: int = 6) -> str:
    """
    Guarda una visualización creada con plotnine.
    """
    plot.save(
        filename=str(path),
        width=width,
        height=height,
        dpi=180,
        verbose=False,
    )
    return str(path)


def comprobar_png(path: str) -> bool:
    """
    Verifica que un PNG existe y no está vacío.
    """
    p = Path(path)
    return p.exists() and p.stat().st_size > 0


# ============================================================
# Assets RAW: carga de datos originales
# ============================================================

@asset
def raw_renta_media() -> pd.DataFrame:
    """
    Carga el dataset de renta media y mediana por secciones.
    """
    df = leer_csv_limpio(DATA_DIR / "rentamedia-sc-3.csv")

    df = df.rename(
        columns={
            "año": "year",
            "TERRITORIO_CODE": "geocode",
            "OBS_VALUE": "value",
            "MEDIDAS#es": "medida",
        }
    )

    df["year"] = df["year"].astype(int)
    df["value"] = convertir_numero_es(df["value"])
    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["section_key"] = clave_seccion_desde_geocode(df["geocode"])

    return df


@asset
def raw_distribucion_ingresos() -> pd.DataFrame:
    """
    Carga el dataset de distribución de renta según fuente de ingresos.
    """
    df = leer_csv_limpio(DATA_DIR / "distribucion-renta-ingresos.csv")

    df = df.rename(
        columns={
            "año": "year",
            "TERRITORIO_CODE": "geocode",
            "OBS_VALUE": "value",
            "MEDIDAS#es": "fuente",
        }
    )

    df["year"] = df["year"].astype(int)
    df["value"] = convertir_numero_es(df["value"])
    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["section_key"] = clave_seccion_desde_geocode(df["geocode"])

    return df


@asset
def raw_ocupacion() -> pd.DataFrame:
    """
    Carga el dataset de ocupación por secciones.
    """
    df = leer_csv_limpio(DATA_DIR / "ocupacion-sc-3.csv")

    df = df.rename(
        columns={
            "año": "year",
            "geocode": "geo_ocupacion",
            "num_casos": "casos",
        }
    )

    df["year"] = df["year"].astype(int)
    df["casos"] = convertir_numero_es(df["casos"]).fillna(0)
    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["section_key"] = clave_seccion_desde_geocode(df["geo_ocupacion"])

    return df


@asset
def raw_actividad_economica() -> pd.DataFrame:
    """
    Carga el dataset de actividad económica por secciones.
    """
    df = leer_csv_limpio(DATA_DIR / "actividad-sc-3.csv")

    df = df.rename(
        columns={
            "Periodo": "year",
            "Actividad económica": "sector",
            "Sexo": "sexo",
            "num_casos": "casos",
            "geocode": "geo_actividad",
        }
    )

    df["year"] = df["year"].astype(int)
    df["casos"] = convertir_numero_es(df["casos"]).fillna(0)
    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["section_key"] = clave_seccion_desde_geocode(df["geo_actividad"])

    return df


# ============================================================
# Assets: transformación y síntesis
# ============================================================

@asset
def renta_media_curated(raw_renta_media: pd.DataFrame) -> pd.DataFrame:
    """
    Construye una tabla de renta por sección y año.

    Se seleccionan indicadores útiles para la historia:
    - Renta bruta media por persona.
    - Renta bruta media por hogar.
    - Media de renta neta por unidad de consumo.
    - Mediana de renta neta por unidad de consumo.

    Además se calcula un proxy sencillo de desigualdad:
    media / mediana.
    """
    df = raw_renta_media.copy()

    metricas = {
        "RENTA_BRUTA_MEDIA_PERSONA": "renta_bruta_media_persona",
        "RENTA_BRUTA_MEDIA_HOGAR": "renta_bruta_media_hogar",
        "RENTA_NETA_UNIDAD_CONSUMO_MEDIA": "renta_neta_uc_media",
        "RENTA_NETA_UNIDAD_CONSUMO_MEDIANA": "renta_neta_uc_mediana",
    }

    df = df[df["MEDIDAS_CODE"].isin(metricas)].copy()
    df["metric"] = df["MEDIDAS_CODE"].map(metricas)

    out = (
        df.pivot_table(
            index=[
                "year",
                "geocode",
                "section_key",
                "municipio",
                "distrito",
                "seccion",
            ],
            columns="metric",
            values="value",
            aggfunc="mean",
        )
        .reset_index()
    )

    out.columns.name = None

    out["renta_ratio_media_mediana"] = (
        out["renta_neta_uc_media"] / out["renta_neta_uc_mediana"]
    )

    # Regla indicada en el enunciado para la cartografía de renta.
    out["map_year_renta"] = out["year"].map(
        {
            2021: 2022,
            2022: 2023,
            2023: 2024,
        }
    )

    out.to_csv(TABLE_DIR / "renta_media_curated.csv", index=False)

    return out


@asset
def distribucion_ingresos_curated(
    raw_distribucion_ingresos: pd.DataFrame,
) -> pd.DataFrame:
    """
    Convierte las fuentes de renta en columnas porcentuales.

    Ejemplo:
    - pct_sueldos_salarios
    - pct_pensiones
    - pct_prestaciones_desempleo
    """
    df = raw_distribucion_ingresos.copy()

    fuentes = {
        "SUELDOS_SALARIOS": "pct_sueldos_salarios",
        "PENSIONES": "pct_pensiones",
        "PRESTACIONES_DESEMPLEO": "pct_prestaciones_desempleo",
        "OTRAS_PRESTACIONES": "pct_otras_prestaciones",
        "OTROS_INGRESOS": "pct_otros_ingresos",
    }

    df["source_metric"] = df["MEDIDAS_CODE"].map(fuentes)

    out = (
        df.pivot_table(
            index=["year", "geocode", "section_key", "municipio"],
            columns="source_metric",
            values="value",
            aggfunc="mean",
        )
        .reset_index()
    )

    out.columns.name = None

    out.to_csv(TABLE_DIR / "distribucion_ingresos_curated.csv", index=False)

    return out


@asset
def ocupacion_curated(raw_ocupacion: pd.DataFrame) -> pd.DataFrame:
    """
    Sintetiza la ocupación por sección.

    Se calculan:
    - porcentaje de ocupación de alta cualificación.
    - porcentaje de ocupaciones elementales.
    - total de ocupación conocida.
    """
    df = raw_ocupacion.copy()

    out = (
        df.pivot_table(
            index=["year", "geo_ocupacion", "section_key", "municipio"],
            columns="ocupacion",
            values="casos",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    out.columns.name = None

    col_alta = "Directores/gerentes y profesionales/técnicos de nivel medio o alto"
    col_elemental = "Ocupaciones elementales"
    col_bajo = "Trabajadores cualificados y oficiales/operarios de nivel bajo"
    col_no_consta = "No consta"

    for col in [col_alta, col_elemental, col_bajo, col_no_consta]:
        if col not in out.columns:
            out[col] = 0

    out["ocupacion_total"] = out[
        [col_alta, col_elemental, col_bajo, col_no_consta]
    ].sum(axis=1)

    out["ocupacion_total_conocida"] = out[
        [col_alta, col_elemental, col_bajo]
    ].sum(axis=1)

    denominador = out["ocupacion_total_conocida"].replace({0: np.nan})

    out["pct_alta_cualificacion"] = 100 * out[col_alta] / denominador
    out["pct_ocupaciones_elementales"] = 100 * out[col_elemental] / denominador

    out = out.rename(
        columns={
            col_alta: "casos_alta_cualificacion",
            col_elemental: "casos_ocupaciones_elementales",
            col_bajo: "casos_cualificados_bajo",
            col_no_consta: "casos_ocupacion_no_consta",
        }
    )

    out.to_csv(TABLE_DIR / "ocupacion_curated.csv", index=False)

    return out


@asset
def actividad_curated(raw_actividad_economica: pd.DataFrame) -> pd.DataFrame:
    """
    Sintetiza la actividad económica por sección.

    Se calculan:
    - porcentaje de servicios.
    - porcentaje de industria + construcción.
    """
    df = raw_actividad_economica.copy()

    out = (
        df.pivot_table(
            index=["year", "geo_actividad", "section_key", "municipio"],
            columns="sector",
            values="casos",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    out.columns.name = None

    sectores = [
        "Agricultura, ganadería y pesca",
        "Construcción",
        "Industria",
        "Servicios",
        "No consta",
    ]

    for col in sectores:
        if col not in out.columns:
            out[col] = 0

    sectores_conocidos = [
        "Agricultura, ganadería y pesca",
        "Construcción",
        "Industria",
        "Servicios",
    ]

    out["actividad_total_conocida"] = out[sectores_conocidos].sum(axis=1)

    denominador = out["actividad_total_conocida"].replace({0: np.nan})

    out["pct_servicios"] = 100 * out["Servicios"] / denominador

    out["pct_industria_construccion"] = (
        100 * (out["Industria"] + out["Construcción"]) / denominador
    )

    out = out.rename(
        columns={
            "Agricultura, ganadería y pesca": "casos_agricultura",
            "Construcción": "casos_construccion",
            "Industria": "casos_industria",
            "Servicios": "casos_servicios",
            "No consta": "casos_actividad_no_consta",
        }
    )

    out.to_csv(TABLE_DIR / "actividad_curated.csv", index=False)

    return out


@asset
def indicadores_seccion(
    renta_media_curated: pd.DataFrame,
    distribucion_ingresos_curated: pd.DataFrame,
    ocupacion_curated: pd.DataFrame,
    actividad_curated: pd.DataFrame,
) -> pd.DataFrame:
    """
    Tabla final integrada por sección y año.

    Une:
    - Renta media y mediana.
    - Distribución de ingresos.
    - Ocupación.
    - Actividad económica.
    """
    renta = renta_media_curated.rename(columns={"geocode": "geo_renta"}).copy()

    ingresos = (
        distribucion_ingresos_curated
        .rename(columns={"geocode": "geo_distribucion_ingresos"})
        .drop(columns=["municipio"], errors="ignore")
        .copy()
    )

    ocupacion = (
        ocupacion_curated
        .drop(columns=["municipio"], errors="ignore")
        .copy()
    )

    actividad = (
        actividad_curated
        .drop(columns=["municipio"], errors="ignore")
        .copy()
    )

    out = (
        renta
        .merge(ingresos, on=["year", "section_key"], how="left")
        .merge(ocupacion, on=["year", "section_key"], how="left")
        .merge(actividad, on=["year", "section_key"], how="left")
    )

    out["perfil_renta"] = pd.cut(
        out["renta_bruta_media_persona"],
        bins=[0, 12000, 16000, 20000, np.inf],
        labels=["baja", "media-baja", "media-alta", "alta"],
    )

    out.to_csv(TABLE_DIR / "indicadores_seccion.csv", index=False)

    return out


# ============================================================
# Assets de visualización
# ============================================================

@asset
def plot_mapa_renta_2023(indicadores_seccion: pd.DataFrame) -> str:
    """
    Mapa principal:
    renta bruta media por persona en 2023.

    Según el enunciado, para renta de 2023 se usa la cartografía 2024.
    """
    df = indicadores_seccion[indicadores_seccion["year"] == 2023].copy()

    gdf = cargar_geojson_secciones(2024)

    gdf = gdf.merge(
        df[["geo_renta", "renta_bruta_media_persona"]],
        left_on="geocode",
        right_on="geo_renta",
        how="left",
    )

    return guardar_mapa_coropletico(
        gdf=gdf,
        columna="renta_bruta_media_persona",
        path=FIG_DIR / "01_mapa_renta_2023.png",
        titulo="Renta bruta media por persona · secciones de la provincia de Santa Cruz de Tenerife (2023)",
        etiqueta_leyenda="Euros",
        cmap="viridis",
    )


@asset
def plot_mapa_cambio_renta_2021_2023(indicadores_seccion: pd.DataFrame) -> str:
    """
    Mapa de cambio:
    variación porcentual de renta bruta media por persona entre 2021 y 2023.
    """
    renta_2021 = (
        indicadores_seccion[indicadores_seccion["year"] == 2021]
        [["section_key", "renta_bruta_media_persona"]]
        .rename(columns={"renta_bruta_media_persona": "renta_2021"})
    )

    renta_2023 = (
        indicadores_seccion[indicadores_seccion["year"] == 2023]
        [["section_key", "renta_bruta_media_persona"]]
        .rename(columns={"renta_bruta_media_persona": "renta_2023"})
    )

    cambio = renta_2023.merge(renta_2021, on="section_key", how="left")

    cambio["var_renta_pct"] = (
        100 * (cambio["renta_2023"] - cambio["renta_2021"]) / cambio["renta_2021"]
    )

    gdf = cargar_geojson_secciones(2024)

    gdf = gdf.merge(
        cambio[["section_key", "var_renta_pct"]],
        on="section_key",
        how="left",
    )

    return guardar_mapa_coropletico(
        gdf=gdf,
        columna="var_renta_pct",
        path=FIG_DIR / "02_mapa_cambio_renta_2021_2023.png",
        titulo="Variación de la renta bruta por persona (2021-2023)",
        etiqueta_leyenda="Variación %",
        cmap="RdYlGn",
    )


@asset
def plot_mapa_servicios_2023(indicadores_seccion: pd.DataFrame) -> str:
    """
    Mapa de actividad económica:
    peso del sector servicios en 2023.

    En actividad económica se usa el mapa del año correspondiente.
    Para 2023 se usa secciones_20230101_tenerife.json.
    """
    df = indicadores_seccion[indicadores_seccion["year"] == 2023].copy()

    gdf = cargar_geojson_secciones(2023)

    gdf = gdf.merge(
        df[["geo_actividad", "pct_servicios"]],
        left_on="geocode",
        right_on="geo_actividad",
        how="left",
    )

    return guardar_mapa_coropletico(
        gdf=gdf,
        columna="pct_servicios",
        path=FIG_DIR / "03_mapa_servicios_2023.png",
        titulo="Peso del sector servicios por sección censal (2023)",
        etiqueta_leyenda="% servicios",
        cmap="magma",
    )


@asset
def plot_mapa_ocupacion_alta_2023(indicadores_seccion: pd.DataFrame) -> str:
    """
    Mapa de ocupación:
    porcentaje de ocupación de alta cualificación en 2023.
    """
    df = indicadores_seccion[indicadores_seccion["year"] == 2023].copy()

    gdf = cargar_geojson_secciones(2023)

    gdf = gdf.merge(
        df[["geo_ocupacion", "pct_alta_cualificacion"]],
        left_on="geocode",
        right_on="geo_ocupacion",
        how="left",
    )

    return guardar_mapa_coropletico(
        gdf=gdf,
        columna="pct_alta_cualificacion",
        path=FIG_DIR / "04_mapa_ocupacion_alta_2023.png",
        titulo="Ocupación de alta cualificación por sección censal (2023)",
        etiqueta_leyenda="% alta cualificación",
        cmap="plasma",
    )


@asset
def plot_scatter_renta_ocupacion_2023(indicadores_seccion: pd.DataFrame) -> str:
    """
    Diagrama de dispersión:
    relación entre renta y ocupación de alta cualificación.

    Gramática:
    - Dataset: indicadores integrados por sección.
    - Geometría: puntos.
    - Eje X: % alta cualificación.
    - Eje Y: renta bruta media por persona.
    - Color: % servicios.
    - Tamaño: ocupación total conocida.
    """
    df = indicadores_seccion[indicadores_seccion["year"] == 2023].copy()

    df = df.dropna(
        subset=[
            "renta_bruta_media_persona",
            "pct_alta_cualificacion",
            "pct_servicios",
            "ocupacion_total_conocida",
        ]
    )

    # Línea de tendencia calculada manualmente para evitar dependencias extra.
    x = df["pct_alta_cualificacion"].astype(float)
    y = df["renta_bruta_media_persona"].astype(float)

    coef = np.polyfit(x, y, deg=1)

    tendencia = pd.DataFrame(
        {
            "pct_alta_cualificacion": [x.min(), x.max()],
            "renta_predicha": [
                coef[0] * x.min() + coef[1],
                coef[0] * x.max() + coef[1],
            ],
        }
    )

    p = (
        ggplot(
            df,
            aes(
                x="pct_alta_cualificacion",
                y="renta_bruta_media_persona",
                color="pct_servicios",
                size="ocupacion_total_conocida",
            ),
        )
        + geom_point(alpha=0.75)
        + geom_line(
            tendencia,
            aes(
                x="pct_alta_cualificacion",
                y="renta_predicha",
            ),
            inherit_aes=False,
            linetype="dashed",
        )
        + labs(
            title="Renta y ocupación cualificada por sección (2023)",
            subtitle="Cada punto representa una sección censal de Santa Cruz de Tenerife",
            x="% ocupación de alta cualificación",
            y="Renta bruta media por persona (€)",
            color="% servicios",
            size="Ocupación conocida",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(weight="bold", size=14),
            legend_position="right",
        )
    )

    return guardar_plotnine(
        p,
        FIG_DIR / "05_scatter_renta_ocupacion_2023.png",
        width=9,
        height=6,
    )


@asset
def plot_fuentes_renta_evolucion(
    raw_distribucion_ingresos: pd.DataFrame,
) -> str:
    """
    Evolución media de las fuentes de renta entre 2021 y 2023.
    """
    df = raw_distribucion_ingresos.copy()

    resumen = (
        df.groupby(["year", "fuente"], as_index=False)["value"]
        .mean()
        .sort_values(["fuente", "year"])
    )

    p = (
        ggplot(
            resumen,
            aes(
                x="factor(year)",
                y="value",
                group="fuente",
                color="fuente",
            ),
        )
        + geom_line()
        + geom_point(size=2)
        + labs(
            title="Evolución media de las fuentes de renta por sección",
            subtitle="Promedio de las secciones de la provincia de Santa Cruz de Tenerife",
            x="Año",
            y="Peso medio (%)",
            color="Fuente de renta",
        )
        + theme_minimal()
        + theme(
            plot_title=element_text(weight="bold", size=14),
            axis_text_x=element_text(rotation=0),
            legend_position="right",
        )
    )

    return guardar_plotnine(
        p,
        FIG_DIR / "06_fuentes_renta_evolucion.png",
        width=9,
        height=5,
    )


@asset
def plot_top_municipios_renta_2023(indicadores_seccion: pd.DataFrame) -> str:
    """
    Ranking municipal:
    media municipal de renta bruta por persona en 2023.

    Este gráfico ayuda a cerrar la historia comparando el patrón
    de secciones con una visión agregada por municipio.
    """
    df = indicadores_seccion[indicadores_seccion["year"] == 2023].copy()

    resumen = (
        df.groupby("municipio", as_index=False)
        .agg(
            renta_media_municipal=(
                "renta_bruta_media_persona",
                "mean",
            ),
            pct_servicios=("pct_servicios", "mean"),
            pct_alta_cualificacion=("pct_alta_cualificacion", "mean"),
        )
        .sort_values("renta_media_municipal", ascending=False)
        .head(20)
    )

    p = (
        ggplot(
            resumen,
            aes(
                x="reorder(municipio, renta_media_municipal)",
                y="renta_media_municipal",
            ),
        )
        + geom_col()
        + coord_flip()
        + labs(
            title="Top 20 municipios por renta media de sus secciones (2023)",
            subtitle="Renta bruta media por persona agregada a nivel municipal",
            x="Municipio",
            y="Renta bruta media por persona (€)",
        )
        + scale_y_continuous(labels=lambda valores: [f"{v:,.0f}" for v in valores])
        + theme_minimal()
        + theme(
            plot_title=element_text(weight="bold", size=14),
            axis_text_y=element_text(size=8),
        )
    )

    return guardar_plotnine(
        p,
        FIG_DIR / "07_top_municipios_renta_2023.png",
        width=9,
        height=6,
    )


# ============================================================
# Catálogo de visualizaciones e index HTML
# ============================================================

@asset
def catalogo_visualizaciones(
    plot_mapa_renta_2023: str,
    plot_mapa_cambio_renta_2021_2023: str,
    plot_mapa_servicios_2023: str,
    plot_mapa_ocupacion_alta_2023: str,
    plot_scatter_renta_ocupacion_2023: str,
    plot_fuentes_renta_evolucion: str,
    plot_top_municipios_renta_2023: str,
) -> pd.DataFrame:
    """
    Crea un catálogo de salidas visuales.
    Sirve para trazabilidad y para checks de calidad.
    """
    registros = [
        {
            "orden": 1,
            "titulo": "Mapa de renta 2023",
            "path": plot_mapa_renta_2023,
            "tipo": "mapa",
        },
        {
            "orden": 2,
            "titulo": "Cambio de renta 2021-2023",
            "path": plot_mapa_cambio_renta_2021_2023,
            "tipo": "mapa",
        },
        {
            "orden": 3,
            "titulo": "Mapa del peso del sector servicios 2023",
            "path": plot_mapa_servicios_2023,
            "tipo": "mapa",
        },
        {
            "orden": 4,
            "titulo": "Mapa de ocupación de alta cualificación 2023",
            "path": plot_mapa_ocupacion_alta_2023,
            "tipo": "mapa",
        },
        {
            "orden": 5,
            "titulo": "Renta y ocupación cualificada",
            "path": plot_scatter_renta_ocupacion_2023,
            "tipo": "dispersión",
        },
        {
            "orden": 6,
            "titulo": "Evolución de fuentes de renta",
            "path": plot_fuentes_renta_evolucion,
            "tipo": "líneas",
        },
        {
            "orden": 7,
            "titulo": "Top municipios por renta",
            "path": plot_top_municipios_renta_2023,
            "tipo": "barras",
        },
    ]

    df = pd.DataFrame(registros)

    df.to_csv(TABLE_DIR / "catalogo_visualizaciones.csv", index=False)

    return df


@asset
def pagina_html_resultados(
    indicadores_seccion: pd.DataFrame,
    catalogo_visualizaciones: pd.DataFrame,
) -> str:
    """
    Genera una página HTML sencilla con la historia y las visualizaciones.
    """
    df_2023 = indicadores_seccion[indicadores_seccion["year"] == 2023].copy()

    renta_media = df_2023["renta_bruta_media_persona"].mean()
    servicios_medio = df_2023["pct_servicios"].mean()
    alta_cualificacion_media = df_2023["pct_alta_cualificacion"].mean()

    tarjetas = ""

    for _, row in catalogo_visualizaciones.sort_values("orden").iterrows():
        path = Path(row["path"])
        rel_path = path.relative_to(OUTPUT_DIR)

        tarjetas += f"""
        <section class="card">
            <h2>{row["orden"]}. {row["titulo"]}</h2>
            <p>Tipo de visualización: <strong>{row["tipo"]}</strong></p>
            <img src="{rel_path.as_posix()}" alt="{row["titulo"]}">
        </section>
        """

    html = f"""
    <!doctype html>
    <html lang="es">
    <head>
        <meta charset="utf-8">
        <title>Proyecto final de visualización</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 40px;
                background: #f7f7f7;
                color: #222;
            }}
            h1 {{
                max-width: 1100px;
            }}
            .intro {{
                max-width: 1100px;
                background: white;
                padding: 24px;
                border-radius: 12px;
                margin-bottom: 24px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            }}
            .metrics {{
                display: flex;
                gap: 16px;
                flex-wrap: wrap;
                margin-top: 16px;
            }}
            .metric {{
                background: #eeeeee;
                border-radius: 10px;
                padding: 14px;
                min-width: 220px;
            }}
            .metric strong {{
                font-size: 22px;
            }}
            .card {{
                max-width: 1100px;
                background: white;
                padding: 24px;
                border-radius: 12px;
                margin-bottom: 24px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            }}
            img {{
                width: 100%;
                height: auto;
                border: 1px solid #ddd;
                border-radius: 8px;
            }}
        </style>
    </head>
    <body>
        <h1>Desigualdad territorial en Tenerife: renta, fuentes de ingresos y estructura laboral</h1>

        <div class="intro">
            <p>
                Este resultado forma parte de un pipeline DataOps construido con Dagster.
                La historia analiza cómo se distribuyen la renta, las fuentes de ingresos,
                la ocupación y la actividad económica en las secciones censales de la
                provincia de Santa Cruz de Tenerife entre 2021 y 2023.
            </p>

            <div class="metrics">
                <div class="metric">
                    <p>Renta media 2023</p>
                    <strong>{renta_media:,.0f} €</strong>
                </div>
                <div class="metric">
                    <p>Peso medio servicios 2023</p>
                    <strong>{servicios_medio:,.1f} %</strong>
                </div>
                <div class="metric">
                    <p>Alta cualificación media 2023</p>
                    <strong>{alta_cualificacion_media:,.1f} %</strong>
                </div>
            </div>
        </div>

        {tarjetas}
    </body>
    </html>
    """

    path = OUTPUT_DIR / "index.html"
    path.write_text(html, encoding="utf-8")

    return str(path)


# ============================================================
# Checks de calidad
# ============================================================

@asset_check(asset=raw_renta_media)
def check_raw_renta_media_no_vacia(raw_renta_media: pd.DataFrame):
    """
    Check de carga:
    el dataset de renta no puede estar vacío.
    """
    n = len(raw_renta_media)

    return AssetCheckResult(
        passed=n > 0,
        metadata={
            "filas": MetadataValue.int(n),
        },
    )


@asset_check(asset=raw_distribucion_ingresos)
def check_distribucion_ingresos_rango(
    raw_distribucion_ingresos: pd.DataFrame,
):
    """
    Check de datos:
    los porcentajes de fuentes de ingresos deben estar entre 0 y 100.
    """
    valores = raw_distribucion_ingresos["value"].dropna()

    fuera_rango = int(((valores < 0) | (valores > 100)).sum())

    return AssetCheckResult(
        passed=fuera_rango == 0,
        metadata={
            "valores_fuera_rango": MetadataValue.int(fuera_rango),
            "valor_min": MetadataValue.float(float(valores.min())),
            "valor_max": MetadataValue.float(float(valores.max())),
        },
    )


@asset_check(asset=indicadores_seccion)
def check_indicadores_sin_duplicados(
    indicadores_seccion: pd.DataFrame,
):
    """
    Check de transformación:
    no debe haber duplicados por year + section_key.
    """
    duplicados = int(
        indicadores_seccion.duplicated(["year", "section_key"]).sum()
    )

    return AssetCheckResult(
        passed=duplicados == 0,
        metadata={
            "duplicados_year_section_key": MetadataValue.int(duplicados),
            "filas": MetadataValue.int(len(indicadores_seccion)),
        },
    )


@asset_check(asset=indicadores_seccion)
def check_metricas_principales_no_nulas(
    indicadores_seccion: pd.DataFrame,
):
    """
    Check de transformación:
    las variables principales deben tener suficiente cobertura.
    """
    columnas = [
        "renta_bruta_media_persona",
        "pct_sueldos_salarios",
        "pct_alta_cualificacion",
        "pct_servicios",
    ]

    coberturas = {
        col: float(indicadores_seccion[col].notna().mean())
        for col in columnas
    }

    cobertura_minima = min(coberturas.values())

    return AssetCheckResult(
        passed=cobertura_minima >= 0.85,
        metadata={
            "cobertura_minima": MetadataValue.float(cobertura_minima),
            "coberturas": MetadataValue.json(coberturas),
        },
    )


@asset_check(asset=indicadores_seccion)
def check_cobertura_geojson_renta(
    indicadores_seccion: pd.DataFrame,
):
    """
    Check espacial:
    comprueba que la renta se puede unir correctamente con el GeoJSON
    siguiendo la regla:
    2021 -> mapa 2022
    2022 -> mapa 2023
    2023 -> mapa 2024
    """
    reglas = {
        2021: 2022,
        2022: 2023,
        2023: 2024,
    }

    coberturas = {}

    for year, anio_mapa in reglas.items():
        datos = indicadores_seccion[indicadores_seccion["year"] == year]
        geo = cargar_geojson_secciones(anio_mapa)

        cobertura = float(datos["geo_renta"].isin(geo["geocode"]).mean())
        coberturas[f"{year}_mapa_{anio_mapa}"] = cobertura

    cobertura_minima = min(coberturas.values())

    return AssetCheckResult(
        passed=cobertura_minima >= 0.95,
        metadata={
            "cobertura_minima": MetadataValue.float(cobertura_minima),
            "coberturas": MetadataValue.json(coberturas),
        },
    )


@asset_check(asset=catalogo_visualizaciones)
def check_visualizaciones_png_ok(
    catalogo_visualizaciones: pd.DataFrame,
):
    """
    Check de salida:
    todos los PNG del catálogo deben existir y tener tamaño mayor que 0.
    """
    problemas = []

    for _, row in catalogo_visualizaciones.iterrows():
        path = row["path"]

        if not comprobar_png(path):
            problemas.append(path)

    return AssetCheckResult(
        passed=len(problemas) == 0,
        metadata={
            "num_visualizaciones": MetadataValue.int(len(catalogo_visualizaciones)),
            "problemas": MetadataValue.json(problemas),
        },
    )


@asset_check(asset=pagina_html_resultados)
def check_html_resultados_existe(
    pagina_html_resultados: str,
):
    """
    Check de salida:
    verifica que la página HTML final se genera correctamente.
    """
    path = Path(pagina_html_resultados)

    existe = path.exists() and path.stat().st_size > 0

    return AssetCheckResult(
        passed=existe,
        metadata={
            "path": MetadataValue.path(path),
            "size_bytes": MetadataValue.int(path.stat().st_size if path.exists() else 0),
        },
    )


# ============================================================
# Job y sensor DataOps
# ============================================================

pipeline_completo_job = define_asset_job(
    name="pipeline_completo_job",
    selection=AssetSelection.all(),
)


@sensor(
    job=pipeline_completo_job,
    minimum_interval_seconds=30,
)
def sensor_cambios_datos(context):
    """
    Sensor DataOps:
    lanza el pipeline si detecta cambios en los CSV o GeoJSON.

    Esto automatiza el proceso:
    - si cambian los datos,
    - se regeneran tablas,
    - se regeneran gráficos,
    - se vuelven a ejecutar checks.
    """
    archivos = []

    for carpeta in [DATA_DIR, CARTO_DIR]:
        if not carpeta.exists():
            continue

        for path in sorted(carpeta.rglob("*")):
            if path.is_file() and path.suffix.lower() in [".csv", ".json"]:
                stat = path.stat()
                archivos.append(
                    f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}"
                )

    firma = hashlib.sha256("|".join(archivos).encode("utf-8")).hexdigest()

    if context.cursor != firma:
        context.update_cursor(firma)
        yield RunRequest(run_key=firma)

    else:
        yield SkipReason("No hay cambios en los datos.")


# ============================================================
# Registro de assets, checks, jobs y sensores
# ============================================================

defs = Definitions(
    assets=[
        raw_renta_media,
        raw_distribucion_ingresos,
        raw_ocupacion,
        raw_actividad_economica,
        renta_media_curated,
        distribucion_ingresos_curated,
        ocupacion_curated,
        actividad_curated,
        indicadores_seccion,
        plot_mapa_renta_2023,
        plot_mapa_cambio_renta_2021_2023,
        plot_mapa_servicios_2023,
        plot_mapa_ocupacion_alta_2023,
        plot_scatter_renta_ocupacion_2023,
        plot_fuentes_renta_evolucion,
        plot_top_municipios_renta_2023,
        catalogo_visualizaciones,
        pagina_html_resultados,
    ],
    asset_checks=[
        check_raw_renta_media_no_vacia,
        check_distribucion_ingresos_rango,
        check_indicadores_sin_duplicados,
        check_metricas_principales_no_nulas,
        check_cobertura_geojson_renta,
        check_visualizaciones_png_ok,
        check_html_resultados_existe,
    ],
    jobs=[
        pipeline_completo_job,
    ],
    sensors=[
        sensor_cambios_datos,
    ],
)