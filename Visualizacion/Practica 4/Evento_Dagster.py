import os
import pandas as pd
from plotnine import ggplot, aes, geom_col, coord_flip, labs, geom_point

from dagster import (
    asset,
    asset_check,
    AssetCheckResult,
    MetadataValue,
    Definitions,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    SkipReason,
)

# -------------------------
# ASSETS
# -------------------------

@asset
def raw_renta():
    df = pd.read_csv("distribucion-renta-canarias.csv")
    df = df[df["MEDIDAS#es"] == "Sueldos y salarios"]
    df = df[df["TIME_PERIOD_CODE"] == df["TIME_PERIOD_CODE"].max()]
    return df


@asset
def raw_codislas():
    df = pd.read_csv("codislas.csv", sep=";", encoding="latin1")
    df["CPRO"] = df["CPRO"].astype(int)
    df["CMUN"] = df["CMUN"].astype(int)
    df["TERRITORIO_CODE"] = (
        df["CPRO"].astype(str).str.zfill(2)
        + df["CMUN"].astype(str).str.zfill(3)
    )
    return df[["TERRITORIO_CODE", "NOMBRE", "ISLA"]]


@asset
def raw_estudios():
    return pd.read_excel("nivelestudios.xlsx", engine="openpyxl")


@asset(deps=[raw_renta, raw_codislas])
def renta_con_isla(raw_renta, raw_codislas):
    df = raw_renta.copy()
    df["TERRITORIO_CODE"] = df["TERRITORIO_CODE"].astype(str).str.zfill(5)
    out = df.merge(raw_codislas, on="TERRITORIO_CODE", how="left")
    return out


@asset(deps=[renta_con_isla])
def plot_renta_top30(renta_con_isla):
    df = renta_con_isla.copy()
    df = df.rename(columns={"TERRITORIO#es": "municipio", "OBS_VALUE": "valor"})
    df = df[df["ISLA"].notna()].copy()
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df[df["valor"].notna()].copy()
    df = df.sort_values("valor", ascending=False).head(30)

    p = (
        ggplot(df, aes(x="reorder(municipio, valor)", y="valor", fill="ISLA"))
        + geom_col()
        + coord_flip()
        + labs(
            title="Canarias: Top 30 Municipios con Mayor Sueldo y Salarios",
            x="Municipio",
            y="Valor",
            color="Isla",
        )
    )
    p.save("plot_renta_top30.png", verbose=False)
    return "plot_renta_top30.png"


@asset(deps=[renta_con_isla])
def plot_media_isla(renta_con_isla):
    df = renta_con_isla.copy()
    df = df.rename(columns={"OBS_VALUE": "valor"})
    df = df[df["ISLA"].notna()].copy()
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df[df["valor"].notna()].copy()

    resumen = (
        df.groupby("ISLA", as_index=False)["valor"]
        .mean()
        .sort_values("valor", ascending=False)
    )

    p = (
        ggplot(resumen, aes(x="reorder(ISLA, valor)", y="valor", fill="ISLA"))
        + geom_col(show_legend=False)
        + labs(
            title="Canarias: media del indicador por isla",
            x="Isla",
            y="Media (valor)",
        )
    )
    p.save("plot_grafico_media_isla.png", verbose=False)
    return "plot_grafico_media_isla.png"


@asset(deps=[raw_estudios])
def estudios_superior_pct(raw_estudios):
    edu = raw_estudios.copy()

    col0 = "Municipios de 500 habitantes o más"
    edu["TERRITORIO_CODE"] = edu[col0].astype(str).str.extract(r"^(\d{5})")[0]
    edu["TERRITORIO_CODE"] = edu["TERRITORIO_CODE"].astype("string")

    latest_period = edu["Periodo"].max()
    edu = edu[edu["Periodo"] == latest_period].copy()

    total = (
        edu[edu["Nivel de estudios en curso"] == "Total"]
        .groupby("TERRITORIO_CODE", as_index=False)["Total"]
        .sum()
        .rename(columns={"Total": "total"})
    )

    sup = (
        edu[edu["Nivel de estudios en curso"] == "Educación superior"]
        .groupby("TERRITORIO_CODE", as_index=False)["Total"]
        .sum()
        .rename(columns={"Total": "superior"})
    )

    out = total.merge(sup, on="TERRITORIO_CODE", how="left")
    out["superior"] = out["superior"].fillna(0)
    out["pct_superior"] = out["superior"] / out["total"]

    return out[["TERRITORIO_CODE", "pct_superior"]]


@asset(deps=[renta_con_isla, estudios_superior_pct])
def plot_renta_vs_estudios(renta_con_isla, estudios_superior_pct):
    df = renta_con_isla.copy()
    df = df.rename(columns={"TERRITORIO#es": "municipio", "OBS_VALUE": "valor"})
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df[df["valor"].notna()].copy()
    df = df.merge(estudios_superior_pct, on="TERRITORIO_CODE", how="left")
    df = df[df["ISLA"].notna()].copy()
    df = df[df["pct_superior"].notna()].copy()

    p = (
        ggplot(df, aes(x="pct_superior", y="valor", color="ISLA"))
        + geom_point(alpha=0.7)
        + labs(
            title="Municipios: indicador vs % Educación superior",
            x="% Educación superior (último periodo)",
            y="Valor (Sueldos y salarios)",
            color="Isla",
        )
    )

    p.save("plot_grafico_renta_vs_estudios.png", verbose=False)
    return "plot_grafico_renta_vs_estudios.png"


# -------------------------
# CHECKS
# -------------------------

@asset_check(asset=raw_renta)
def check_raw_renta_no_vacio(raw_renta):
    return AssetCheckResult(
        passed=len(raw_renta) > 0,
        metadata={"rows": MetadataValue.int(len(raw_renta))},
    )


@asset_check(asset=raw_renta)
def check_obs_value_no_nulos(raw_renta):
    n_null = int(raw_renta["OBS_VALUE"].isna().sum())
    return AssetCheckResult(
        passed=(n_null == 0),
        metadata={"null_obs_value": MetadataValue.int(n_null)},
    )


@asset_check(asset=renta_con_isla)
def check_merge_isla_cubre_mayoria(renta_con_isla):
    pct = float(renta_con_isla["ISLA"].notna().mean())
    return AssetCheckResult(
        passed=(pct >= 0.85),
        metadata={"pct_isla_no_na": MetadataValue.float(pct)},
    )


@asset_check(asset=plot_renta_top30)
def check_png_top30_existe(plot_renta_top30):
    return AssetCheckResult(
        passed=os.path.exists(plot_renta_top30),
        metadata={"path": MetadataValue.path(plot_renta_top30)},
    )


# -------------------------
# EJERCICIO 9
# -------------------------

pipeline_completo_job = define_asset_job(
    name="pipeline_completo_job",
    selection=AssetSelection.all(),
)


@sensor(job=pipeline_completo_job)
def sensor_cambios_carpeta(context):
    carpeta = "."

    estado_actual = []
    for nombre in sorted(os.listdir(carpeta)):
        ruta = os.path.join(carpeta, nombre)
        if os.path.isfile(ruta):
            estado_actual.append(f"{nombre}:{os.path.getmtime(ruta)}")

    firma_actual = "|".join(estado_actual)
    firma_anterior = context.cursor or ""

    if firma_actual != firma_anterior:
        context.update_cursor(firma_actual)
        yield RunRequest(run_key=firma_actual)
    else:
        yield SkipReason("No hay cambios en la carpeta del proyecto")


defs = Definitions(
    assets=[
        raw_renta,
        raw_codislas,
        raw_estudios,
        renta_con_isla,
        plot_renta_top30,
        plot_media_isla,
        estudios_superior_pct,
        plot_renta_vs_estudios,
    ],
    asset_checks=[
        check_raw_renta_no_vacio,
        check_obs_value_no_nulos,
        check_merge_isla_cubre_mayoria,
        check_png_top30_existe,
    ],
    jobs=[pipeline_completo_job],
    sensors=[sensor_cambios_carpeta],
)