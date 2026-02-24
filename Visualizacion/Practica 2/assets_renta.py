import pandas as pd
from dagster import asset


# Cargar renta (filtrada: medida + último año)
@asset
def raw_renta():
    df = pd.read_csv("distribucion-renta-canarias.csv")

    # elegir medida concreta
    df = df[df["MEDIDAS#es"] == "Sueldos y salarios"]

    # elegir último año
    df = df[df["TIME_PERIOD_CODE"] == df["TIME_PERIOD_CODE"].max()]

    return df


# Cargar codislas
@asset
def raw_codislas():
    df = pd.read_csv("codislas.csv", sep=";", encoding="latin1")

    # Aseguramos tipos
    df["CPRO"] = df["CPRO"].astype(int)
    df["CMUN"] = df["CMUN"].astype(int)

    # Código municipio 5 dígitos: provincia(2)+mun(3)
    df["TERRITORIO_CODE"] = (
        df["CPRO"].astype(str).str.zfill(2) + df["CMUN"].astype(str).str.zfill(3)
    )

    # Nos quedamos con lo necesario
    return df[["TERRITORIO_CODE", "NOMBRE", "ISLA"]]


# Cargar niveles estudios (raw)
@asset
def raw_estudios():
    return pd.read_excel("nivelestudios.xlsx", engine="openpyxl")


# join renta + isla/nombre municipio
@asset(deps=[raw_renta, raw_codislas])
def renta_con_isla(raw_renta, raw_codislas):
    df = raw_renta.copy()
    df["TERRITORIO_CODE"] = df["TERRITORIO_CODE"].astype(str).str.zfill(5)

    out = df.merge(raw_codislas, on="TERRITORIO_CODE", how="left")
    return out


# -------------------------
# GRÁFICO 1: TOP 30 municipios
# -------------------------
@asset(deps=[renta_con_isla])
def plot_renta_top30(renta_con_isla: pd.DataFrame):

    from plotnine import ggplot, aes, geom_col, coord_flip, labs

    df = renta_con_isla.copy()

    # Renombrar primero 
    df = df.rename(columns={
        "TERRITORIO#es": "municipio",
        "OBS_VALUE": "valor",
    })

    # Quitar los que no tengan isla
    df = df[df["ISLA"].notna()].copy()

    # Top 30
    df = df.sort_values("valor", ascending=False).head(30)

    p = (
        ggplot(df, aes(x="reorder(municipio, valor)", y="valor", fill="ISLA"))
        + geom_col()
        + coord_flip()
        + labs(
            title="Canarias: Top 30 municipios (Sueldos y salarios, último año)",
            x="Municipio",
            y="Valor",
            fill="Isla",
        )
    )

    p.save("grafico_top30.png", verbose=False)
    return "grafico_top30.png"


# ---------------------------------------------------
# GRÁFICO 2: media por isla
# ---------------------------------------------------
@asset(deps=[renta_con_isla])
def plot_media_isla(renta_con_isla: pd.DataFrame):

    from plotnine import ggplot, aes, geom_col, labs

    df = renta_con_isla.copy()
    df = df.rename(columns={"OBS_VALUE": "valor"})

    df = df[df["ISLA"].notna()].copy()

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
            y="Media (valor)"
        )
    )

    p.save("grafico_media_isla.png", verbose=False)
    return "grafico_media_isla.png"


# ---------------------------------------------------
# Indicador de estudios: % Educación superior
# ---------------------------------------------------
@asset(deps=[raw_estudios])
def estudios_superior_pct(raw_estudios: pd.DataFrame) -> pd.DataFrame:
    edu = raw_estudios.copy()

    # Extraer código municipio desde la primera columna
    col0 = "Municipios de 500 habitantes o más"
    edu["TERRITORIO_CODE"] = edu[col0].astype(str).str.extract(r"^(\d{5})")[0]
    edu["TERRITORIO_CODE"] = edu["TERRITORIO_CODE"].astype("string")

    # Último periodo
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


# ---------------------------------------------------
# GRÁFICO 3: relación renta vs % superior
# ---------------------------------------------------
@asset(deps=[renta_con_isla, estudios_superior_pct])
def plot_renta_vs_estudios(renta_con_isla: pd.DataFrame, estudios_superior_pct: pd.DataFrame):

    from plotnine import ggplot, aes, geom_point, labs

    df = renta_con_isla.copy()
    df = df.rename(columns={
        "TERRITORIO#es": "municipio",
        "OBS_VALUE": "valor",
    })

    df = df.merge(estudios_superior_pct, on="TERRITORIO_CODE", how="left")
    df = df[df["ISLA"].notna()].copy()

    # Quitamos nulos de pct_superior
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

    p.save("grafico_renta_vs_estudios.png", verbose=False)
    return "grafico_renta_vs_estudios.png"
