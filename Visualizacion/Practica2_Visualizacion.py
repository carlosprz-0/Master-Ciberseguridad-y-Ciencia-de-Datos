import pandas as pd
from dagster import asset

@asset
def raw_renta_data():
    df = pd.read_csv("distribucion-renta-canarias.csv")
    return df

@asset(deps=[raw_renta_data])
def clean_renta_data(raw_renta_data):
    df = raw_renta_data.copy()

    # Ejemplo típico de limpieza (ajústalo a tus columnas reales)
    df = df.dropna()

    # Renombrar columnas si hace falta
    df.columns = df.columns.str.strip().str.lower()

    return df

from dagster import asset_check, AssetCheckResult

@asset_check(asset=clean_renta_data)
def check_num_municipios(clean_renta_data):
    return AssetCheckResult(
        passed=len(clean_renta_data) >= 88,
        description="El dataset debe tener al menos 88 municipios"
    )

from plotnine import *
import pandas as pd

df = pd.read_csv("distribucion-renta-canarias.csv")

(
    ggplot(df, aes(x="Municipio", y="Renta_media"))
    + geom_col()
    + coord_flip()
    + theme_minimal()
)

@asset(deps=[clean_renta_data])
def renta_plot(clean_renta_data):

    from plotnine import *
    import matplotlib.pyplot as plt

    p = (
        ggplot(clean_renta_data,
               aes(x="Municipio", y="Renta_media"))
        + geom_col()
        + coord_flip()
        + theme_minimal()
    )

    p.save("renta_plot.png")

    return "renta_plot.png"