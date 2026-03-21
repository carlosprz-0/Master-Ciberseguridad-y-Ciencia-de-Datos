import geopandas as gpd
import pandas as pd
from plotnine import (
    ggplot, aes, geom_polygon, coord_equal, theme_void,
    scale_fill_cmap, labs, theme, element_text
)

# Leer GeoJSON
gdf = gpd.read_file("Municipios-2024.json")

# Elegimos una variable numérica del GeoJSON.
# Ejemplo: tpar_t (tasa de paro total) o tsal_t
variable_mapa = "tpar_t"

# Convertimos multipolígonos/polígonos a una tabla de vértices
gdf = gdf.explode(index_parts=False).reset_index(drop=True)

filas = []
for idx, row in gdf.iterrows():
    geom = row.geometry
    if geom.geom_type == "Polygon":
        x, y = geom.exterior.coords.xy
        for xi, yi in zip(x, y):
            filas.append({
                "long": xi,
                "lat": yi,
                "group": idx,
                "municipio": row["label"],
                "valor": row[variable_mapa]
            })

df_plot = pd.DataFrame(filas)

plot = (
    ggplot(df_plot, aes(x="long", y="lat", group="group", fill="valor"))
    + geom_polygon(color="white", size=0.2)
    + coord_equal()
    + scale_fill_cmap(name=variable_mapa, cmap_name="viridis")
    + theme_void()
    + labs(
        title="Mapa municipal de Canarias",
        subtitle=f"Variable representada: {variable_mapa}",
        fill=variable_mapa
    )
    + theme(
        plot_title=element_text(size=14, weight="bold"),
        plot_subtitle=element_text(size=10)
    )
)

plot.save("mapa_municipios.png", width=10, height=8, dpi=120)