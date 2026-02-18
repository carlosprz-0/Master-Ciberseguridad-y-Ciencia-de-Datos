from dagster import Definitions
from assets_renta import (
    raw_renta, raw_codislas, raw_estudios,
    renta_con_isla,
    plot_renta_top30, plot_media_isla,
    estudios_superior_pct, plot_renta_vs_estudios
)

defs = Definitions(assets=[
    raw_renta, raw_codislas, raw_estudios,
    renta_con_isla,
    plot_renta_top30, plot_media_isla,
    estudios_superior_pct, plot_renta_vs_estudios
])
