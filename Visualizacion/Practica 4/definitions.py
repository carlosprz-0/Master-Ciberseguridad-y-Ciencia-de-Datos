from dagster import Definitions
from assets_renta import (
    raw_renta, raw_codislas, raw_estudios,
    renta_con_isla,
    plot_renta_top30, plot_media_isla,
    estudios_superior_pct, plot_renta_vs_estudios,

    # checks
    check_raw_renta_no_vacio,
    check_obs_value_no_nulos,
    check_merge_isla_cubre_mayoria,
    check_png_top30_existe
)

defs = Definitions(
    assets=[
        raw_renta, raw_codislas, raw_estudios,
        renta_con_isla,
        plot_renta_top30, plot_media_isla,
        estudios_superior_pct, plot_renta_vs_estudios
    ],
    asset_checks=[
        check_raw_renta_no_vacio,
        check_obs_value_no_nulos,
        check_merge_isla_cubre_mayoria,
        check_png_top30_existe
    ]
)

