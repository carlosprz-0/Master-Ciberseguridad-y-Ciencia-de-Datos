#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera:
  - contraste_mannwhitney_total.csv
  - tabla_contraste_mannwhitney_total.tex

Requisitos:
  pip install pandas scipy numpy

Uso:
  python3 contraste_mannwhitney_total.py
"""

import os
import math
import glob
import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu


LEVELS = [512, 768, 1024]
PLATFORMS = ["PC personal", "Raspberry Pi 4"]
CIPHERS = ["ascon-aead128", "aes-ccm128"]


def parse_filename(path):
    base = os.path.basename(path).replace(".csv", "")
    name = base.replace("results_attack_", "")

    if name.endswith("_pc_personal"):
        platform = "PC personal"
        name = name[:-len("_pc_personal")]
    else:
        platform = "Raspberry Pi 4"

    cipher, level = name.rsplit("_", 1)
    return platform, cipher, int(level)


def cliffs_delta(x, y):
    """
    Cliff's delta.
    Valor positivo: ASCON tiende a tener tiempos mayores que AES.
    Valor negativo: ASCON tiende a tener tiempos menores que AES.
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    y_sorted = np.sort(y)

    less = np.searchsorted(y_sorted, x, side="left")       # y < x
    less_equal = np.searchsorted(y_sorted, x, side="right") # y <= x
    greater = len(y) - less_equal                         # y > x

    return (less.sum() - greater.sum()) / (len(x) * len(y))


def cliffs_magnitude(delta):
    ad = abs(delta)
    if ad < 0.147:
        return "despreciable"
    if ad < 0.33:
        return "pequeño"
    if ad < 0.474:
        return "moderado"
    return "grande"


def holm_bonferroni(p_values):
    """
    Corrección de Holm-Bonferroni.
    Devuelve p-valores ajustados preservando el orden original.
    """
    p_values = np.asarray(p_values, dtype=float)
    m = len(p_values)
    order = np.argsort(p_values)
    adjusted = np.empty(m, dtype=float)

    running_max = 0.0
    for rank, idx in enumerate(order):
        raw_adjusted = (m - rank) * p_values[idx]
        running_max = max(running_max, raw_adjusted)
        adjusted[idx] = min(running_max, 1.0)

    return adjusted


def fmt_num(x, decimals=6):
    return f"{x:.{decimals}f}"


def fmt_p_latex(p):
    if p < 1e-4 and p > 0:
        exp = int(math.floor(math.log10(p)))
        coeff = p / (10 ** exp)
        return f"${coeff:.2f}\\cdot 10^{{{exp}}}$"
    return f"{p:.6f}"


def load_data():
    files = glob.glob("results_attack_*.csv")
    if not files:
        raise FileNotFoundError("No se han encontrado archivos results_attack_*.csv en la carpeta actual.")

    data = {}
    for path in files:
        platform, cipher, level = parse_filename(path)
        df = pd.read_csv(path)

        required = {"iteration", "total_ms", "setup_ms", "backdoor_keygen_ms", "recovery_ms", "success"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"El archivo {path} no contiene las columnas requeridas: {missing}")

        data[(platform, cipher, level)] = df

    return data


def main():
    data = load_data()
    records = []

    for platform in PLATFORMS:
        for level in LEVELS:
            df_ascon = data[(platform, "ascon-aead128", level)]
            df_aes = data[(platform, "aes-ccm128", level)]

            ascon = df_ascon["total_ms"].to_numpy()
            aes = df_aes["total_ms"].to_numpy()

            u_stat, p_value = mannwhitneyu(ascon, aes, alternative="two-sided", method="auto")
            delta = cliffs_delta(ascon, aes)

            records.append({
                "Plataforma": platform,
                "Nivel": level,
                "N_ASCON": len(ascon),
                "N_AES": len(aes),
                "Media_ASCON_ms": float(np.mean(ascon)),
                "Media_AES_ms": float(np.mean(aes)),
                "Mediana_ASCON_ms": float(np.median(ascon)),
                "Mediana_AES_ms": float(np.median(aes)),
                "Dif_media_ms": float(np.mean(ascon) - np.mean(aes)),
                "Dif_mediana_ms": float(np.median(ascon) - np.median(aes)),
                "U": float(u_stat),
                "p_valor": float(p_value),
                "Cliffs_delta": float(delta),
                "Magnitud": cliffs_magnitude(delta),
                "Exito_ASCON": f"{int(df_ascon['success'].sum())}",
                "Exito_AES": f"{int(df_aes['success'].sum())}",
            })

    results = pd.DataFrame(records)
    results["p_Holm"] = holm_bonferroni(results["p_valor"].values)

    results = results[[
        "Plataforma", "Nivel", "N_ASCON", "N_AES",
        "Media_ASCON_ms", "Media_AES_ms",
        "Mediana_ASCON_ms", "Mediana_AES_ms",
        "Dif_media_ms", "Dif_mediana_ms",
        "U", "p_valor", "p_Holm",
        "Cliffs_delta", "Magnitud",
        "Exito_ASCON", "Exito_AES"
    ]]

    results.to_csv("contraste_mannwhitney_total.csv", index=False)

    lines = []
    lines.append(r"\begin{table}[H]")
    lines.append(r"\centering")
    lines.append(r"\scriptsize")
    lines.append(r"\resizebox{\textwidth}{!}{%")
    lines.append(r"\begin{tabular}{llrrrrrrrlll}")
    lines.append(r"\hline")
    lines.append(
        r"\textbf{Plataforma} & \textbf{Nivel} & \textbf{Iter.} & "
        r"\textbf{Media Ascon} & \textbf{Media AES} & "
        r"\textbf{Mediana Ascon} & \textbf{Mediana AES} & "
        r"\textbf{$\Delta$ medias} & \textbf{$\Delta$ medianas} & "
        r"\textbf{$p_{\mathrm{Holm}}$} & \textbf{Cliff} & \textbf{Éxito} \\"
    )
    lines.append(r"\hline")

    for _, row in results.iterrows():
        success = f"{row['Exito_ASCON']} / {row['Exito_AES']}"
        lines.append(
            f"{row['Plataforma']} & {int(row['Nivel'])} & {int(row['N_ASCON'])} & "
            f"{fmt_num(row['Media_ASCON_ms'])} & {fmt_num(row['Media_AES_ms'])} & "
            f"{fmt_num(row['Mediana_ASCON_ms'])} & {fmt_num(row['Mediana_AES_ms'])} & "
            f"{fmt_num(row['Dif_media_ms'])} & {fmt_num(row['Dif_mediana_ms'])} & "
            f"{fmt_p_latex(row['p_Holm'])} & {fmt_num(row['Cliffs_delta'], 3)} ({row['Magnitud']}) & {success} \\\\"
        )

    lines.append(r"\hline")
    lines.append(r"\end{tabular}%")
    lines.append(r"}")
    lines.append(
        r"\caption{Contraste de Mann-Whitney U entre ASCON-AEAD128 y AES-CCM128 "
        r"para la métrica T.Total, con corrección Holm-Bonferroni y tamaño del efecto "
        r"mediante Cliff's delta. Los tiempos están expresados en milisegundos.}"
    )
    lines.append(r"\label{tab:mann-whitney-total}")
    lines.append(r"\end{table}")

    with open("tabla_contraste_mannwhitney_total.tex", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("Archivos generados:")
    print(" - contraste_mannwhitney_total.csv")
    print(" - tabla_contraste_mannwhitney_total.tex")


if __name__ == "__main__":
    main()
