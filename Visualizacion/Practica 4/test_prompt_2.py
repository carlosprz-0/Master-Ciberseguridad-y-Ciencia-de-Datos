import os
import requests
from pathlib import Path
from dagster import asset


@asset
def system_content():
    return """Eres un experto en Python, análisis de datos, visualización y Dagster.
Generas código Python limpio, ejecutable y listo para integrarse en un pipeline Dagster.
Tu salida debe contener únicamente código Python válido.
No expliques nada.
No uses markdown.
No envuelvas la respuesta en bloques de código.
"""


@asset
def codigo_base_assets():
    return Path("assets_renta.py").read_text(encoding="utf-8")


@asset(deps=[codigo_base_assets])
def user_content(codigo_base_assets):
    return f"""
Tu tarea es generar código Python completo para un pipeline Dagster que cree visualizaciones y asset checks.

Debes basarte en este código existente del proyecto y mantener nomenclatura, estilo y estructura compatibles:

CODIGO_EXISTENTE:
{codigo_base_assets}

OBJETIVO

A partir de ese código, genera una versión que permita:
1. Mantener los assets de datos
2. Generar automáticamente el código de los gráficos
3. Generar automáticamente asset_checks para validar datos y salidas

Los gráficos objetivo son:
- plot_renta_top30
- plot_media_isla
- plot_renta_vs_estudios

Los checks deben cubrir:
- datos no vacíos
- nulos en columnas clave
- cobertura del merge por ISLA
- rango válido de pct_superior
- existencia de PNG
- PNG no vacío
- tamaño mínimo de datos transformados
- validación de valor positivo en el scatter

REQUISITOS
- Usa exclusivamente pandas, os, dagster y plotnine
- Usa asset, asset_check, AssetCheckResult y MetadataValue
- Devuelve solo código Python
- No expliques nada
"""


@asset(deps=[system_content, user_content])
def prompt_messages(system_content, user_content):
    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content},
    ]


@asset(deps=[prompt_messages])
def payload_prompt(prompt_messages):
    return {
        "model": "ollama/llama3.1:8b",
        "messages": prompt_messages,
        "temperature": 0,
        "stream": False,
    }


@asset(deps=[payload_prompt])
def codigo_generado_ia(payload_prompt):

    response = requests.post(
        "http://gpu1.esit.ull.es:4000/v1/chat/completions",
        headers={
            "Content-Type": "application/json",
        },
        json=payload_prompt,
        timeout=300,
    )

    response.raise_for_status()
    data = response.json()

    contenido = data["choices"][0]["message"]["content"]

    with open("codigo_rentas_generado.py", "w", encoding="utf-8") as f:
        f.write(contenido)

    return contenido