import re
from pathlib import Path
from dagster import asset, asset_check, AssetCheckResult, MetadataValue

# -------------------------------------------------------------------
# Asset auxiliar: lee el contenido de test_prompt.py
# -------------------------------------------------------------------

@asset
def test_prompt_source():
    ruta = Path("test_prompt.py")
    contenido = ruta.read_text(encoding="utf-8")
    return {
        "ruta": str(ruta),
        "contenido": contenido,
    }


# -------------------------------------------------------------------
# CHECK 1: comprobar que el archivo existe y no está vacío
# -------------------------------------------------------------------

@asset_check(asset=test_prompt_source)
def check_test_prompt_existe_y_no_vacio(test_prompt_source):
    ruta = test_prompt_source["ruta"]
    contenido = test_prompt_source["contenido"]

    passed = bool(contenido.strip())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": ruta,
            "longitud_caracteres": MetadataValue.int(len(contenido)),
            "mensaje": "Comprueba que test_prompt.py existe y contiene código."
        }
    )


# -------------------------------------------------------------------
# CHECK 2: validación básica de sintaxis/estructura textual
# Nota: no usa import para que siga funcionando aunque haya errores de sintaxis
# -------------------------------------------------------------------

@asset_check(asset=test_prompt_source)
def check_test_prompt_estructura_basica(test_prompt_source):
    contenido = test_prompt_source["contenido"]

    patrones_obligatorios = {
        "asset_islas_raw": r"@asset\s+def\s+islas_raw\s*\(",
        "asset_template_ia": r"@asset\s+def\s+template_ia\s*\(",
        "asset_codigo_generado_ia": r"@asset\s+def\s+codigo_generado_ia\s*\(",
        "asset_visualizacion_png": r"@asset\s+def\s+visualizacion_png\s*\(",
    }

    resultados = {
        nombre: bool(re.search(patron, contenido, re.MULTILINE | re.DOTALL))
        for nombre, patron in patrones_obligatorios.items()
    }

    passed = all(resultados.values())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "checks_detectados": MetadataValue.json(resultados),
            "mensaje": "Verifica que estén definidos los assets principales del pipeline."
        }
    )


# -------------------------------------------------------------------
# CHECK 3: el payload de template_ia tiene la estructura mínima esperada
# -------------------------------------------------------------------

@asset_check(asset=test_prompt_source)
def check_template_ia_payload_esperado(test_prompt_source):
    contenido = test_prompt_source["contenido"]

    validaciones = {
        "usa_model": '"model"' in contenido,
        "usa_messages": '"messages"' in contenido,
        "usa_temperature": '"temperature"' in contenido,
        "usa_stream": '"stream"' in contenido,
        "incluye_system_role": '"role": "system"' in contenido,
        "incluye_user_role": '"role": "user"' in contenido,
        "incluye_generar_plot": "def generar_plot(df):" in contenido,
    }

    passed = all(validaciones.values())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "validaciones": MetadataValue.json(validaciones),
            "mensaje": "Comprueba que el payload del prompt tenga la estructura mínima para invocar al modelo."
        }
    )


# -------------------------------------------------------------------
# CHECK 4: el prompt describe correctamente el gráfico esperado
# -------------------------------------------------------------------

@asset_check(asset=test_prompt_source)
def check_prompt_describe_grafico_objetivo(test_prompt_source):
    contenido = test_prompt_source["contenido"]

    validaciones = {
        "mapea_año_x": "Variable 'año' mapeada al eje X" in contenido,
        "mapea_valor_y": "Variable 'valor' mapeada al eje Y" in contenido,
        "usa_geom_line": "geom_line" in contenido,
        "resalta_tenerife": "Tenerife" in contenido,
        "usa_scale_color_manual": "scale_color_manual" in contenido,
        "titulo_grafico": "Evolución del Gasto por Isla" in contenido,
    }

    passed = all(validaciones.values())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "validaciones": MetadataValue.json(validaciones),
            "mensaje": "Comprueba que la especificación del gráfico esté bien descrita en el prompt."
        }
    )


# -------------------------------------------------------------------
# CHECK 5: detección de errores probables en el código
# -------------------------------------------------------------------

@asset_check(asset=test_prompt_source)
def check_test_prompt_sin_errores_tipicos(test_prompt_source):
    contenido = test_prompt_source["contenido"]

    errores_detectados = {}

    # Error probable: se usa 'lineas' pero no está definida
    errores_detectados["variable_lineas_no_definida"] = (
        "for l in lineas:" in contenido and "lineas =" not in contenido
    )

    # Error probable: bloque de descripción tiene paréntesis extra en string multilínea
    errores_detectados["parentesis_sospechoso_en_descripcion"] = ")\n    \"\"\"" in contenido

    # Señal de que se hace push automático desde el asset
    errores_detectados["git_push_en_asset"] = 'subprocess.run(["git", "push"])' in contenido

    passed = not any(errores_detectados.values())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "errores_detectados": MetadataValue.json(errores_detectados),
            "mensaje": (
                "Detecta errores frecuentes: variables no definidas, cierre sospechoso del bloque "
                "de descripción y side effects como git push dentro del asset."
            )
        }
    )


# -------------------------------------------------------------------
# CHECK 6: el asset visualizacion_png genera un PNG
# -------------------------------------------------------------------

@asset_check(asset=test_prompt_source)
def check_visualizacion_guarda_png(test_prompt_source):
    contenido = test_prompt_source["contenido"]

    validaciones = {
        "define_nombre_png": ".png" in contenido,
        "usa_save_plotnine": ".save(" in contenido,
        "retorna_ruta_archivo": "ruta_archivo" in contenido,
    }

    passed = all(validaciones.values())

    return AssetCheckResult(
        passed=passed,
        metadata={
            "validaciones": MetadataValue.json(validaciones),
            "mensaje": "Comprueba que el asset de visualización guarda el gráfico como PNG y devuelve la ruta."
        }
    )