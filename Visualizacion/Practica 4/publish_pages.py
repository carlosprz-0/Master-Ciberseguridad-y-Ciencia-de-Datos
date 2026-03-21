import shutil
import subprocess
import os

IMAGES = [
    "Visualizacion/Practica 4/plot_renta_top30.png",
    "Visualizacion/Practica 4/plot_grafico_media_isla.png",
    "Visualizacion/Practica 4/plot_grafico_renta_vs_estudios.png",
]

def run(cmd):
    subprocess.run(cmd, check=True)

def main():
    # cambiar a gh-pages
    run(["git", "checkout", "gh-pages"])

    # copiar imágenes al root
    for img in IMAGES:
        if os.path.exists(img):
            shutil.copy(img, os.path.basename(img))
        else:
            print(f"No existe: {img}")

    # commit y push
    run(["git", "add", "."])

    diff = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if diff.returncode != 0:
        run(["git", "commit", "-m", "Update graficos"])
        run(["git", "push", "origin", "gh-pages"])
        print("✔ Subido a gh-pages")
    else:
        print("Nada que subir")

    # volver a main
    run(["git", "checkout", "main"])

if __name__ == "__main__":
    main()