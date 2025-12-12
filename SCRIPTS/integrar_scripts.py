import os
from pathlib import Path

# Ruta donde están los scripts
carpeta_scripts = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS")
archivo_salida = carpeta_scripts / "acumulado_scripts.txt"

# Obtener todos los archivos .py ordenados alfabéticamente
archivos_py = sorted([f for f in carpeta_scripts.iterdir() if f.suffix == ".py"])

with open(archivo_salida, "w", encoding="utf-8") as salida:
    for archivo in archivos_py:
        salida.write(f"# ===== ARCHIVO: {archivo.name} =====\n\n")
        with open(archivo, "r", encoding="utf-8") as f:
            salida.write(f.read())
        salida.write("\n\n" + "#" * 80 + "\n\n")

print(f"Archivo generado: {archivo_salida}")
