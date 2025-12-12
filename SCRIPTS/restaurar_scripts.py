# separar_scripts_desde_acumulado.py
# Uso:
#   1) Guarda este archivo (por ejemplo, en tu Escritorio).
#   2) Ejecuta:  python3 separar_scripts_desde_acumulado.py
#      (asegúrate de ajustar la ruta de "ruta_acumulado" si está en otro lugar).
# Qué hace:
#   - Lee "acumulado_scripts.txt"
#   - Cada bloque comienza con una línea '---' y después una línea '# Nombre.py'
#   - Escribe cada script en una carpeta de salida sin sobreescribir accidentalmente
#   - Genera un CSV resumen con los archivos exportados

from pathlib import Path
import re
import csv
import sys
from datetime import datetime

# === Ajusta estas rutas si lo deseas ===
ruta_acumulado = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/acumulado_scripts.txt")
carpeta_salida = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS_RESTAURADOS")

# === No tocar desde aquí ===
def main():
    if not ruta_acumulado.exists():
        print(f"[ERROR] No encuentro el archivo: {ruta_acumulado}")
        print("Asegúrate de que la ruta sea correcta o mueve el archivo a esa ubicación.")
        sys.exit(1)

    carpeta_salida.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Leyendo: {ruta_acumulado}")
    texto = ruta_acumulado.read_text(encoding="utf-8", errors="ignore")
    lineas = texto.splitlines()

    scripts_exportados = []
    i = 0
    nombre_actual = None
    buffer = []

    def flush():
        nonlocal nombre_actual, buffer, scripts_exportados
        if nombre_actual and buffer:
            # Limpia blancos iniciales y arma contenido final
            while buffer and buffer[0].strip() == "":
                buffer.pop(0)
            contenido = ("\n".join(buffer)).rstrip() + "\n"

            # Evita sobreescribir: si existe, agrega sufijo con timestamp
            destino = carpeta_salida / nombre_actual
            if destino.exists():
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                destino = carpeta_salida / f"{destino.stem}__rec_{stamp}{destino.suffix}"

            destino.write_text(contenido, encoding="utf-8")
            scripts_exportados.append({
                "script": nombre_actual,
                "ruta": str(destino),
                "bytes": len(contenido.encode("utf-8"))
            })
        nombre_actual = None
        buffer = []

    while i < len(lineas):
        linea = lineas[i]

        # Detecta inicio de bloque de script: '---' seguido de '# Algo.py'
        if linea.strip() == "---" and i + 1 < len(lineas):
            # Cierra el anterior si lo había
            flush()

            encabezado = lineas[i + 1].strip()
            m = re.match(r"^#\s+(.+\.py)\s*$", encabezado)
            if m:
                nombre_actual = m.group(1).strip()
                buffer = []
                i += 2
                # Salta un posible blanco tras encabezado
                if i < len(lineas) and lineas[i].strip() == "":
                    i += 1
                continue
            else:
                # No es un encabezado de script, trata '---' como texto normal
                if nombre_actual is not None:
                    buffer.append(linea)
                i += 1
                continue
        else:
            if nombre_actual is not None:
                buffer.append(linea)
            i += 1

    # Último bloque
    flush()

    # CSV resumen
    csv_path = carpeta_salida / "resumen_scripts_exportados.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["script", "ruta", "bytes"])
        w.writeheader()
        for r in scripts_exportados:
            w.writerow(r)

    print(f"[OK] Exportados {len(scripts_exportados)} scripts a: {carpeta_salida}")
    print(f"[OK] Resumen: {csv_path}")
    for r in scripts_exportados[:10]:
        print(f"  - {r['script']}  ->  {r['ruta']} ({r['bytes']} bytes)")
    if len(scripts_exportados) > 10:
        print(f"  ... y {len(scripts_exportados)-10} más.")

if __name__ == "__main__":
    main()