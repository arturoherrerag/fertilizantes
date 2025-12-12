# exportar_excels_a_csv.py
from pathlib import Path
import pandas as pd
import csv
import re
from conexion import engine, psycopg_conn, DB_NAME  # conexión centralizada

# === Configuración ===
BASE_DIR = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones_compilado")
SALIDA_DIR = BASE_DIR / "CSV_EXPORT"
EXTENSIONES = {".xlsx", ".xls"}
PRESERVAR_TEXTO = True
ENCODING_CSV = "utf-8-sig"  # UTF-8 con BOM (Excel-friendly)
LINE_TERMINATOR = "\n"      # Fin de línea tipo Unix (macOS)

def limpiar_nombre_archivo(nombre: str) -> str:
    nombre = nombre.strip().replace(" ", "_")
    nombre = re.sub(r"[^\w\-\.]", "", nombre, flags=re.UNICODE)
    return nombre

def exportar_excel_a_csv(path_excel: Path, salida_dir: Path) -> list[Path]:
    print(f"Procesando: {path_excel.name}")
    archivos_csv = []

    if path_excel.name.startswith("~$"):
        print(f"  - Saltado (temporal): {path_excel.name}")
        return archivos_csv

    try:
        xls = pd.ExcelFile(path_excel)
        hojas = xls.sheet_names
    except Exception as e:
        print(f"  ! Error leyendo {path_excel.name}: {e}")
        return archivos_csv

    base_nombre = limpiar_nombre_archivo(path_excel.stem)

    for hoja in hojas:
        try:
            if PRESERVAR_TEXTO:
                df = pd.read_excel(
                    path_excel,
                    sheet_name=hoja,
                    dtype=str,
                    keep_default_na=False,
                )
            else:
                df = pd.read_excel(path_excel, sheet_name=hoja)

            hoja_sufijo = limpiar_nombre_archivo(str(hoja))
            csv_name = f"{base_nombre}.csv" if len(hojas) == 1 else f"{base_nombre}__{hoja_sufijo}.csv"
            ruta_csv = salida_dir / csv_name

            df.to_csv(
                ruta_csv,
                index=False,
                encoding=ENCODING_CSV,
                quoting=csv.QUOTE_MINIMAL,
                lineterminator=LINE_TERMINATOR,  # ✅ corregido
            )

            archivos_csv.append(ruta_csv)
            print(f"  ✓ {hoja} → {ruta_csv.name}  ({len(df):,} filas, {len(df.columns)} cols)")

        except Exception as e:
            print(f"  ! Error exportando hoja '{hoja}' de {path_excel.name}: {e}")

    return archivos_csv

def main():
    SALIDA_DIR.mkdir(parents=True, exist_ok=True)

    print("=== Exportador Excel → CSV (UTF-8-BOM) ===")
    print(f"Origen : {BASE_DIR}")
    print(f"Destino: {SALIDA_DIR}")
    print("-------------------------------------------")

    total_csv = 0
    for path in sorted(BASE_DIR.iterdir()):
        if path.is_file() and path.suffix.lower() in EXTENSIONES:
            generados = exportar_excel_a_csv(path, SALIDA_DIR)
            total_csv += len(generados)

    print("-------------------------------------------")
    print(f"Listo. CSV generados: {total_csv}")
    print("Codificación: UTF-8 con BOM (compatible con Excel en Mac).")

if __name__ == "__main__":
    main()