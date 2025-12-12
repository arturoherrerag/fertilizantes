#!/usr/bin/env python3
"""
importar_directorio_csv.py
Carga inicial del directorio desde CSV:
  - Trunca la tabla
  - Lee todo el CSV (UTF-8-SIG)
  - Inserta sin validaciones de unicidad
Uso:
  $ python importar_directorio_csv.py
Requiere: conexion.py (exporta `engine`)
"""
import pandas as pd
import unicodedata
from sqlalchemy import text
from conexion import engine

# Ruta al CSV exportado de Excel, con codificación UTF-8-SIG
CSV_PATH = (
    "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
    "DIRECTORIO DE COORDINACIONES ESTATALES 2025_09062025.csv"
)

def quitar_acentos(s):
    return (unicodedata.normalize("NFKD", s)
            .encode("ascii", "ignore")
            .decode()) if isinstance(s, str) else s

def load_csv():
    # 1) Leer CSV
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", dtype=str)

    # 2) Normalizar encabezados a snake_case sin acentos
    df.columns = (
        df.columns
          .str.strip()
          .map(quitar_acentos)
          .str.lower()
          .str.replace(" ", "_")
          .str.replace(r"[^0-9a-z_]", "", regex=True)
    )

    # 3) Asegurar columnas mínimas
    req = {"estado","unidad_operativa","cargo","nombre_completo"}
    faltan = req - set(df.columns)
    if faltan:
        raise RuntimeError(f"Faltan columnas en CSV: {faltan}")

    # 4) Rellenar opcionales
    df["zona_operativa"]      = df.get("zona_operativa", "N/A").fillna("N/A")
    df["id_ceda_agricultura"] = df.get("id_ceda_agricultura", "N/A").fillna("N/A")
    df["curp"]                = df.get("curp", None)
    df["correo_electronico"]  = df.get("correo_electronico", None)
    df["telefono"]            = df.get("telefono", None)
    df["comentarios"]         = df.get("comentarios", None)
    df["fecha_actualizacion"] = pd.Timestamp.now()

    # 5) Estandarizar texto
    for c in ["estado","unidad_operativa","zona_operativa","cargo"]:
        df[c] = (df[c]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    .replace({"": "N/A"}))
    return df

def main():
    df = load_csv()
    with engine.begin() as conn:
        # A) Truncar tabla
        conn.execute(text("TRUNCATE TABLE directorio;"))
        # B) Insertar todo
        df.to_sql(
            "directorio",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000
        )
    print(f"✅ Directorio inicial cargado: {len(df):,} filas.")

if __name__ == "__main__":
    main()
