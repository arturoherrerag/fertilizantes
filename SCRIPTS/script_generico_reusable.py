#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cargador UPSERT autodirigido por el esquema real en PostgreSQL.

- NO usa archivos de esquema; obtiene columnas y orden desde information_schema.columns
- Normaliza encabezados de los CSV: minúsculas + '_' y alias comunes
- Convierte DATE/TIMESTAMP/NUMERIC/BOOLEAN según tipos reales de la tabla
- UPSERT por acuse_estatal

Requisitos:
- conexion.py debe exponer `psycopg_conn` (psycopg2.connect)
"""

import os
import re
import glob
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from conexion import psycopg_conn  # conexión centralizada

# ================== PARÁMETROS ==================
RUTA_BASE = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones_compilado"
TABLA = "derechohabientes_padrones_compilado_2025"
ESQUEMA = "public"  # ajusta si usas otro schema

CHUNK_ROWS = 100_000
PAGE_SIZE = 10_000
ENCODINGS = ["utf-8-sig", "utf-8"]

# Alias de encabezados que suelen aparecer en los CSV y deben mapearse a nombres de columna reales
ALIAS_MAP_RAW = {
    "id ceda": "id_ceda_agricultura",
    "id_ceda": "id_ceda_agricultura",
    "id ceda agricultura": "id_ceda_agricultura",
    "id_ceda_agric": "id_ceda_agricultura",
    "publicacion__fecha": "fecha_de_publicacion",
}

# Boolean mapping
BOOL_TRUE = {"true", "t", "1", "si", "sí", "s", "y", "yes"}
BOOL_FALSE = {"false", "f", "0", "no", "n"}

# ================== UTILIDADES ==================
def norm_name(name: str) -> str:
    """Nombre normalizado: minúsculas + espacios a '_'."""
    return str(name).strip().lower().replace(" ", "_")

def normalize_headers(headers):
    """Normaliza y mapea alias conocidos."""
    out = []
    for h in headers:
        n = norm_name(h)
        n = ALIAS_MAP_RAW.get(n, n)
        out.append(n)
    return out

def get_table_schema(conn) -> pd.DataFrame:
    """
    Obtiene columnas y tipos de la tabla (en orden por ordinal_position).
    Retorna DataFrame con columnas: column_name, data_type, udt_name, ordinal_position.
    """
    q = """
        SELECT
            column_name,
            data_type,
            udt_name,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    return pd.read_sql(q, conn, params=(ESQUEMA, TABLA))

def classify_types(schema_df):
    """
    A partir de los tipos reales de la tabla, construye sets por tipo lógico.
    Devuelve:
      - ordered_cols (lista en el orden real de la tabla)
      - date_cols, ts_cols, num_cols, big_cols, bool_cols (sets)
    """
    ordered_cols = schema_df["column_name"].str.lower().tolist()

    # information_schema.data_type/udt_name pueden variar entre 'numeric', 'bigint', 'timestamp without time zone', etc.
    date_cols = set()
    ts_cols = set()
    num_cols = set()
    big_cols = set()
    bool_cols = set()

    for _, r in schema_df.iterrows():
        c = r["column_name"].lower()
        dt = str(r["data_type"]).lower()
        udt = str(r["udt_name"]).lower()

        if "date" == dt:
            date_cols.add(c)
        elif "timestamp" in dt:
            ts_cols.add(c)
        elif dt == "numeric":
            num_cols.add(c)
        elif dt in ("bigint", "integer", "double precision", "real", "smallint"):
            big_cols.add(c)
        elif dt == "boolean":
            bool_cols.add(c)
        # VARCHAR/TEXT se tratarán como texto, no conversiones

    return ordered_cols, date_cols, ts_cols, (num_cols | big_cols), bool_cols

def to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")

def to_ts_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d %H:%M:%S")

def normalize_chunk(df: pd.DataFrame, ordered_cols, date_cols, ts_cols, num_cols, bool_cols) -> pd.DataFrame:
    # Normalizar encabezados y aplicar alias
    df.columns = normalize_headers(df.columns)

    # Asegurar todas las columnas de la tabla (faltantes -> None)
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = None

    # Reducir y ordenar EXACTAMENTE como la tabla
    df = df[ordered_cols]

    # Clave primaria
    if "acuse_estatal" not in df.columns:
        df["acuse_estatal"] = None
    df["acuse_estatal"] = df["acuse_estatal"].astype(str).str.strip()
    df = df[df["acuse_estatal"].notna() & (df["acuse_estatal"] != "")]

    # Fechas
    for c in date_cols:
        df[c] = to_date_series(df[c])

    # Timestamps
    for c in ts_cols:
        df[c] = to_ts_series(df[c])

    # Numéricos: convertir coma decimal -> punto; vacío -> None
    for c in num_cols:
        s = df[c].astype(str).str.replace(",", ".", regex=False)
        s = s.where(s.str.strip().ne(""), None)
        df[c] = s

    # Booleanos
    for c in bool_cols:
        df[c] = df[c].map(lambda x: (str(x).strip().lower() in BOOL_TRUE) if pd.notna(x) and str(x).strip() != "" else None)

    # NaN -> None
    df = df.where(pd.notnull(df), None)

    # Deduplicar por PK dentro del chunk (última gana)
    df = df.drop_duplicates(subset=["acuse_estatal"], keep="last")

    return df

def upsert_chunk(cur, df: pd.DataFrame, ordered_cols):
    if df.empty:
        return
    records = [tuple(df[c] for c in ordered_cols) for _, df in df.iterrows()]
    collist = ", ".join([f'"{c}"' for c in ordered_cols])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in ordered_cols if c != "acuse_estatal"])
    query = f'INSERT INTO "{TABLA}" ({collist}) VALUES %s ON CONFLICT ("acuse_estatal") DO UPDATE SET {set_clause}'
    extras.execute_values(cur, query, records, page_size=PAGE_SIZE)

def cargar_archivo(path_csv: str, conn, ordered_cols, date_cols, ts_cols, num_cols, bool_cols):
    print(f"➡️ Procesando {os.path.basename(path_csv)}")
    reader = None
    for enc in ENCODINGS:
        try:
            reader = pd.read_csv(path_csv, dtype=str, chunksize=CHUNK_ROWS, encoding=enc, sep=None, engine="python", low_memory=True)
            break
        except UnicodeDecodeError:
            continue
    if reader is None:
        print(f"❌ No se pudo leer {path_csv} (encodings probados: {ENCODINGS})")
        return

    total = 0
    for chunk in reader:
        df = normalize_chunk(chunk, ordered_cols, date_cols, ts_cols, num_cols, bool_cols)
        if df.empty:
            continue
        with conn.cursor() as cur:
            upsert_chunk(cur, df, ordered_cols)
        conn.commit()
        total += len(df)
        print(f"   +{len(df)} (acumulado: {total})")
    print(f"✅ Listo {os.path.basename(path_csv)} ({total} filas)")

def main():
    # 1) Enumerar CSVs a cargar
    archivos = [p for p in glob.glob(os.path.join(RUTA_BASE, "*.csv"))
                if "_perfil" not in p
                and not os.path.basename(p).lower().startswith(("cruce_", "resumen_", "esquema_detectado"))]
    if not archivos:
        print("⚠️ No se encontraron CSV para cargar.")
        return

    with psycopg_conn as conn:
        conn.autocommit = False

        # 2) Leer esquema real de la tabla
        schema_df = get_table_schema(conn)
        if schema_df.empty:
            raise RuntimeError(f"No se encontró la tabla {ESQUEMA}.{TABLA} en PostgreSQL.")

        ordered_cols, date_cols, ts_cols, num_cols, bool_cols = classify_types(schema_df)

        # 3) Cargar uno por uno
        for path in sorted(archivos):
            cargar_archivo(path, conn, ordered_cols, date_cols, ts_cols, num_cols, bool_cols)

    print(f"🚀 Carga completa en {ESQUEMA}.{TABLA}")

if __name__ == "__main__":
    main()