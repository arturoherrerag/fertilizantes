#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cargador UPSERT autodirigido por el esquema real en PostgreSQL, con:
- Parseo robusto de fechas/timestamps (evita warnings).
- Validación previa de NUMERIC(p,s) para evitar overflow (redondeo y descarte si excede límite).
- UPSERT por acuse_estatal.

Requisitos:
- conexion.py debe exponer `psycopg_conn` (psycopg2.connect)
"""

import os
import re
import glob
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, getcontext
from conexion import psycopg_conn  # conexión centralizada
import unicodedata

def _strip_accents(s: str) -> str:
    """Elimina acentos/diacríticos para normalizar encabezados."""
    return ''.join(ch for ch in unicodedata.normalize('NFD', s) if unicodedata.category(ch) != 'Mn')

# ================== PARÁMETROS ==================
RUTA_BASE = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones_compilado"
TABLA = "derechohabientes_padrones_compilado_2025"
ESQUEMA = "public"  # ajusta si usas otro schema

CHUNK_ROWS = 100_000
PAGE_SIZE = 10_000
ENCODINGS = ["utf-8-sig", "utf-8"]
VOLCAR_RECHAZOS = True  # si True, guarda rechazos numéricos en CSV

# Alias de encabezados que suelen aparecer en los CSV y deben mapearse a nombres de columna reales
ALIAS_MAP_RAW = {
    "id ceda": "id_ceda_agricultura",
    "id_ceda": "id_ceda_agricultura",
    "id ceda agricultura": "id_ceda_agricultura",
    "id_ceda_agric": "id_ceda_agricultura",
    "publicacion__fecha": "fecha_de_publicacion",
    "fecha_de_publicacion": "fecha_de_publicacion",  # soporte a encabezado "Fecha de publicación"
}

# Boolean mapping
BOOL_TRUE = {"true", "t", "1", "si", "sí", "s", "y", "yes"}
BOOL_FALSE = {"false", "f", "0", "no", "n"}

# Evitar problemas de precisión con Decimal (suficiente para nuestros números)
getcontext().prec = 38

# ================== UTILIDADES ==================
def norm_name(name: str) -> str:
    """Nombre normalizado: minúsculas + sin acentos + espacios a '_'"""
    s = str(name).strip().lower()
    s = _strip_accents(s)
    # Unificar separadores comunes
    s = s.replace(" ", "_")
    s = s.replace("/", "_").replace("\\", "_")
    s = s.replace("(", "_").replace(")", "_")
    s = s.replace("[", "_").replace("]", "_")
    s = s.replace("__", "_")
    return s

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
    Retorna: column_name, data_type, udt_name, ordinal_position, numeric_precision, numeric_scale
    """
    q = """
        SELECT
            column_name,
            data_type,
            udt_name,
            ordinal_position,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    return pd.read_sql(q, conn, params=(ESQUEMA, TABLA))

def classify_types(schema_df):
    """
    Devuelve:
      - ordered_cols (lista en el orden real de la tabla)
      - date_cols, ts_cols, num_cols, bool_cols (sets)
      - numeric_specs: dict {col: (precision, scale)} SOLO para NUMERIC(p,s)
    """
    ordered_cols = schema_df["column_name"].str.lower().tolist()

    date_cols = set()
    ts_cols = set()
    num_cols = set()
    bool_cols = set()
    numeric_specs = {}

    for _, r in schema_df.iterrows():
        c = r["column_name"].lower()
        dt = str(r["data_type"]).lower()

        if dt == "date":
            date_cols.add(c)
        elif "timestamp" in dt:
            ts_cols.add(c)
        elif dt == "numeric":
            num_cols.add(c)
            p = r.get("numeric_precision")
            s = r.get("numeric_scale")
            # Algunos drivers devuelven None; tratarlos como sin límite si no viene
            if pd.notna(p) and pd.notna(s):
                numeric_specs[c] = (int(p), int(s))
        elif dt in ("bigint", "integer", "double precision", "real", "smallint", "decimal"):
            num_cols.add(c)  # tratar como numérico genérico sin (p,s)
        elif dt == "boolean":
            bool_cols.add(c)

    return ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs

# --------- Parsers de fechas/timestamps sin warnings ---------
def to_date_series(s: pd.Series) -> pd.Series:
    s1 = s.astype(str).str.strip()
    s1 = s1.where(s1 != "", pd.NA)
    # Recortar hora si viene: "14/05/2025 0:00:00" -> "14/05/2025"
    s1 = s1.str.split().str[0]

    # Detectar ISO (YYYY-MM-DD) vs DD/MM/YYYY
    iso_mask = s1.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    d = pd.Series(pd.NaT, index=s1.index)

    if iso_mask.any():
        d.loc[iso_mask] = pd.to_datetime(s1[iso_mask], errors="coerce", dayfirst=False, format="%Y-%m-%d")

    # Resto: intentar con dayfirst=True (DD/MM/YYYY)
    rest_mask = ~iso_mask
    if rest_mask.any():
        d.loc[rest_mask] = pd.to_datetime(s1[rest_mask], errors="coerce", dayfirst=True)

    return d.dt.strftime("%Y-%m-%d")

def to_ts_series(s: pd.Series) -> pd.Series:
    s1 = s.astype(str).str.strip()
    s1 = s1.where(s1 != "", pd.NA)

    # ISO completo con hora (YYYY-MM-DD HH:MM:SS) o solo fecha ISO
    iso_full = s1.str.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$", na=False)
    d = pd.Series(pd.NaT, index=s1.index)

    if iso_full.any():
        d.loc[iso_full] = pd.to_datetime(s1[iso_full], errors="coerce", dayfirst=False)

    # Resto: intentar dayfirst=True (DD/MM/YYYY [HH:MM:SS])
    rest_mask = ~iso_full
    if rest_mask.any():
        d.loc[rest_mask] = pd.to_datetime(s1[rest_mask], errors="coerce", dayfirst=True)

    return d.dt.strftime("%Y-%m-%d %H:%M:%S")

# --------- Manejo de NUMERIC(p,s) para evitar overflow ---------
def _to_decimal(x):
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

def enforce_numeric_specs(df: pd.DataFrame, num_cols: set, numeric_specs: dict):
    """
    Normaliza numéricos y aplica (p,s) cuando se conocen:
      - Reemplaza coma por punto; vacíos -> None
      - Redondea a 's' decimales (ROUND_HALF_UP)
      - Si |valor| >= 10^(p-s) -> None y registra incidencia
    Devuelve lista de incidencias
    """
    incidencias = []

    # Normalizar coma a punto y vacíos->None
    for c in num_cols:
        if c not in df.columns:
            continue
        s = df[c].astype(str).str.replace(",", ".", regex=False)
        s = s.where(s.str.strip().ne(""), None)
        df[c] = s

    for c, (p, s) in numeric_specs.items():
        if c not in df.columns:
            continue
        # Límite absoluto permitido para la parte entera
        try:
            limit = (Decimal(10) ** Decimal(p - s))
        except Exception:
            # Si no se puede calcular por algún motivo, omitir validación estricta
            limit = None
        # Cuantía para redondeo: 1e-<s>
        q = Decimal(f"1e-{s}") if s > 0 else Decimal(1)

        def fix_one(val, acuse):
            if val is None:
                return None
            v = str(val)
            try:
                d = Decimal(v)
            except (InvalidOperation, ValueError):
                incidencias.append((c, acuse, val, "no_convertible"))
                return None
            # Redondeo a s decimales
            try:
                d = d.quantize(q, rounding=ROUND_HALF_UP)
            except InvalidOperation:
                incidencias.append((c, acuse, val, "error_redondeo"))
                return None
            # Descartar NaN/Inf
            if not d.is_finite():
                incidencias.append((c, acuse, val, "no_finito"))
                return None
            # Validar parte entera si hay límite
            if limit is not None:
                try:
                    if abs(d) >= limit:
                        incidencias.append((c, acuse, str(val), f"overflow_{p}_{s}"))
                        return None
                except InvalidOperation:
                    incidencias.append((c, acuse, val, "comparacion_invalida"))
                    return None
            return str(d)

        df[c] = [fix_one(v, a) for v, a in zip(df[c], df.get("acuse_estatal", [None]*len(df)))]

    return incidencias

# ================== PIPELINE ==================
def normalize_chunk(df: pd.DataFrame, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs) -> (pd.DataFrame, list):
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

    # Booleanos
    for c in bool_cols:
        df[c] = df[c].map(lambda x: (str(x).strip().lower() in BOOL_TRUE) if pd.notna(x) and str(x).strip() != "" else None)

    # Numéricos (incluye BIGINT/INTEGER/NUMERIC). Validación estricta para NUMERIC(p,s)
    incidencias = enforce_numeric_specs(df, num_cols, numeric_specs)

    # NaN -> None
    df = df.where(pd.notnull(df), None)

    # Deduplicar por PK dentro del chunk (última gana)
    df = df.drop_duplicates(subset=["acuse_estatal"], keep="last")

    return df, incidencias

def upsert_chunk(cur, df: pd.DataFrame, ordered_cols):
    if df.empty:
        return
    records = [tuple(row[c] for c in ordered_cols) for _, row in df.iterrows()]
    collist = ", ".join([f'"{c}"' for c in ordered_cols])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in ordered_cols if c != "acuse_estatal"])
    query = f'INSERT INTO "{TABLA}" ({collist}) VALUES %s ON CONFLICT ("acuse_estatal") DO UPDATE SET {set_clause}'
    extras.execute_values(cur, query, records, page_size=PAGE_SIZE)

def cargar_archivo(path_csv: str, conn, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs):
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
    rechazos = []
    for chunk in reader:
        df, inc = normalize_chunk(chunk, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs)
        rechazos.extend(inc)
        if df.empty:
            continue
        with conn.cursor() as cur:
            upsert_chunk(cur, df, ordered_cols)
        conn.commit()
        total += len(df)
        print(f"   +{len(df)} (acumulado: {total})")

    if rechazos:
        print(f"⚠️ {len(rechazos)} valores numéricos descartados por no caber / no convertir.")
        # imprime algunas muestras
        for r in rechazos[:20]:
            print("   rechazo:", r)
        if VOLCAR_RECHAZOS:
            out_csv = os.path.join(RUTA_BASE, "rechazos_numeric.csv")
            pd.DataFrame(rechazos, columns=["columna", "acuse_estatal", "valor", "motivo"]).to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"   ⇢ Detalle completo en: {out_csv}")

    print(f"✅ Listo {os.path.basename(path_csv)} ({total} filas)")

def main():
    # 1) Enumerar CSVs a cargar
    archivos = [p for p in glob.glob(os.path.join(RUTA_BASE, "*.csv"))
                if "_perfil" not in p
                and not os.path.basename(p).lower().startswith(("cruce_", "resumen_", "esquema_detectado", "rechazos_numeric"))]
    if not archivos:
        print("⚠️ No se encontraron CSV para cargar.")
        return

    with psycopg_conn as conn:
        conn.autocommit = False

        # 2) Leer esquema real de la tabla
        schema_df = get_table_schema(conn)
        if schema_df.empty:
            raise RuntimeError(f"No se encontró la tabla {ESQUEMA}.{TABLA} en PostgreSQL.")

        ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs = classify_types(schema_df)

        # 3) Cargar uno por uno
        for path in sorted(archivos):
            cargar_archivo(path, conn, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs)

    print(f"🚀 Carga completa en {ESQUEMA}.{TABLA}")

if __name__ == "__main__":
    main()