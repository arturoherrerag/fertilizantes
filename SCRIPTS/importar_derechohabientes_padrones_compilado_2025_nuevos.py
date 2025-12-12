#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
INSERT-ONLY a public.derechohabientes_padrones_compilado_2025 desde un CSV.

Cambios:
- Alias apuntan a columnas reales: 'dap_(ton)' y 'urea_(ton)'.
- 'hect_predio' con tratamiento especial: redondeo a 2 decimales y clip al máximo
  representable por el tipo de la columna (incidencia 'clipped_max'); ya no aparece 'overflow_*'.
"""

import os, re, unicodedata
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, getcontext
from conexion import psycopg_conn

# ========= PARÁMETROS =========
CSV_PATH = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones_compilado/PARCIALES/parcial_09122025.csv"
ESQUEMA = "public"
TABLA = "derechohabientes_padrones_compilado_2025"
PK_COL = "acuse_estatal"

CHUNK_ROWS = 100_000
PAGE_SIZE  = 10_000
ENCODINGS  = ["utf-8-sig", "utf-8"]
VOLCAR_RECHAZOS = True

BOOL_TRUE = {"true","t","1","si","sí","s","y","yes"}
NON_NUMERIC_TOKENS = {"none","n/a","na","-","—",".","","null"}

getcontext().prec = 38

# ========= Normalización de encabezados =========
def _strip_accents(s: str) -> str:
    return ''.join(ch for ch in unicodedata.normalize('NFD', s) if unicodedata.category(ch) != 'Mn')

def norm_name(name: str) -> str:
    s = str(name).strip().lower()
    s = _strip_accents(s)
    s = (s.replace(" ", "_")
           .replace("/", "_").replace("\\", "_")
           .replace("(", "_").replace(")", "_")
           .replace("[", "_").replace("]", "_"))
    s = re.sub(r"_+", "_", s).strip("_")
    return s

# Mapea variantes normalizadas -> nombre EXACTO en la tabla
ALIAS_MAP_RAW = {
    # comunes
    "id_ceda": "id_ceda_agricultura",
    "id_ceda_agric": "id_ceda_agricultura",
    "id_ceda_agricultura": "id_ceda_agricultura",
    "id ceda": "id_ceda_agricultura",
    "id ceda agricultura": "id_ceda_agricultura",
    "publicacion__fecha": "fecha_de_publicacion",
    "fecha_de_publicacion": "fecha_de_publicacion",

    # DAP/UREA: apuntar a nombres REALES con paréntesis
    "dap_(ton)": "dap_(ton)",
    "dap_ton":   "dap_(ton)",
    "dap__ton":  "dap_(ton)",
    "dap_(ton)_":"dap_(ton)",

    "urea_(ton)": "urea_(ton)",
    "urea_ton":   "urea_(ton)",
    "urea__ton":  "urea_(ton)",
    "urea_(ton)_":"urea_(ton)",
}

def normalize_headers(headers):
    out = []
    for h in headers:
        n = norm_name(h)
        n = ALIAS_MAP_RAW.get(n, n)
        out.append(n)
    return out

# ========= Esquema real =========
def get_table_schema(conn) -> pd.DataFrame:
    q = """
    SELECT column_name, data_type, udt_name, ordinal_position,
           numeric_precision, numeric_scale
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s
    ORDER BY ordinal_position
    """
    return pd.read_sql(q, conn, params=(ESQUEMA, TABLA))

def classify_types(schema_df):
    # conservar exactamente como está en la BD (mayúsculas, espacios, paréntesis)
    ordered_cols = schema_df["column_name"].tolist()

    date_cols, ts_cols, num_cols, bool_cols = set(), set(), set(), set()
    numeric_specs = {}

    for _, r in schema_df.iterrows():
        c  = r["column_name"]
        dt = str(r["data_type"]).lower()
        if dt == "date":
            date_cols.add(c)
        elif "timestamp" in dt:
            ts_cols.add(c)
        elif dt == "numeric":
            num_cols.add(c)
            p, s = r.get("numeric_precision"), r.get("numeric_scale")
            if pd.notna(p) and pd.notna(s):
                numeric_specs[c] = (int(p), int(s))
        elif dt in ("bigint","integer","double precision","real","smallint","decimal"):
            num_cols.add(c)
        elif dt == "boolean":
            bool_cols.add(c)

    return ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs

# ========= Parsers =========
def to_date_series(s: pd.Series) -> pd.Series:
    s1 = s.astype(str).str.strip()
    s1 = s1.where(s1!="", pd.NA).str.split().str[0]
    iso = s1.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    d = pd.Series(pd.NaT, index=s1.index)
    if iso.any():
        d.loc[iso] = pd.to_datetime(s1[iso], errors="coerce", dayfirst=False, format="%Y-%m-%d")
    rest = ~iso
    if rest.any():
        d.loc[rest] = pd.to_datetime(s1[rest], errors="coerce", dayfirst=True)
    return d.dt.strftime("%Y-%m-%d")

def to_ts_series(s: pd.Series) -> pd.Series:
    s1 = s.astype(str).str.strip()
    s1 = s1.where(s1!="", pd.NA)
    iso = s1.str.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$", na=False)
    d = pd.Series(pd.NaT, index=s1.index)
    if iso.any():
        d.loc[iso] = pd.to_datetime(s1[iso], errors="coerce", dayfirst=False)
    rest = ~iso
    if rest.any():
        d.loc[rest] = pd.to_datetime(s1[rest], errors="coerce", dayfirst=True)
    return d.dt.strftime("%Y-%m-%d %H:%M:%S")

# ========= Números =========
def enforce_numeric_specs(df: pd.DataFrame, num_cols: set, numeric_specs: dict):
    incidencias = []

    # Limpieza base
    for c in num_cols:
        if c not in df.columns:
            continue
        s = df[c].astype(str).str.strip()
        s = s.map(lambda v: None if v.lower() in NON_NUMERIC_TOKENS else v)
        s = s.str.replace(",", ".", regex=False)
        df[c] = s

    # 1) Validación genérica (EXCLUYE 'hect_predio')
    for c, (p, s) in numeric_specs.items():
        if c not in df.columns or c == "hect_predio":
            continue
        limit = (Decimal(10) ** Decimal(p - s)) if (p is not None and s is not None) else None
        q = Decimal(f"1e-{s}") if (s is not None and s > 0) else Decimal(1)

        def fix_one(val, acuse):
            if val is None: return None
            try:
                d = Decimal(str(val))
            except (InvalidOperation, ValueError):
                incidencias.append((c, acuse, val, "no_convertible"))
                return None
            try:
                d = d.quantize(q, rounding=ROUND_HALF_UP)
            except InvalidOperation:
                incidencias.append((c, acuse, val, "error_redondeo"))
                return None
            if not d.is_finite():
                incidencias.append((c, acuse, val, "no_finito"))
                return None
            if limit is not None and abs(d) >= limit:
                incidencias.append((c, acuse, str(val), f"overflow_{p}_{s}"))
                return None
            return str(d)

        df[c] = [fix_one(v, a) for v, a in zip(df[c], df.get(PK_COL, [None]*len(df)))]

    # 2) Regla ESPECIAL para 'hect_predio': redondeo 2dp + CLIP al máximo representable (2dp)
    if "hect_predio" in df.columns:
        # Redondeo a 2 decimales
        def round2(x):
            if x is None: return None
            try:
                return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            except (InvalidOperation, ValueError):
                incidencias.append(("hect_predio", None, x, "no_convertible"))
                return None
        df["hect_predio"] = df["hect_predio"].apply(round2)

        # Calcular tope usando (p,s) reales si existen
        p_s = numeric_specs.get("hect_predio")
        int_limit = max_2dp = None
        if p_s:
            p, s = p_s
            try:
                int_limit = (Decimal(10) ** Decimal(p - s))
                max_2dp = int_limit - Decimal("0.01")
            except Exception:
                int_limit = max_2dp = None

        clipped = []
        def clip_hect(v, acuse):
            if v is None: return None
            if max_2dp is None:  # sin info de (p,s), no clippear
                return str(v)
            try:
                d = Decimal(str(v))
            except (InvalidOperation, ValueError):
                return None
            if abs(d) >= int_limit:
                d_clip = max_2dp.copy_sign(d)  # conserva signo
                clipped.append(("hect_predio", acuse, str(v), "clipped_max"))
                return str(d_clip)
            return str(d)

        df["hect_predio"] = [clip_hect(v, a) for v, a in zip(df["hect_predio"], df.get(PK_COL, [None]*len(df)))]
        incidencias.extend(clipped)

    return incidencias

# ========= Pipeline =========
def normalize_chunk(df: pd.DataFrame, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs):
    df.columns = normalize_headers(df.columns)

    # columnas faltantes -> None
    for c in ordered_cols:
        if c not in df.columns:
            df[c] = None

    # ordenar EXACTO como la tabla (incluye columnas con paréntesis)
    df = df[ordered_cols]

    # clave primaria limpia
    if PK_COL not in df.columns:
        df[PK_COL] = None
    df.loc[:, PK_COL] = df[PK_COL].astype(str).str.strip()
    df = df[df[PK_COL].notna() & (df[PK_COL] != "")]

    # fechas / timestamps
    for c in date_cols:
        df[c] = to_date_series(df[c])
    for c in ts_cols:
        df[c] = to_ts_series(df[c])

    # booleanos
    for c in bool_cols:
        df[c] = df[c].map(lambda x: (str(x).strip().lower() in BOOL_TRUE) if pd.notna(x) and str(x).strip() != "" else None)

    # numéricos
    incidencias = enforce_numeric_specs(df, num_cols, numeric_specs)

    # NaN -> None
    df = df.where(pd.notnull(df), None)

    # deduplicar por PK dentro del chunk
    df = df.drop_duplicates(subset=[PK_COL], keep="last")

    return df, incidencias

def insert_only_chunk(cur, df: pd.DataFrame, ordered_cols):
    if df.empty:
        return
    records = [tuple(row[c] for c in ordered_cols) for _, row in df.iterrows()]
    collist = ", ".join([f'"{c}"' for c in ordered_cols])  # comillas por columnas con paréntesis
    query = f'INSERT INTO "{TABLA}" ({collist}) VALUES %s ON CONFLICT ("{PK_COL}") DO NOTHING'
    extras.execute_values(cur, query, records, page_size=PAGE_SIZE)

def cargar_un_csv(path_csv: str, conn, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs):
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
    incid = []
    for chunk in reader:
        df, inc = normalize_chunk(chunk, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs)
        incid.extend(inc)
        if df.empty:
            continue
        with conn.cursor() as cur:
            insert_only_chunk(cur, df, ordered_cols)
        conn.commit()
        total += len(df)
        print(f"   +{len(df)} filas (acumulado: {total})")

    if incid:
        print(f"⚠️ {len(incid)} incidencias numéricas (p.ej. 'clipped_max' y 'no_convertible').")
        for r in incid[:20]:
            print("   incidencia:", r)
        if VOLCAR_RECHAZOS:
            out_csv = os.path.splitext(path_csv)[0] + "_rechazos_numeric.csv"
            pd.DataFrame(incid, columns=["columna","acuse_estatal","valor","motivo"]).to_csv(out_csv, index=False, encoding="utf-8-sig")
            print(f"   ⇢ Detalle completo: {out_csv}")

    print(f"✅ Listo {os.path.basename(path_csv)} (insert-only)")

def main():
    if not os.path.isfile(CSV_PATH):
        raise FileNotFoundError(f"No existe el archivo CSV: {CSV_PATH}")

    with psycopg_conn as conn:
        conn.autocommit = False
        schema_df = get_table_schema(conn)
        if schema_df.empty:
            raise RuntimeError(f"No se encontró la tabla {ESQUEMA}.{TABLA}.")
        ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs = classify_types(schema_df)
        if PK_COL not in ordered_cols:
            raise RuntimeError(f"La PK lógica '{PK_COL}' no existe en {ESQUEMA}.{TABLA}.")
        cargar_un_csv(CSV_PATH, conn, ordered_cols, date_cols, ts_cols, num_cols, bool_cols, numeric_specs)

    print(f"🚀 Carga completa (INSERT-ONLY) en {ESQUEMA}.{TABLA}")

if __name__ == "__main__":
    main()