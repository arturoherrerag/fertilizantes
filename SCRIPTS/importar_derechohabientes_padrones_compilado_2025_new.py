#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Cargador UPSERT autodirigido por el esquema real en PostgreSQL, con:
- TRUNCATE opcional antes de cargar (activado por defecto).
- Staging por archivo (TEMP TABLE con nombre único) para inserción masiva eficiente.
- Parseo robusto de fechas/timestamps.
- Validación NUMERIC(p,s) con ROUND_HALF_UP y descarte por overflow.
- Validación de longitud para VARCHAR/CHAR; truncado seguro (no para PK).
- Preserva ceros a la izquierda (lee CSV como texto).
- UPSERT por acuse_estatal (requiere índice único en esa columna).
- Resumen de carga por archivo y log unificado de incidencias.

Requisitos:
- conexion.py debe exponer `psycopg_conn` (psycopg2.connect) y `engine` (SQLAlchemy Engine).
"""

import os
import re
import glob
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, getcontext
from conexion import psycopg_conn, engine  # conexión centralizada
import unicodedata
from datetime import datetime, timezone
from uuid import uuid4  # ← para nombres únicos en staging

# ================== PARÁMETROS ==================
RUTA_BASE = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones_compilado"
TABLA = "derechohabientes_padrones_compilado_2025"
ESQUEMA = "public"  # ajusta si usas otro schema

CHUNK_ROWS = 100_000      # tamaño de chunk al leer CSV
PAGE_SIZE = 10_000        # tamaño de página para execute_values
ENCODINGS = ["utf-8-sig", "utf-8"]
USE_STAGING = True        # usar tabla temporal por archivo para inserción masiva
TRUNCATE_BEFORE_LOAD = True  # vaciar la tabla antes de cargar
VOLCAR_INCIDENCIAS = True     # guardar incidencias (numéricas y texto) en CSV unificado

# Alias de encabezados que suelen aparecer en los CSV y deben mapearse a nombres de columna reales
ALIAS_MAP_RAW = {
    "id ceda": "id_ceda_agricultura",
    "id_ceda": "id_ceda_agricultura",
    "id ceda agricultura": "id_ceda_agricultura",
    "id_ceda_agric": "id_ceda_agricultura",
    "publicacion__fecha": "fecha_de_publicacion",
    "fecha_de_publicacion": "fecha_de_publicacion",

    # Variantes defensivas por si el normalizador viejo dejó estos nombres
    "dap_ton_": "dap_(ton)",
    "urea_ton_": "urea_(ton)",
}

# Boolean mapping
BOOL_TRUE = {"true", "t", "1", "si", "sí", "s", "y", "yes"}
BOOL_FALSE = {"false", "f", "0", "no", "n"}

# Precisión Decimal suficiente
getcontext().prec = 38

# ================== UTILIDADES ==================
def _strip_accents(s: str) -> str:
    """Elimina acentos/diacríticos para normalizar encabezados."""
    return ''.join(ch for ch in unicodedata.normalize('NFD', s) if unicodedata.category(ch) != 'Mn')

def norm_name(name: str) -> str:
    """Nombre normalizado: minúsculas + sin acentos + separadores a '_' (conserva paréntesis)."""
    s = str(name).strip().lower()
    s = _strip_accents(s)
    # espacios y separadores típicos -> "_"
    s = s.replace(" ", "_")
    s = s.replace("/", "_").replace("\\", "_")
    # OJO: NO tocamos "(" ni ")" para que "DAP (ton)" -> "dap_(ton)"
    s = s.replace("[", "_").replace("]", "_")
    s = re.sub(r"__+", "_", s)
    return s

def normalize_headers(headers):
    """Normaliza encabezados y aplica alias conocidos."""
    out = []
    for h in headers:
        n = norm_name(h)
        n = ALIAS_MAP_RAW.get(n, n)
        out.append(n)
    return out

def get_table_schema(conn) -> pd.DataFrame:
    """
    Obtiene columnas y tipos de la tabla (en orden).
    Retorna: column_name, data_type, udt_name, ordinal_position,
             numeric_precision, numeric_scale, character_maximum_length
    """
    q = """
        SELECT
            column_name,
            data_type,
            udt_name,
            ordinal_position,
            numeric_precision,
            numeric_scale,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    # Usa SQLAlchemy engine para evitar warning de pandas
    return pd.read_sql(q, engine, params=(ESQUEMA, TABLA))

def classify_types(schema_df):
    """
    Devuelve:
      - ordered_cols_exact (lista exacta de columnas)
      - date_cols, ts_cols, num_cols, bool_cols (sets en minúsculas)
      - numeric_specs: dict {col_lower: (precision, scale)} para NUMERIC(p,s)
      - text_specs: dict {col_lower: max_len} para VARCHAR/CHAR con límite
    """
    ordered_cols_exact = schema_df["column_name"].tolist()

    date_cols = set()
    ts_cols = set()
    num_cols = set()
    bool_cols = set()
    numeric_specs = {}
    text_specs = {}

    for _, r in schema_df.iterrows():
        c_exact = r["column_name"]
        c_lower = c_exact.lower()
        dt = str(r["data_type"]).lower()

        if dt == "date":
            date_cols.add(c_lower)
        elif "timestamp" in dt:
            ts_cols.add(c_lower)
        elif dt == "numeric":
            num_cols.add(c_lower)
            p = r.get("numeric_precision")
            s = r.get("numeric_scale")
            if pd.notna(p) and pd.notna(s):
                numeric_specs[c_lower] = (int(p), int(s))
        elif dt in ("bigint", "integer", "double precision", "real", "smallint", "decimal"):
            num_cols.add(c_lower)
        elif dt == "boolean":
            bool_cols.add(c_lower)
        elif dt in ("character varying", "character"):
            maxlen = r.get("character_maximum_length")
            if pd.notna(maxlen):
                text_specs[c_lower] = int(maxlen)

    return ordered_cols_exact, date_cols, ts_cols, num_cols, bool_cols, numeric_specs, text_specs

# --------- Parsers de fechas/timestamps ---------
def to_date_series(s: pd.Series) -> pd.Series:
    s1 = s.astype(str).str.strip()
    s1 = s1.where(s1 != "", pd.NA)
    s1 = s1.str.split().str[0]  # corta hora si viene
    iso_mask = s1.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    d = pd.Series(pd.NaT, index=s1.index)
    if iso_mask.any():
        d.loc[iso_mask] = pd.to_datetime(s1[iso_mask], errors="coerce", dayfirst=False, format="%Y-%m-%d")
    rest_mask = ~iso_mask
    if rest_mask.any():
        d.loc[rest_mask] = pd.to_datetime(s1[rest_mask], errors="coerce", dayfirst=True)
    return d.dt.strftime("%Y-%m-%d")

def to_ts_series(s: pd.Series) -> pd.Series:
    s1 = s.astype(str).str.strip()
    s1 = s1.where(s1 != "", pd.NA)
    iso_full = s1.str.match(r"^\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}(:\d{2})?)?$", na=False)
    d = pd.Series(pd.NaT, index=s1.index)
    if iso_full.any():
        d.loc[iso_full] = pd.to_datetime(s1[iso_full], errors="coerce", dayfirst=False)
    rest_mask = ~iso_full
    if rest_mask.any():
        d.loc[rest_mask] = pd.to_datetime(s1[rest_mask], errors="coerce", dayfirst=True)
    return d.dt.strftime("%Y-%m-%d %H:%M:%S")

# --------- Validaciones NUMERIC(p,s) y TEXTO ----------
def enforce_numeric_specs(df: pd.DataFrame,
                          ordered_cols_exact,
                          num_cols_lower: set,
                          numeric_specs: dict):
    """
    Normaliza numéricos y aplica (p,s) cuando se conocen:
      - Reemplaza coma por punto; vacíos -> None
      - Redondea a 's' decimales (ROUND_HALF_UP)
      - Si |valor| >= 10^(p-s) -> None y registra incidencia
    Devuelve lista de incidencias (tipo, columna, acuse, valor, motivo)
    """
    incidencias = []
    lower_map = {c.lower(): c for c in ordered_cols_exact}

    # Normalización robusta previa (evita "None"/"nan" como texto)
    def _clean_num(x):
        if x is None:
            return None
        sx = str(x).strip()
        if sx == "" or sx.lower() in {"none", "nan"}:
            return None
        return sx.replace(",", ".")

    # Normalizar coma->punto en todas las columnas numéricas
    for lc in num_cols_lower:
        c = lower_map.get(lc, None)
        if c and c in df.columns:
            df[c] = df[c].map(_clean_num)

    # Aplicar restricciones NUMERIC(p,s)
    for lc, (p, s) in numeric_specs.items():
        c = lower_map.get(lc, None)
        if not c or c not in df.columns:
            continue

        try:
            limit = (Decimal(10) ** Decimal(p - s))
        except Exception:
            limit = None

        q = Decimal(f"1e-{s}") if s > 0 else Decimal(1)

        pk = "acuse_estatal" if "acuse_estatal" in df.columns else next((col for col in df.columns if col.lower()=="acuse_estatal"), None)
        acuses = df[pk] if pk else [None]*len(df)

        def fix_one(val, acuse):
            if val is None:
                return None
            v = str(val)
            try:
                d = Decimal(v)
            except (InvalidOperation, ValueError):
                incidencias.append(("numeric", c, acuse, val, "no_convertible"))
                return None
            # Redondear
            try:
                d = d.quantize(q, rounding=ROUND_HALF_UP)
            except InvalidOperation:
                incidencias.append(("numeric", c, acuse, val, "error_redondeo"))
                return None
            if not d.is_finite():
                incidencias.append(("numeric", c, acuse, val, "no_finito"))
                return None
            if limit is not None:
                try:
                    if abs(d) >= limit:
                        incidencias.append(("numeric", c, acuse, str(val), f"overflow_{p}_{s}"))
                        return None
                except InvalidOperation:
                    incidencias.append(("numeric", c, acuse, val, "comparacion_invalida"))
                    return None
            return str(d)

        df[c] = [fix_one(v, a) for v, a in zip(df[c], acuses)]

    return incidencias

def enforce_text_specs(df: pd.DataFrame,
                       ordered_cols_exact,
                       text_specs: dict,
                       pk_lower: str = "acuse_estatal"):
    """
    Aplica límites de longitud para VARCHAR/CHAR:
      - Si el texto excede max_len:
          * Si es la PK (acuse_estatal): marcar como None (fila se descartará).
          * Si no es PK: truncar y registrar incidencia.
    Devuelve lista de incidencias (tipo, columna, acuse, valor_original, motivo).
    """
    incidencias = []
    lower_map = {c.lower(): c for c in ordered_cols_exact}
    pk_exact = lower_map.get(pk_lower, pk_lower)

    acuse_series = df[pk_exact] if pk_exact in df.columns else pd.Series([None] * len(df), index=df.index)

    for lc, maxlen in text_specs.items():
        c = lower_map.get(lc)
        if not c or c not in df.columns:
            continue

        col = df[c].astype(str)

        def fix_text(x, acuse):
            if x is None:
                return None
            sx = str(x).strip()
            if sx == "":
                return None
            if len(sx) <= maxlen:
                return sx
            if c == pk_exact:
                incidencias.append(("text", c, acuse, sx, f"pk_excede_varchar_{maxlen}"))
                return None  # invalidamos la PK para descartar la fila
            else:
                incidencias.append(("text", c, acuse, sx, f"truncado_varchar_{maxlen}"))
                return sx[:maxlen]

        df[c] = [fix_text(v, a) for v, a in zip(col, acuse_series)]

    return incidencias

# ================== PIPELINE ==================
def normalize_chunk(df: pd.DataFrame,
                    ordered_cols_exact,
                    date_cols, ts_cols, num_cols, bool_cols, numeric_specs, text_specs) -> (pd.DataFrame, list):
    """Normaliza columnas, ordena según esquema exacto y aplica parsers/validaciones."""
    # 1) Normaliza encabezados del CSV
    normalized = normalize_headers(df.columns)

    # 2) Mapa normalizado -> nombre EXACTO del esquema (si existe)
    schema_lower_to_exact = {c.lower(): c for c in ordered_cols_exact}
    norm_to_exact = {}
    for n in normalized:
        if n.lower() in schema_lower_to_exact:
            norm_to_exact[n] = schema_lower_to_exact[n.lower()]

    # 3) Renombra y filtra sólo columnas que existen en el esquema
    df.columns = normalized
    df = df[[c for c in df.columns if c in norm_to_exact]]
    df = df.rename(columns=norm_to_exact)

    # 4) Asegura todas las columnas y orden exacto
    for c in ordered_cols_exact:
        if c not in df.columns:
            df[c] = None
    df = df[ordered_cols_exact]

    lower_map = {c.lower(): c for c in ordered_cols_exact}

    # 5) Fechas/Timestamps
    for lc in date_cols:
        c = lower_map.get(lc, lc)
        if c in df.columns:
            df[c] = to_date_series(df[c])
    for lc in ts_cols:
        c = lower_map.get(lc, lc)
        if c in df.columns:
            df[c] = to_ts_series(df[c])

    # 6) Booleanos
    for lc in bool_cols:
        c = lower_map.get(lc, lc)
        if c in df.columns:
            df[c] = df[c].map(lambda x: (str(x).strip().lower() in BOOL_TRUE) if pd.notna(x) and str(x).strip() != "" else None)

    # 7) Numéricos y 8) Textos
    incidencias = []
    incidencias.extend(enforce_numeric_specs(df, ordered_cols_exact, num_cols, numeric_specs))
    incidencias.extend(enforce_text_specs(df, ordered_cols_exact, text_specs, pk_lower="acuse_estatal"))

    # 9) NaN -> None
    df = df.where(pd.notnull(df), None)

    # 10) PK y deduplicado
    pk_exact = lower_map.get("acuse_estatal", "acuse_estatal")
    if pk_exact not in df.columns:
        df[pk_exact] = None
    df = df[df[pk_exact].notna() & (df[pk_exact] != "")]
    df = df.drop_duplicates(subset=[pk_exact], keep="last")

    return df, incidencias

def upsert_execute_values(cur, df: pd.DataFrame, ordered_cols_exact):
    """UPSERT directo con execute_values (sin staging)."""
    if df.empty:
        return 0
    records = [tuple(row[c] for c in ordered_cols_exact) for _, row in df.iterrows()]
    collist = ", ".join([f'"{c}"' for c in ordered_cols_exact])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in ordered_cols_exact if c.lower() != "acuse_estatal"])
    query = f'INSERT INTO "{ESQUEMA}"."{TABLA}" ({collist}) VALUES %s ON CONFLICT ("acuse_estatal") DO UPDATE SET {set_clause}'
    extras.execute_values(cur, query, records, page_size=PAGE_SIZE)
    return len(records)

def ensure_temp_stage(cur, ordered_cols_exact, base_hint: str = ""):
    """Crea una tabla temporal stage con nombre único y misma estructura que la tabla destino (sin datos)."""
    suffix = uuid4().hex[:8]
    safe_hint = re.sub(r"[^a-zA-Z0-9_]", "_", base_hint)[:20] if base_hint else ""
    stage_name = f"stage_{TABLA}_{safe_hint}_{os.getpid()}_{suffix}" if safe_hint else f"stage_{TABLA}_{os.getpid()}_{suffix}"
    cur.execute(f'CREATE TEMP TABLE "{stage_name}" AS SELECT * FROM "{ESQUEMA}"."{TABLA}" LIMIT 0;')
    return stage_name

def bulk_insert_to_stage(cur, stage_name: str, df: pd.DataFrame, ordered_cols_exact):
    """Inserta en la tabla temporal por bloques con execute_values."""
    if df.empty:
        return 0
    records = [tuple(row[c] for c in ordered_cols_exact) for _, row in df.iterrows()]
    collist = ", ".join([f'"{c}"' for c in ordered_cols_exact])
    query = f'INSERT INTO "{stage_name}" ({collist}) VALUES %s'
    extras.execute_values(cur, query, records, page_size=PAGE_SIZE)
    return len(records)

def merge_from_stage(cur, stage_name: str, ordered_cols_exact):
    """Inserta/actualiza desde stage a destino con ON CONFLICT."""
    collist = ", ".join([f'"{c}"' for c in ordered_cols_exact])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in ordered_cols_exact if c.lower() != "acuse_estatal"])
    query = f'''
        INSERT INTO "{ESQUEMA}"."{TABLA}" ({collist})
        SELECT {collist} FROM "{stage_name}"
        ON CONFLICT ("acuse_estatal") DO UPDATE
        SET {set_clause};
    '''
    cur.execute(query)

def cargar_archivo(path_csv: str, conn, ordered_cols_exact, date_cols, ts_cols, num_cols, bool_cols, numeric_specs, text_specs):
    """Carga un archivo CSV (en chunks) y retorna (total_cargadas_consideradas, total_incidencias)."""
    print(f"➡️ Procesando {os.path.basename(path_csv)}")
    reader = None
    for enc in ENCODINGS:
        try:
            # sep=None autodetecta delimitador (coma/;|etc)
            reader = pd.read_csv(path_csv, dtype=str, chunksize=CHUNK_ROWS, encoding=enc, sep=None, engine="python", low_memory=True)
            break
        except UnicodeDecodeError:
            continue
    if reader is None:
        print(f"❌ No se pudo leer {path_csv} (encodings probados: {ENCODINGS})")
        return 0, 0

    total = 0
    incidencias_total = []

    with conn.cursor() as cur:
        if USE_STAGING:
            stage_name = ensure_temp_stage(cur, ordered_cols_exact, base_hint=os.path.basename(path_csv).split(".")[0])
            conn.commit()

        for chunk in reader:
            df, incid = normalize_chunk(chunk, ordered_cols_exact, date_cols, ts_cols, num_cols, bool_cols, numeric_specs, text_specs)
            incidencias_total.extend(incid)
            if df.empty:
                continue

            if USE_STAGING:
                added = bulk_insert_to_stage(cur, stage_name, df, ordered_cols_exact)
            else:
                added = upsert_execute_values(cur, df, ordered_cols_exact)

            total += added
            conn.commit()
            print(f"   +{len(df)} normalizadas (acumulado: {total})")

        if USE_STAGING:
            merge_from_stage(cur, stage_name, ordered_cols_exact)
            conn.commit()
            # Limpieza explícita de la tabla temporal para no chocar con el siguiente archivo
            with conn.cursor() as cur2:
                cur2.execute(f'DROP TABLE IF EXISTS "{stage_name}";')
            conn.commit()

    # Volcar incidencias (si hay)
    if incidencias_total and VOLCAR_INCIDENCIAS:
        out_csv = os.path.join(RUTA_BASE, "rechazos_detalle.csv")
        mode = "a" if os.path.exists(out_csv) else "w"
        header = not os.path.exists(out_csv)
        # columnas: tipo, columna, acuse_estatal, valor, motivo
        pd.DataFrame(incidencias_total, columns=["tipo", "columna", "acuse_estatal", "valor", "motivo"]).to_csv(
            out_csv, index=False, encoding="utf-8-sig", mode=mode, header=header
        )
        print(f"   ⇢ Incidencias registradas en: {out_csv} (total: {len(incidencias_total)})")

    print(f"✅ Listo {os.path.basename(path_csv)} ({total} filas consideradas en inserción)")
    return total, len(incidencias_total)

def main():
    # 1) Enumerar CSVs a cargar (evita archivos auxiliares)
    archivos = [p for p in glob.glob(os.path.join(RUTA_BASE, "*.csv"))
                if "_perfil" not in p
                and not os.path.basename(p).lower().startswith((
                    "cruce_", "resumen_", "esquema_detectado", "rechazos_numeric", "rechazos_detalle", "resumen_carga"
                ))]
    if not archivos:
        print("⚠️ No se encontraron CSV para cargar.")
        return

    resumen = []  # acumulador por archivo

    with psycopg_conn as conn:
        conn.autocommit = False

        # 2) Leer esquema real de la tabla (vía SQLAlchemy para evitar warning)
        schema_df = get_table_schema(conn)
        if schema_df.empty:
            raise RuntimeError(f"No se encontró la tabla {ESQUEMA}.{TABLA} en PostgreSQL.")

        ordered_cols_exact, date_cols, ts_cols, num_cols, bool_cols, numeric_specs, text_specs = classify_types(schema_df)

        # 🧹 (Opcional) Limpieza previa
        if TRUNCATE_BEFORE_LOAD:
            print(f"⚠️ Ejecutando TRUNCATE en {ESQUEMA}.{TABLA} ...")
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE "{ESQUEMA}"."{TABLA}" RESTART IDENTITY;')
            conn.commit()
            print("   → Tabla vaciada correctamente.\n")

        # 3) Cargar uno por uno
        for path in sorted(archivos):
            start = datetime.now(timezone.utc)
            total, cant_incid = cargar_archivo(
                path, conn,
                ordered_cols_exact, date_cols, ts_cols, num_cols, bool_cols, numeric_specs, text_specs
            )
            end = datetime.now(timezone.utc)
            resumen.append({
                "archivo": os.path.basename(path),
                "filas_cargadas_consideradas": total,
                "incidencias_registradas": cant_incid,
                "inicio_utc": start.strftime("%Y-%m-%d %H:%M:%S"),
                "fin_utc": end.strftime("%Y-%m-%d %H:%M:%S"),
                "duracion_s": round((end - start).total_seconds(), 3),
                "modo_staging": USE_STAGING,
                "truncate_previo": TRUNCATE_BEFORE_LOAD,
            })

    # 4) Volcar resumen
    if resumen:
        out_resumen = os.path.join(RUTA_BASE, "resumen_carga.csv")
        pd.DataFrame(resumen).to_csv(out_resumen, index=False, encoding="utf-8-sig")
        print(f"🧾 Resumen de carga: {out_resumen}")

    print(f"🚀 Carga completa en {ESQUEMA}.{TABLA}")

if __name__ == "__main__":
    main()