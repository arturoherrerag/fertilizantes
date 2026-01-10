import os
import glob
import unicodedata
import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.types import Text, Integer, Numeric, Date
from conexion import engine, DB_NAME


# =============================================================================
# UTILIDADES
# =============================================================================
def remover_acentos(cadena):
    if not isinstance(cadena, str):
        return cadena
    nfkd_form = unicodedata.normalize("NFD", cadena)
    return nfkd_form.encode("ASCII", "ignore").decode("utf-8", "ignore")


def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    # Caso típico de encoding raro
    if "estatus_de_recepciÃ³n" in df.columns:
        df.rename(columns={"estatus_de_recepciÃ³n": "estatus_de_recepción"}, inplace=True)

    # Normalizar nombres
    df.columns = [
        remover_acentos(col).lower().strip().replace(" ", "_")
        for col in df.columns
    ]

    # Error común
    if "estatus_de_recepcian" in df.columns:
        df["estatus_de_recepcion"] = df["estatus_de_recepcian"]
        df.drop(columns=["estatus_de_recepcian"], inplace=True)

    return df


def limpiar_vacios(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    return s.replace(
        {"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA, "NULL": pd.NA, "0000-00-00": pd.NA}
    )


def parse_fecha_mex(s: pd.Series) -> pd.Series:
    """
    Parseo robusto (México day-first) para formatos mixtos:
    - YYYY-MM-DD
    - DD/MM/YY, DD/MM/YYYY
    - DD-MM-YY, DD-MM-YYYY
    """
    s = limpiar_vacios(s)

    formatos = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d-%m-%Y",
        "%d-%m-%y",
        "%m/%d/%Y",  # por si llega algo en US (raro)
        "%m/%d/%y",
    ]

    out = pd.Series([pd.NaT] * len(s), index=s.index, dtype="datetime64[ns]")
    for fmt in formatos:
        tmp = pd.to_datetime(s, format=fmt, errors="coerce")
        out = out.fillna(tmp)

    return out.dt.date


def to_numeric_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    x = df[col].astype(str).str.strip()
    x = x.replace({"": np.nan, "nan": np.nan, "NaN": np.nan, "None": np.nan, "NULL": np.nan})
    x = x.str.replace(",", "", regex=False)
    df[col] = pd.to_numeric(x, errors="coerce")


def to_int_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    x = df[col].astype(str).str.strip()
    x = x.replace({"": np.nan, "nan": np.nan, "NaN": np.nan, "None": np.nan, "NULL": np.nan})
    df[col] = pd.to_numeric(x, errors="coerce").astype("Int64")


# =============================================================================
# 1) RUTAS
# =============================================================================
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"

# OJO: conservo tu patrón para no romper tu flujo actual
archivo_remanentes = glob.glob(os.path.join(ruta_base, "*TRANSFERENCIAS-NACIONAL-ANUAL_2025_rem.csv"))[0]
archivo_eliminar = os.path.join(ruta_base, "eliminar_remanentes.xlsx")
archivo_corregidos = os.path.join(ruta_base, "remanentes_corregido.csv")

table_name = "remanentes"

print(f"▶️ Ejecutando: importar_remanentes.py...")
print(f"📄 Archivo base: {os.path.basename(archivo_remanentes)}")


# =============================================================================
# 2) LECTURA + NORMALIZACIÓN
# =============================================================================
remanentes_df = pd.read_csv(archivo_remanentes, dtype=str, encoding="utf-8")
remanentes_df = normalizar_columnas(remanentes_df)

if "folio_transferencia" in remanentes_df.columns:
    remanentes_df["folio_transferencia"] = remanentes_df["folio_transferencia"].astype(str).str.strip()

print(f"✅ Leídos {len(remanentes_df):,} registros del archivo base.")


# =============================================================================
# 3) ELIMINAR REGISTROS (si aplica)
# =============================================================================
if os.path.exists(archivo_eliminar):
    eliminar_df = pd.read_excel(archivo_eliminar, dtype=str)
    eliminar_df = normalizar_columnas(eliminar_df)

    if "folio_transferencia" in eliminar_df.columns and "folio_transferencia" in remanentes_df.columns:
        eliminar_df["folio_transferencia"] = eliminar_df["folio_transferencia"].astype(str).str.strip()

        antes = len(remanentes_df)
        remanentes_df = remanentes_df[~remanentes_df["folio_transferencia"].isin(eliminar_df["folio_transferencia"])]
        eliminados = antes - len(remanentes_df)
        print(f"🧹 Eliminados {eliminados} registros por 'eliminar_remanentes.xlsx'.")
    else:
        print("⚠️ 'eliminar_remanentes.xlsx' no trae 'folio_transferencia' (se omite eliminación).")
else:
    print("ℹ️ Sin 'eliminar_remanentes.xlsx' (se omite eliminación).")


# =============================================================================
# 4) AGREGAR CORREGIDOS (si se repite folio, gana corregido)
# =============================================================================
if os.path.exists(archivo_corregidos):
    corregidos_df = pd.read_csv(archivo_corregidos, dtype=str, encoding="utf-8-sig")
    corregidos_df = normalizar_columnas(corregidos_df)

    if "folio_transferencia" in corregidos_df.columns:
        corregidos_df["folio_transferencia"] = corregidos_df["folio_transferencia"].astype(str).str.strip()

    if "folio_transferencia" in remanentes_df.columns and "folio_transferencia" in corregidos_df.columns:
        folios_corregidos = set(corregidos_df["folio_transferencia"].dropna().tolist())

        antes = len(remanentes_df)
        remanentes_df = remanentes_df[~remanentes_df["folio_transferencia"].isin(folios_corregidos)]
        remanentes_df = pd.concat([remanentes_df, corregidos_df], ignore_index=True)

        print(f"➕ Integradas {len(folios_corregidos)} correcciones desde 'remanentes_corregido.csv'.")
        print(f"   Registros: {antes:,} → {len(remanentes_df):,}")
    else:
        antes = len(remanentes_df)
        remanentes_df = pd.concat([remanentes_df, corregidos_df], ignore_index=True)
        print(f"➕ Añadidos {len(remanentes_df) - antes} registros corregidos (sin llave folio_transferencia).")
else:
    print("ℹ️ Sin 'remanentes_corregido.csv' (se omite corrección).")


# =============================================================================
# 5) PARSEO DE FECHAS (sin warning)
# =============================================================================
for col in ["fecha_de_salida", "fecha_de_llegada"]:
    if col in remanentes_df.columns:
        remanentes_df[col] = parse_fecha_mex(remanentes_df[col])

# =============================================================================
# 6) LIMPIEZA NUMÉRICA
# =============================================================================
to_numeric_col(remanentes_df, "toneladas_iniciales")
to_numeric_col(remanentes_df, "toneladas_en_el_destino")
to_int_col(remanentes_df, "bultos_iniciales")
to_int_col(remanentes_df, "bultos_en_el_destino")


# =============================================================================
# 7) ASIGNAR id_ceda_agricultura SEGÚN ESTATUS
# =============================================================================
if "id_ceda_agricultura" not in remanentes_df.columns:
    remanentes_df["id_ceda_agricultura"] = None

if "estatus" in remanentes_df.columns:
    est = remanentes_df["estatus"].astype(str).str.strip().str.lower()

    if "cdf_destino_final" in remanentes_df.columns:
        mask_entregada = est == "entregada"
        remanentes_df.loc[mask_entregada, "id_ceda_agricultura"] = remanentes_df.loc[mask_entregada, "cdf_destino_final"]

    if "cdf_destino_original" in remanentes_df.columns:
        mask_no_entregada = est != "entregada"
        remanentes_df.loc[mask_no_entregada, "id_ceda_agricultura"] = remanentes_df.loc[mask_no_entregada, "cdf_destino_original"]


# =============================================================================
# 8) VALIDACIÓN PREVIA (para evitar truncar si algo viene mal)
# =============================================================================
# Si tu tabla en BD exige NOT NULL en alguna fecha, deja aquí las que aplique.
# Por seguridad mínima, validamos que lo que sí exista no quede sin parsear.
validar_fechas = [c for c in ["fecha_de_salida", "fecha_de_llegada"] if c in remanentes_df.columns]
for c in validar_fechas:
    n_null = int(remanentes_df[c].isna().sum())
    if n_null > 0:
        print(f"\n❌ VALIDACIÓN FALLIDA: {n_null} filas con '{c}' inválida (no parseada). Ejemplos:")
        cols_show = [x for x in ["folio_transferencia", "fecha_de_salida", "fecha_de_llegada"] if x in remanentes_df.columns]
        print(remanentes_df[remanentes_df[c].isna()][cols_show].head(25).to_string(index=False))
        raise SystemExit("Deteniendo importación para evitar truncar e insertar datos inválidos.")

print(f"🧾 Registros finales a insertar: {len(remanentes_df):,}")


# =============================================================================
# 9) DTYPE PARA INSERCIÓN
# =============================================================================
dtype_sqlalchemy = {
    "folio_transferencia": Text,
    "cdf_origen": Text,
    "estatus": Text,
    "producto": Text,
    "abreviacion_producto": Text,
    "toneladas_iniciales": Numeric(10, 3),
    "toneladas_en_el_destino": Numeric(10, 3),
    "fecha_de_salida": Date,
    "cdf_destino_original": Text,
    "cdf_destino_final": Text,
    "fecha_de_llegada": Date,
    "estatus_de_recepcion": Text,
    "descripcion": Text,
    "destino_final": Text,
    "nombre_operador": Text,
    "telefono_operador": Text,
    "placas_transporte": Text,
    "tipo_transporte": Text,
    "bultos_iniciales": Integer,
    "numero_ticket_bascula": Text,
    "bultos_en_el_destino": Integer,
    "id_ceda_agricultura": Text,
}


# =============================================================================
# 10) TRUNCATE + INSERT
# =============================================================================
print(f"\n🗑 Truncando tabla '{table_name}'...")
with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE {table_name};"))

print(f"⬆️ Insertando {len(remanentes_df):,} registros en '{table_name}'...")
remanentes_df.to_sql(
    name=table_name,
    con=engine,
    if_exists="append",
    index=False,
    dtype=dtype_sqlalchemy,
    method="multi",
    chunksize=5000,
)

print(f"✅ Tabla '{table_name}' actualizada en la base '{DB_NAME}'.")