import os
import glob
import unicodedata
import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.types import Text, Integer, Numeric, Date
from conexion import engine  # Conexión centralizada

# =============================================================================
# FUNCIONES UTILITARIAS
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

    # Error común: estatus_de_recepcian
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
    Parseo robusto para México (day-first), soporta formatos mixtos típicos:
    - SIGAP: YYYY-MM-DD
    - Correcciones/Excel: DD/MM/YY, DD/MM/YYYY, DD-MM-YYYY, etc.
    """
    s = limpiar_vacios(s)

    formatos = [
        "%Y-%m-%d",  # 2025-10-10
        "%Y/%m/%d",  # 2025/10/10
        "%d/%m/%Y",  # 10/10/2025
        "%d/%m/%y",  # 10/10/25
        "%d-%m-%Y",  # 10-10-2025
        "%d-%m-%y",  # 10-10-25
        "%m/%d/%Y",  # por si acaso viene US (raro)
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
# 1) RUTAS DE ARCHIVOS
# =============================================================================
ruta_base = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/"
archivo_transferencias = glob.glob(
    os.path.join(ruta_base, "*-FERTILIZANTES-TRANSFERENCIAS-NACIONAL-ANUAL_*.CSV")
)[0]
archivo_eliminar = os.path.join(ruta_base, "eliminar_transferencias.xlsx")
archivo_corregidos = os.path.join(ruta_base, "transferencias_corregido.csv")


# =============================================================================
# 2) LECTURA SIGAP
# =============================================================================
transferencias_df = pd.read_csv(archivo_transferencias, dtype=str, encoding="utf-8")
transferencias_df = normalizar_columnas(transferencias_df)

# Asegurar clave
if "folio_transferencia" in transferencias_df.columns:
    transferencias_df["folio_transferencia"] = transferencias_df["folio_transferencia"].astype(str).str.strip()


# =============================================================================
# 3) ELIMINAR REGISTROS (si aplica)
# =============================================================================
if os.path.exists(archivo_eliminar):
    eliminar_df = pd.read_excel(archivo_eliminar, dtype=str)
    eliminar_df = normalizar_columnas(eliminar_df)

    if "folio_transferencia" in eliminar_df.columns and "folio_transferencia" in transferencias_df.columns:
        eliminar_df["folio_transferencia"] = eliminar_df["folio_transferencia"].astype(str).str.strip()
        antes = len(transferencias_df)
        transferencias_df = transferencias_df[
            ~transferencias_df["folio_transferencia"].isin(eliminar_df["folio_transferencia"])
        ]
        print(f"✅ Se eliminaron {antes - len(transferencias_df)} registros con 'eliminar_transferencias.xlsx'.")


# =============================================================================
# 4) AGREGAR CORREGIDOS (y si se repite folio, gana el corregido)
# =============================================================================
if os.path.exists(archivo_corregidos):
    # utf-8-sig por si viene con BOM desde Excel
    corregidos_df = pd.read_csv(archivo_corregidos, dtype=str, encoding="utf-8-sig")
    corregidos_df = normalizar_columnas(corregidos_df)

    if "folio_transferencia" in corregidos_df.columns:
        corregidos_df["folio_transferencia"] = corregidos_df["folio_transferencia"].astype(str).str.strip()

    if "folio_transferencia" in transferencias_df.columns and "folio_transferencia" in corregidos_df.columns:
        folios_corregidos = set(corregidos_df["folio_transferencia"].dropna().tolist())
        antes = len(transferencias_df)

        # Quitar del SIGAP cualquier folio que venga corregido (si existiera)
        transferencias_df = transferencias_df[~transferencias_df["folio_transferencia"].isin(folios_corregidos)]

        # Concatenar
        transferencias_df = pd.concat([transferencias_df, corregidos_df], ignore_index=True)
        print(f"✅ Se añadieron {len(folios_corregidos)} folios desde 'transferencias_corregido.csv'. (SIGAP quedó en {antes} → {len(transferencias_df)})")
    else:
        # Si por alguna razón no trae folio, lo agregamos sin reemplazo
        antes = len(transferencias_df)
        transferencias_df = pd.concat([transferencias_df, corregidos_df], ignore_index=True)
        print(f"✅ Se añadieron {len(transferencias_df) - antes} registros corregidos (sin llave folio_transferencia).")


# =============================================================================
# 5) PARSEO DE FECHAS (México DD/MM/YY y DD/MM/YYYY, etc.)
# =============================================================================
for col in ["fecha_de_salida", "fecha_de_llegada"]:
    if col in transferencias_df.columns:
        transferencias_df[col] = parse_fecha_mex(transferencias_df[col])

# =============================================================================
# 6) LIMPIEZA NUMÉRICA (evitar texto en Numeric/Integer)
# =============================================================================
to_numeric_col(transferencias_df, "toneladas_iniciales")
to_numeric_col(transferencias_df, "toneladas_en_el_destino")
to_int_col(transferencias_df, "bultos_iniciales")
to_int_col(transferencias_df, "bultos_en_el_destino")


# =============================================================================
# 7) ASIGNAR id_ceda_agricultura
# =============================================================================
if "id_ceda_agricultura" not in transferencias_df.columns:
    transferencias_df["id_ceda_agricultura"] = None

if "estatus" in transferencias_df.columns:
    est = transferencias_df["estatus"].astype(str).str.strip().str.lower()

    if "cdf_destino_final" in transferencias_df.columns:
        mask_entregada = est == "entregada"
        transferencias_df.loc[mask_entregada, "id_ceda_agricultura"] = transferencias_df.loc[mask_entregada, "cdf_destino_final"]

    if "cdf_destino_original" in transferencias_df.columns:
        mask_no_entregada = est != "entregada"
        transferencias_df.loc[mask_no_entregada, "id_ceda_agricultura"] = transferencias_df.loc[mask_no_entregada, "cdf_destino_original"]


# =============================================================================
# 8) VALIDACIONES FUERTES ANTES DE TRUNCAR/INSERTAR
# =============================================================================
requeridas_notnull = ["fecha_de_salida"]  # aquí puedes agregar otras si tu tabla las exige

for col in requeridas_notnull:
    if col in transferencias_df.columns:
        faltan = transferencias_df[transferencias_df[col].isna()]
        if len(faltan) > 0:
            print(f"\n❌ VALIDACIÓN FALLIDA: {len(faltan)} filas tienen '{col}' vacío y la BD lo exige NOT NULL.")
            cols_show = [c for c in ["folio_transferencia", "fecha_de_salida", "fecha_de_llegada"] if c in faltan.columns]
            print(faltan[cols_show].head(25).to_string(index=False))
            raise SystemExit("Deteniendo importación para evitar truncar e insertar datos inválidos.")


# =============================================================================
# 9) DTYPE PARA INSERCIÓN (ojo: bultos_en_el_destino correcto)
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
# 10) INSERTAR A BASE DE DATOS
# =============================================================================
table_name = "transferencias"

print(f"\n🗑 Truncando tabla '{table_name}'...")
with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE {table_name};"))

print(f"⬆️ Insertando {len(transferencias_df)} registros en '{table_name}'...")
transferencias_df.to_sql(
    name=table_name,
    con=engine,
    if_exists="append",
    index=False,
    dtype=dtype_sqlalchemy,
    method="multi",
    chunksize=5000,
)

print("✅ Proceso finalizado sin errores.")