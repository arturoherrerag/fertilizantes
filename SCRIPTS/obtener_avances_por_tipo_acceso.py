#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import math
import re
import pandas as pd

# === Ruta base con los CSV ya generados ===
RUTA_BASE = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/BASES_ORIGINALES_SIGAP/derechohabientes_padrones_compilado"

# === Salidas ===
SALIDA_CRUCE = "cruce_derechohabientes_estatus.csv"  # detalle acuse + estatus + estado + toneladas
SALIDA_RESUMEN = "resumen_por_estado_y_estatus.csv"  # pivote por estado (filas) y estatus (columnas)

# === Tamaño de lote para consultas IN a PostgreSQL (evita listas demasiado grandes) ===
CHUNK_SIZE = 10000

# Usaremos tu conexión centralizada
from conexion import engine  # Debe exponer un SQLAlchemy engine


# ----------------------- Utilidades de normalización -----------------------
def _norm(s: str) -> str:
    """minúsculas + quita espacios laterales + reemplaza espacios múltiples y símbolos comunes por '_' """
    if s is None:
        return ""
    s0 = str(s).strip().lower()
    s0 = re.sub(r"\s+", "_", s0)
    s0 = s0.replace("(", "_").replace(")", "_")
    s0 = s0.replace("[", "_").replace("]", "_")
    s0 = s0.replace("/", "_").replace("\\", "_")
    s0 = s0.replace("__", "_")
    return s0

def _colname_lookup(cols, *candidatos, contains=None, all_contains=None):
    """
    Busca en 'cols' la columna que empata con cualquiera de los 'candidatos' normalizados exactamente.
    También puede buscar por 'contains' (una cadena) o 'all_contains' (lista de subcadenas que deben aparecer).
    Retorna el nombre original encontrado o None.
    """
    mapa = {_norm(c): c for c in cols}

    # Prioridad: match exacto por candidatos
    for objetivo in candidatos:
        key = _norm(objetivo)
        if key in mapa:
            return mapa[key]

    # Búsqueda por contains (solo una cadena)
    if contains:
        key = _norm(contains)
        for k_norm, c_org in mapa.items():
            if key in k_norm:
                return c_org

    # Búsqueda por all_contains (todas las subcadenas deben estar presentes)
    if all_contains and isinstance(all_contains, (list, tuple)):
        must = [_norm(x) for x in all_contains]
        for k_norm, c_org in mapa.items():
            if all(x in k_norm for x in must):
                return c_org

    return None

def _to_numeric_series(s: pd.Series) -> pd.Series:
    """Convierte a número permitiendo coma decimal; vacíos -> 0."""
    if s is None:
        return pd.Series(dtype="float64")
    # Reemplazar coma por punto y coercionar
    return pd.to_numeric(s.astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0.0)


# ----------------------- Lectura y unificación de CSVs -----------------------
def leer_y_unificar_csvs(ruta_base: str) -> pd.DataFrame:
    """
    Lee TODOS los .csv en la ruta_base y conserva:
    - acuse_estatal
    - estatus
    - estado_predio_inegi
    - DAP (ton)
    - UREA (ton)

    Devuelve un DataFrame único, sin duplicados por acuse (se queda con la última aparición).
    """
    patrones = [os.path.join(ruta_base, "*.csv")]
    archivos = []
    for p in patrones:
        archivos.extend(glob.glob(p))

    if not archivos:
        raise FileNotFoundError("No se encontraron .csv en la carpeta especificada.")

    frames = []
    for path in archivos:
        try:
            df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="utf-8", dtype=str, errors="replace")

        # Buscar columnas clave con tolerancia
        col_acuse = _colname_lookup(df.columns, "acuse_estatal", "Acuse_estatal")
        col_estatus = _colname_lookup(df.columns, "estatus")
        # estado_predio_inegi puede venir como "estado_predio_inegi", "Estado_predio_Inegi", etc.
        col_estado = _colname_lookup(df.columns, "estado_predio_inegi", "estado_predio_Inegi", "Estado_predio_inegi", "Estado_predio_Inegi")
        # DAP (ton): buscamos por 'dap' y 'ton'
        col_dap = (_colname_lookup(df.columns, "dap_(ton)", contains="dap") or
                   _colname_lookup(df.columns, all_contains=["dap", "ton"]))
        # UREA (ton): buscamos por 'urea' y 'ton'
        col_urea = (_colname_lookup(df.columns, "urea_(ton)", contains="urea") or
                    _colname_lookup(df.columns, all_contains=["urea", "ton"]))

        if not col_acuse:
            raise KeyError(f"El archivo '{os.path.basename(path)}' no contiene la columna 'Acuse_estatal'/'acuse_estatal'.")
        if not col_estatus:
            raise KeyError(f"El archivo '{os.path.basename(path)}' no contiene la columna 'estatus'.")
        if not col_estado:
            raise KeyError(f"El archivo '{os.path.basename(path)}' no contiene la columna 'estado_predio_inegi' (con alguna variante).")
        if not col_dap:
            raise KeyError(f"El archivo '{os.path.basename(path)}' no contiene la columna DAP (ton).")
        if not col_urea:
            raise KeyError(f"El archivo '{os.path.basename(path)}' no contiene la columna UREA (ton).")

        tmp = df[[col_acuse, col_estatus, col_estado, col_dap, col_urea]].copy()
        tmp.rename(columns={
            col_acuse: "acuse_estatal",
            col_estatus: "estatus",
            col_estado: "estado_predio_inegi",
            col_dap: "dap_ton",
            col_urea: "urea_ton"
        }, inplace=True)

        # Limpieza ligera
        tmp["acuse_estatal"] = tmp["acuse_estatal"].astype(str).str.strip()
        tmp["estatus"] = tmp["estatus"].astype(str).str.strip()
        tmp["estado_predio_inegi"] = tmp["estado_predio_inegi"].astype(str).str.strip()

        # Toneladas → numérico
        tmp["dap_ton"] = _to_numeric_series(tmp["dap_ton"])
        tmp["urea_ton"] = _to_numeric_series(tmp["urea_ton"])

        # Filtrar vacíos de clave
        tmp = tmp[tmp["acuse_estatal"].notna() & (tmp["acuse_estatal"] != "")]
        frames.append(tmp)

    base = pd.concat(frames, ignore_index=True)
    # En caso de duplicados por acuse, nos quedamos con la ÚLTIMA aparición
    base = base.drop_duplicates(subset=["acuse_estatal"], keep="last")
    return base


# ----------------------- Consulta a PostgreSQL -----------------------
def obtener_acuses_existentes_en_bd(acuses: pd.Series) -> pd.DataFrame:
    """
    Consulta en lotes a PostgreSQL para validar qué acuses existen en la tabla 'derechohabientes'.
    Devuelve un DataFrame con una sola columna 'acuse_estatal' (los existentes).
    """
    acuses_list = acuses.dropna().unique().tolist()
    if not acuses_list:
        return pd.DataFrame(columns=["acuse_estatal"])

    existentes = []
    total = len(acuses_list)
    num_chunks = math.ceil(total / CHUNK_SIZE)

    from sqlalchemy import text
    with engine.connect() as conn:
        for i in range(num_chunks):
            lote = acuses_list[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
            # Consulta usando IN con tuple
            query = text("""
                SELECT acuse_estatal
                FROM derechohabientes
                WHERE acuse_estatal IN :vals
            """)
            df_chunk = pd.read_sql(query, conn, params={"vals": tuple(lote)})
            if not df_chunk.empty:
                df_chunk["acuse_estatal"] = df_chunk["acuse_estatal"].astype(str).str.strip()
                existentes.append(df_chunk)

    if existentes:
        return pd.concat(existentes, ignore_index=True).drop_duplicates(subset=["acuse_estatal"])
    else:
        return pd.DataFrame(columns=["acuse_estatal"])


# ----------------------- Principal -----------------------
def main():
    # 1) Leer y unificar CSVs locales (acuse, estatus, estado, dap_ton, urea_ton)
    df_csv = leer_y_unificar_csvs(RUTA_BASE)

    # 2) Consultar a BD cuáles acuses existen
    df_existentes = obtener_acuses_existentes_en_bd(df_csv["acuse_estatal"])

    # 3) Join interno para quedarnos con acuses que están en BD
    df_cruce = df_existentes.merge(df_csv, on="acuse_estatal", how="inner")

    # Guardar cruce detallado por si se requiere auditoría
    ruta_cruce = os.path.join(RUTA_BASE, SALIDA_CRUCE)
    df_cruce[["acuse_estatal", "estado_predio_inegi", "estatus", "dap_ton", "urea_ton"]].to_csv(
        ruta_cruce, index=False, encoding="utf-8-sig"
    )

    # 4) Resumen por ESTADO (filas) y ESTATUS (columnas):
    #    Conteo de registros y suma de DAP/UREA por cada estatus
    #    Primero agregamos por (estado, estatus)
    agg = (
        df_cruce
        .groupby(["estado_predio_inegi", "estatus"], dropna=False, as_index=False)
        .agg(
            conteo=("acuse_estatal", "size"),
            dap_ton_sum=("dap_ton", "sum"),
            urea_ton_sum=("urea_ton", "sum"),
        )
    )

    # Pivotear a columnas por estatus; columnas serán MultiIndex (metricas)
    # Después aplanamos a nombre_columna = "<estatus>__<metrica>"
    pivot = agg.pivot_table(
        index="estado_predio_inegi",
        columns="estatus",
        values=["conteo", "dap_ton_sum", "urea_ton_sum"],
        fill_value=0,
        aggfunc="sum"
    )

    # Aplanar nombres de columnas
    # columnas tipo: ('conteo', 'ENTREGADO') -> 'ENTREGADO__conteo'
    pivot.columns = [f"{str(col_estatus)}__{metrica}" for (metrica, col_estatus) in pivot.columns]
    pivot = pivot.reset_index().sort_values("estado_predio_inegi")

    ruta_resumen = os.path.join(RUTA_BASE, SALIDA_RESUMEN)
    pivot.to_csv(ruta_resumen, index=False, encoding="utf-8-sig")

    # 5) Mensaje en consola
    print("✅ Proceso completado.")
    print(f"   - Registros únicos en CSV: {len(df_csv)}")
    print(f"   - Acuses encontrados en BD: {len(df_existentes)}")
    print(f"   - Registros en el cruce final: {len(df_cruce)}")
    print(f"   - Archivo de cruce: {ruta_cruce}")
    print(f"   - Resumen por estado x estatus: {ruta_resumen}")

if __name__ == "__main__":
    main()