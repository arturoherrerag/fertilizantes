#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Carga la tabla public.derechohabientes_2024 desde beneficiarios_2024.csv
usando COPY, dejando TODOS los registros en la BD.

- Si un valor numérico/fecha no se puede convertir:
  - Se carga como NULL (numéricos) o como fecha centinela 1900-01-01 (fecha_entrega).
  - Se registra el problema en un archivo de LOG .txt

Archivos que genera:
- derechohabientes_2024_clean_for_copy.csv  (para COPY)
- derechohabientes_2024_errores_log.txt     (descripción de errores)
"""

import os
import pandas as pd
from conexion import psycopg_conn  # conexión psycopg2 a PostgreSQL

# ========= RUTAS Y PARÁMETROS =========

CSV_ORIGEN  = "/Users/Arturo/AGRICULTURA/2024/beneficiarios_2024.csv"
CSV_LIMPIO  = "/Users/Arturo/AGRICULTURA/2024/derechohabientes_2024_clean_for_copy.csv"
LOG_ERRORES = "/Users/Arturo/AGRICULTURA/2024/derechohabientes_2024_errores_log.txt"

CHUNK_SIZE   = 100_000
CSV_SEP      = ","
CSV_ENCODING = "utf-8-sig"   # BOM para que Excel detecte UTF-8

# Orden de columnas EXACTO según la tabla derechohabientes_2024
COLUMNAS_ORDEN = [
    "clave_estado_predio_capturada",
    "estado_predio_capturada",
    "clave_municipio_predio_capturada",
    "municipio_predio_capturada",
    "clave_localidad_predio_capturada",
    "localidad_predio_capturada",
    "id_nu_solicitud",
    "cdf_entrega",
    "id_cdf_entrega",
    "acuse_estatal",
    "curp_solicitud",
    "curp_renapo",
    "curp_historica",
    "sn_primer_apellido",
    "sn_segundo_apellido",
    "ln_nombre",
    "es_pob_indigena",
    "cultivo",
    "ton_dap_entregada",
    "ton_urea_entregada",
    "fecha_entrega",
    "folio_persona",
    "cuadernillo",
    "nombre_ddr",
    "clave_ddr",
    "nombre_cader_ventanilla",
    "clave_cader_ventanilla",
    "dap_2024_25_kg",
    "urea_2024_25_kg",
    "dap_2023_25_kg",
    "urea_2023_25_kg",
    "superficie_apoyada",
]

# Columnas numéricas ENTERAS
COLS_INT = [
    "clave_estado_predio_capturada",
    "clave_municipio_predio_capturada",
    "clave_localidad_predio_capturada",
    "id_nu_solicitud",
    "id_cdf_entrega",
    "folio_persona",
    "clave_ddr",
    "clave_cader_ventanilla",
    "dap_2024_25_kg",
    "urea_2024_25_kg",
    "dap_2023_25_kg",
    "urea_2023_25_kg",
    "superficie_apoyada",
]

# Columnas numéricas DECIMALES
COLS_DEC = [
    "ton_dap_entregada",
    "ton_urea_entregada",
]

# Columna de fecha
COL_FECHA = "fecha_entrega"

# Valores que trataremos como NULL (además de vacío)
NULL_LIKE = {"", "nan", "NaN", "None"}


def procesar_chunk(chunk: pd.DataFrame, start_line: int, log_file):
    """
    Procesa un chunk:
    - Normaliza tipos.
    - Registra errores de conversión en log_file.
    - Devuelve el DataFrame listo para escribir al CSV limpio.
    `start_line` es el número de línea del CSV (1-based) donde inicia este chunk (contando encabezado).
    """
    # Copia y reseteo de índice para que 0..n-1 coincida con la posición en el chunk
    chunk = chunk.copy().reset_index(drop=True)

    # Normalizar nombres de columnas a minúsculas
    chunk.columns = [c.strip().lower() for c in chunk.columns]

    # Verificar columnas
    faltan = [c for c in COLUMNAS_ORDEN if c not in chunk.columns]
    if faltan:
        raise ValueError(f"Faltan columnas en el CSV de origen: {faltan}")

    df = chunk[COLUMNAS_ORDEN].copy()

    # Pasar todo a string y strip
    df = df.apply(lambda s: s.astype(str).str.strip())
    df = df.replace({"nan": "", "NaN": "", "None": ""})

    # ---------- ENTEROS ----------
    for col in COLS_INT:
        raw = df[col]
        mask_null = raw.isin(NULL_LIKE)

        converted = pd.to_numeric(
            raw.where(~mask_null, None),
            errors="coerce"
        )

        mask_error = converted.isna() & (~mask_null)

        if mask_error.any():
            idx_err = mask_error[mask_error].index
            for i in idx_err:
                linea_csv = start_line + i
                valor = raw.iat[i]
                log_file.write(
                    f"Línea {linea_csv}, columna {col}: "
                    f"valor '{valor}' no se puede convertir a ENTERO; se cargará como NULL.\n"
                )

        # Para válidos: dejamos como string del entero
        df.loc[~mask_error & ~mask_null, col] = (
            converted[~mask_error & ~mask_null].astype("Int64").astype(str)
        )
        # Para null (incluidos errores): cadena vacía -> NULL en COPY
        df.loc[mask_null | mask_error, col] = ""

    # ---------- DECIMALES ----------
    for col in COLS_DEC:
        raw = df[col]
        mask_null = raw.isin(NULL_LIKE)

        converted = pd.to_numeric(
            raw.where(~mask_null, None),
            errors="coerce"
        )
        mask_error = converted.isna() & (~mask_null)

        if mask_error.any():
            idx_err = mask_error[mask_error].index
            for i in idx_err:
                linea_csv = start_line + i
                valor = raw.iat[i]
                log_file.write(
                    f"Línea {linea_csv}, columna {col}: "
                    f"valor '{valor}' no se puede convertir a DECIMAL; se cargará como NULL.\n"
                )

        df.loc[~mask_error & ~mask_null, col] = (
            converted[~mask_error & ~mask_null].astype(str)
        )
        df.loc[mask_null | mask_error, col] = ""

    # ---------- FECHA ----------
    raw_fecha = df[COL_FECHA]

    # Aseguramos limpieza básica
    raw_fecha = raw_fecha.replace({"nan": "", "NaN": "", "None": ""})

    # Patrones:
    #  - ISO: YYYY-MM-DD
    #  - DMY: DD/MM/YY o DD/MM/YYYY
    mask_iso   = raw_fecha.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    mask_dmy   = raw_fecha.str.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", na=False)
    mask_vacio = raw_fecha.eq("")
    mask_otros = ~(mask_iso | mask_dmy | mask_vacio)

    # 1) Fechas en formato ISO (YYYY-MM-DD): las dejamos tal cual
    df.loc[mask_iso, COL_FECHA] = raw_fecha[mask_iso]

    # 2) Fechas en formato DD/MM/AA[AA]: las convertimos
    if mask_dmy.any():
        idx_dmy = raw_fecha[mask_dmy].index
        fechas_dmy = pd.to_datetime(
            raw_fecha.loc[idx_dmy],
            dayfirst=True,
            errors="coerce"
        )

        idx_err = fechas_dmy[fechas_dmy.isna()].index
        idx_ok  = fechas_dmy[fechas_dmy.notna()].index

        # Correctas -> YYYY-MM-DD
        if len(idx_ok) > 0:
            df.loc[idx_ok, COL_FECHA] = fechas_dmy.loc[idx_ok].dt.strftime("%Y-%m-%d")

        # Errores -> fecha centinela + log
        if len(idx_err) > 0:
            for i in idx_err:
                linea_csv = start_line + i
                valor = raw_fecha.loc[i]
                log_file.write(
                    f"Línea {linea_csv}, columna {COL_FECHA}: "
                    f"valor '{valor}' no se puede convertir desde DD/MM/AA[AA]; "
                    f"se usará fecha centinela 1900-01-01.\n"
                )
            df.loc[idx_err, COL_FECHA] = "1900-01-01"

    # 3) Valores totalmente vacíos
    if mask_vacio.any():
        idx_vacios = raw_fecha[mask_vacio].index
        for i in idx_vacios:
            linea_csv = start_line + i
            log_file.write(
                f"Línea {linea_csv}, columna {COL_FECHA}: "
                f"valor vacío; se usará fecha centinela 1900-01-01.\n"
            )
        df.loc[idx_vacios, COL_FECHA] = "1900-01-01"

    # 4) Otros formatos raros
    if mask_otros.any():
        idx_otros = raw_fecha[mask_otros].index
        for i in idx_otros:
            linea_csv = start_line + i
            valor = raw_fecha.loc[i]
            log_file.write(
                f"Línea {linea_csv}, columna {COL_FECHA}: "
                f"formato no reconocido '{valor}'; se usará fecha centinela 1900-01-01.\n"
            )
        df.loc[idx_otros, COL_FECHA] = "1900-01-01"

    return df


def generar_csv_limpio_y_log():
    """
    Lee el CSV origen por chunks, genera:
    - CSV_LIMPIO: listo para COPY (todas las filas).
    - LOG_ERRORES: descripción de cualquier conversión problemática.
    """
    if not os.path.exists(CSV_ORIGEN):
        raise FileNotFoundError(f"No se encontró el archivo origen: {CSV_ORIGEN}")

    os.makedirs(os.path.dirname(CSV_LIMPIO), exist_ok=True)

    # Borrar previos
    for path in (CSV_LIMPIO, LOG_ERRORES):
        if os.path.exists(path):
            os.remove(path)

    reader = pd.read_csv(
        CSV_ORIGEN,
        chunksize=CHUNK_SIZE,
        sep=CSV_SEP,
        encoding=CSV_ENCODING,
        dtype=str,
    )

    first_clean = True
    total_rows = 0

    # Línea inicial de datos: 2 (1 es el encabezado)
    start_line = 2

    with open(LOG_ERRORES, "a", encoding="utf-8") as log_file:
        log_file.write(
            "LOG DE ERRORES DE CONVERSIÓN - derechohabientes_2024\n"
            "----------------------------------------------------\n"
        )

        for i, chunk in enumerate(reader, start=1):
            print(f"Procesando chunk {i}...")
            n_chunk_rows = len(chunk)

            df_proc = procesar_chunk(chunk, start_line, log_file)

            # Escribir CSV limpio
            df_proc.to_csv(
                CSV_LIMPIO,
                mode="a",
                index=False,
                header=first_clean,
                encoding=CSV_ENCODING,
            )
            first_clean = False

            total_rows += n_chunk_rows
            start_line += n_chunk_rows

        log_file.write("\nFin del LOG.\n")

    print("\nProcesamiento completado.")
    print(f"Filas totales procesadas (incluyendo nulos): {total_rows}")
    print(f"CSV limpio para COPY generado en:\n  {CSV_LIMPIO}")
    print(f"Log de errores generado en:\n  {LOG_ERRORES}")


def cargar_con_copy():
    """
    TRUNCATE + COPY desde CSV_LIMPIO a public.derechohabientes_2024.
    """
    if not os.path.exists(CSV_LIMPIO):
        raise FileNotFoundError(
            f"No se encontró el CSV limpio para COPY: {CSV_LIMPIO}\n"
            "Primero ejecuta generar_csv_limpio_y_log()."
        )

    copy_sql = """
        COPY public.derechohabientes_2024 (
            clave_estado_predio_capturada,
            estado_predio_capturada,
            clave_municipio_predio_capturada,
            municipio_predio_capturada,
            clave_localidad_predio_capturada,
            localidad_predio_capturada,
            id_nu_solicitud,
            cdf_entrega,
            id_cdf_entrega,
            acuse_estatal,
            curp_solicitud,
            curp_renapo,
            curp_historica,
            sn_primer_apellido,
            sn_segundo_apellido,
            ln_nombre,
            es_pob_indigena,
            cultivo,
            ton_dap_entregada,
            ton_urea_entregada,
            fecha_entrega,
            folio_persona,
            cuadernillo,
            nombre_ddr,
            clave_ddr,
            nombre_cader_ventanilla,
            clave_cader_ventanilla,
            dap_2024_25_kg,
            urea_2024_25_kg,
            dap_2023_25_kg,
            urea_2023_25_kg,
            superficie_apoyada
        )
        FROM STDIN
        WITH (
            FORMAT csv,
            HEADER true,
            DELIMITER ',',
            NULL ''
        );
    """

    with psycopg_conn:
        with psycopg_conn.cursor() as cur:
            print("\nTruncando tabla public.derechohabientes_2024...")
            cur.execute("TRUNCATE TABLE public.derechohabientes_2024;")

            print("Ejecutando COPY desde CSV limpio...")
            with open(CSV_LIMPIO, "r", encoding=CSV_ENCODING) as f:
                cur.copy_expert(copy_sql, f)

    print("Carga con COPY completada correctamente.")


def main():
    print("=== Paso 1: generar CSV limpio y LOG de errores ===")
    generar_csv_limpio_y_log()

    print("\n=== Paso 2: cargar CSV limpio con COPY ===")
    cargar_con_copy()


if __name__ == "__main__":
    main()