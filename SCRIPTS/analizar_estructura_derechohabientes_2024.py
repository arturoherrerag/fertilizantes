#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Analiza la estructura del archivo beneficiarios_2024.csv para diseñar
la tabla derechohabientes_2024 en PostgreSQL.

Para cada columna calcula:
- valores_totales
- valores_no_nulos
- longitud_maxima (texto)
- si puede ser entero, numérico (con decimales), fecha
- dígitos máximos enteros y decimales (para NUMERIC)

Salida: CSV con el resumen de estructura.
"""

import os
import math
import pandas as pd

# Si quieres mantener tu estándar de entorno:
from conexion import engine, psycopg_conn, DB_NAME  # noqa: F401

# ========= PARÁMETROS =========
CSV_PATH = "/Users/Arturo/AGRICULTURA/2024/beneficiarios_2024.csv"
CHUNK_SIZE = 100_000

OUTPUT_PATH = "/Users/Arturo/AGRICULTURA/2024/analisis_estructura_derechohabientes_2024.csv"

# Tamaño máximo de muestra por columna para intentar detectar fecha
DATE_SAMPLE_MAX = 500


def inicializar_stats(columnas):
    stats = {}
    for col in columnas:
        stats[col] = {
            "valores_totales": 0,
            "valores_no_nulos": 0,
            "longitud_maxima": 0,
            "int_candidato": True,
            "numeric_candidato": True,
            "max_digitos_enteros": 0,
            "max_digitos_decimales": 0,
            "muestra_fecha": [],  # valores de ejemplo para probar fecha
        }
    return stats


def actualizar_stats_columna(col, serie, stats_col):
    """
    Actualiza estadísticas para una columna en un chunk.
    `serie` llega como tipo string (por diseño del read_csv).
    """

    # Normalizamos: strip y manejamos 'nan' / vacíos como nulos
    s = serie.astype(str).str.strip()
    # Excluir vacíos y 'nan'/'NaN' como valores válidos
    mask_valid = (s != "") & (~s.str.lower().eq("nan"))

    valores_totales_chunk = len(s)
    valores_no_nulos_chunk = mask_valid.sum()

    stats_col["valores_totales"] += valores_totales_chunk
    stats_col["valores_no_nulos"] += valores_no_nulos_chunk

    if valores_no_nulos_chunk == 0:
        return  # Nada más que hacer en este chunk

    vals = s[mask_valid]

    # ---------- Longitud máxima de texto ----------
    max_len_chunk = vals.str.len().max()
    if pd.notna(max_len_chunk):
        stats_col["longitud_maxima"] = max(
            stats_col["longitud_maxima"],
            int(max_len_chunk)
        )

    # ---------- Detección de números ----------
    # Si ya descartamos numeric, no seguimos gastando tiempo
    # pero si aún es candidato, probamos con regex.
    # Permitimos: +123, -123, 123, 123.45, -0.001, etc.
    if stats_col["numeric_candidato"]:
        # Valores que parecen numéricos (enteros o decimales)
        mask_numeric = vals.str.match(r"^[+-]?\d+(\.\d+)?$")
        if not mask_numeric.all():
            # Hay valores que no son numéricos -> no es numeric
            stats_col["numeric_candidato"] = False
            stats_col["int_candidato"] = False
        else:
            # Todos son numéricos, revisamos enteros vs decimales
            mask_int = vals.str.match(r"^[+-]?\d+$")
            if not mask_int.all():
                stats_col["int_candidato"] = False

            # Cálculo de dígitos enteros y decimales
            solo_numeros = vals[mask_numeric]

            # Quitamos signo
            sin_signo = solo_numeros.str.replace(r"[+-]", "", regex=True)
            partes = sin_signo.str.split(".", n=1, expand=True)

            parte_entera = partes[0]
            # Quitamos ceros a la izquierda solo para contar dígitos significativos
            parte_entera_sin_ceros = parte_entera.str.lstrip("0")
            # Si termina vacío (ej. "0"), ponemos 1 dígito
            parte_entera_len = parte_entera_sin_ceros.str.len().replace(0, 1)

            max_ent_chunk = parte_entera_len.max()
            if pd.notna(max_ent_chunk):
                stats_col["max_digitos_enteros"] = max(
                    stats_col["max_digitos_enteros"],
                    int(max_ent_chunk)
                )

            if partes.shape[1] > 1:
                parte_decimal = partes[1].fillna("")
                # Quitamos ceros a la derecha para el conteo de decimales significativos
                parte_decimal_sin_ceros = parte_decimal.str.rstrip("0")
                parte_decimal_len = parte_decimal_sin_ceros.str.len()
                max_dec_chunk = parte_decimal_len.max()
                if pd.notna(max_dec_chunk):
                    stats_col["max_digitos_decimales"] = max(
                        stats_col["max_digitos_decimales"],
                        int(max_dec_chunk)
                    )

    # ---------- Muestra para fecha ----------
    # Solo si todavía no tenemos suficiente muestra
    muestra_actual = stats_col["muestra_fecha"]
    if len(muestra_actual) < DATE_SAMPLE_MAX:
        faltan = DATE_SAMPLE_MAX - len(muestra_actual)
        # Tomamos los primeros 'faltan' valores del chunk
        nuevos = vals.head(faltan).tolist()
        stats_col["muestra_fecha"].extend(nuevos)


def inferir_tipo_columna(nombre_col, stats_col):
    """
    A partir de las stats de la columna, sugiere tipo lógico:
    - integer
    - numeric
    - date
    - text
    """
    non_null = stats_col["valores_no_nulos"]
    if non_null == 0:
        return "text"  # todo nulo, lo dejamos como texto por seguridad

    # Intento de fecha con la muestra
    tipo_fecha = False
    muestra = stats_col["muestra_fecha"]
    if muestra:
        try:
            dt = pd.to_datetime(muestra, errors="coerce", dayfirst=True)
            # Porcentaje de valores parseables como fecha
            tasa_ok = dt.notna().mean()
            if tasa_ok >= 0.9:  # si >= 90% parecen fecha, lo tomamos como date
                tipo_fecha = True
        except Exception:
            tipo_fecha = False

    if tipo_fecha:
        return "date"

    if stats_col["int_candidato"] and stats_col["numeric_candidato"]:
        return "integer"

    if stats_col["numeric_candidato"]:
        return "numeric"

    # En cualquier otro caso, lo tratamos como texto
    return "text"


def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"No se encontró el archivo: {CSV_PATH}")

    # Leemos por chunks como texto para tener control sobre formatos
    reader = pd.read_csv(
        CSV_PATH,
        chunksize=CHUNK_SIZE,
        dtype=str,
        # Si tienes separador distinto, ajústalo:
        sep=",",
        encoding="utf-8",
    )

    stats = None

    print("Analizando estructura de:", CSV_PATH)

    for i, chunk in enumerate(reader, start=1):
        if stats is None:
            # Inicializamos stats con la lista de columnas del primer chunk
            stats = inicializar_stats(chunk.columns.tolist())

        print(f"  Procesando chunk {i}...")

        for col in chunk.columns:
            actualizar_stats_columna(col, chunk[col], stats[col])

    # Ahora inferimos tipo para cada columna y armamos resumen en DataFrame
    resumen_rows = []
    for col, st in stats.items():
        tipo = inferir_tipo_columna(col, st)

        # Para numeric calculamos precision y scale sugeridos
        precision_sugerida = None
        scale_sugerida = None

        if tipo == "integer":
            # max_digitos_enteros -> suficiente para BIGINT/INT
            precision_sugerida = st["max_digitos_enteros"]
            scale_sugerida = 0
        elif tipo == "numeric":
            # Para NUMERIC(precision, scale):
            # precision = enteros + decimales
            precision_sugerida = (
                st["max_digitos_enteros"] + st["max_digitos_decimales"]
            )
            scale_sugerida = st["max_digitos_decimales"]
            # Por seguridad, si precision queda muy baja y scale 0 pero había decimales raros,
            # podrías forzar un mínimo, pero aquí respetamos lo observado.

        resumen_rows.append(
            {
                "columna": col,
                "tipo_inferido": tipo,
                "valores_totales": st["valores_totales"],
                "valores_no_nulos": st["valores_no_nulos"],
                "longitud_maxima_texto": st["longitud_maxima"],
                "max_digitos_enteros": st["max_digitos_enteros"],
                "max_digitos_decimales": st["max_digitos_decimales"],
                "precision_sugerida": precision_sugerida,
                "scale_sugerida": scale_sugerida,
            }
        )

    df_resumen = pd.DataFrame(resumen_rows)

    # Ordenamos por nombre de columna para que sea más legible
    df_resumen = df_resumen.sort_values("columna")

    # Guardamos a CSV
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_resumen.to_csv(OUTPUT_PATH, index=False)

    print("\nAnálisis completado.")
    print("Resumen de estructura guardado en:")
    print(OUTPUT_PATH)
    print("\nVista rápida:")
    print(df_resumen.to_string(index=False))


if __name__ == "__main__":
    main()