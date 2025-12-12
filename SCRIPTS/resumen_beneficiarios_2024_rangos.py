#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Resumen por rangos de superficie para beneficiarios 2024.

- Cuenta productores (por Acuse_estatal) por rangos de superficie_apoyada.
- Suma ton_dap_entregada, ton_urea_entregada y superficie_apoyada por los mismos rangos.
- Genera un DataFrame resumen y lo exporta a CSV.

Rangos utilizados (superficie_apoyada en hectáreas):
  0  <= sup < 5   -> "0 a 4"
  5  <= sup < 10  -> "5 a 9"
  10 <= sup < 15  -> "10 a 14"
  15 <= sup < 20  -> "15 a 19"
  20 <= sup       -> "20 o más"
"""

import os
import pandas as pd

from conexion import engine, psycopg_conn, DB_NAME  # noqa: F401


# ========= PARÁMETROS =========
CSV_PATH = "/Users/Arturo/AGRICULTURA/2024/beneficiarios_2024.csv"
CSV_SEP = ","
CSV_ENCODING = "utf-8"

OUTPUT_PATH = "/Users/Arturo/AGRICULTURA/2024/resumen_beneficiarios_2024_rangos_superficie.csv"


def main():
    # ========== 1. CARGA DEL CSV ==========
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"No se encontró el archivo: {CSV_PATH}")

    df = pd.read_csv(
        CSV_PATH,
        sep=CSV_SEP,
        encoding=CSV_ENCODING
    )

    # ========== 2. VALIDACIÓN DE COLUMNAS ==========
    required_cols = [
        "Acuse_estatal",
        "superficie_apoyada",
        "ton_dap_entregada",
        "ton_urea_entregada",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el CSV: {missing}")

    # ========== 3. CONVERSIÓN A NUMÉRICO ==========
    df["superficie_apoyada"] = pd.to_numeric(
        df["superficie_apoyada"], errors="coerce"
    )
    df["ton_dap_entregada"] = pd.to_numeric(
        df["ton_dap_entregada"], errors="coerce"
    )
    df["ton_urea_entregada"] = pd.to_numeric(
        df["ton_urea_entregada"], errors="coerce"
    )

    # Filtramos superficies válidas (no nulas y no negativas)
    df_valid = df[df["superficie_apoyada"].notna() & (df["superficie_apoyada"] >= 0)].copy()

    # ========== 4. DEFINICIÓN DE RANGOS ==========
    bins = [0, 5, 10, 15, 20, float("inf")]
    labels = ["0 a 4", "5 a 9", "10 a 14", "15 a 19", "20 o más"]

    df_valid["rango_superficie"] = pd.cut(
        df_valid["superficie_apoyada"],
        bins=bins,
        labels=labels,
        right=False,
        include_lowest=True,
    )

    # ========== 5. CÁLCULO DE SUMAS Y CONTADORES ==========
    resumen = (
        df_valid
        .groupby("rango_superficie", observed=True)
        .agg(
            productores=("Acuse_estatal", "nunique"),
            superficie_total_ha=("superficie_apoyada", "sum"),       # ⬅️ NUEVO: suma de hectáreas
            ton_dap_entregada=("ton_dap_entregada", "sum"),
            ton_urea_entregada=("ton_urea_entregada", "sum"),
        )
        .reset_index()
    )

    # Total DAP + UREA
    resumen["ton_total_entregada"] = (
        resumen["ton_dap_entregada"].fillna(0)
        + resumen["ton_urea_entregada"].fillna(0)
    )

    # Orden explícito de categorías
    resumen["rango_superficie"] = pd.Categorical(
        resumen["rango_superficie"],
        categories=labels,
        ordered=True,
    )
    resumen = resumen.sort_values("rango_superficie")

    # ========== 6. EXPORTAR A CSV ==========
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    resumen.to_csv(OUTPUT_PATH, index=False)

    # ========== 7. MOSTRAR RESUMEN EN CONSOLA ==========
    print("\nResumen por rangos de superficie (beneficiarios 2024):\n")
    print(resumen.to_string(index=False))

    total_registros = len(df)
    total_validos = len(df_valid)
    print("\nRegistros totales en el archivo:", total_registros)
    print("Registros con superficie_apoyada válida (>= 0):", total_validos)
    print("Registros excluidos por superficie_apoyada nula o negativa:",
          total_registros - total_validos)

    print(f"\nArchivo de salida generado en:\n{OUTPUT_PATH}")


if __name__ == "__main__":
    main()