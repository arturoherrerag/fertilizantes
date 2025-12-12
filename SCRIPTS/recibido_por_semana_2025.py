#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autor: Arturo Herrera G.
Descripción: Calcula las toneladas recibidas por semana a partir de
             RECIBIDO POR SEMANA 2025_12062025.xlsx
"""

import os
import sys
import pandas as pd
from pathlib import Path
from datetime import date

# 🔧 1) Parámetros -----------------------------------------------------------------
RUTA_EXCEL = Path("/Users/Arturo/Downloads/RECIBIDO POR SEMANA 2025_12062025.xlsx")
RUTA_SALIDA = Path("/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/"
                   "toneladas_recibidas_por_semana_2025.csv")

COLUMNAS_REQUERIDAS = {
    "folio_del_flete",
    "abreviación producto",
    "toneladas_en_el_destino",
    "cdf_destino_final",
    "fecha_de_entrega",
    "estado_llegada"
}

# 🛠️ 2) Funciones auxiliares -------------------------------------------------------
def validar_columnas(df: pd.DataFrame) -> None:
    """Verifica que el DataFrame contenga todas las columnas requeridas."""
    faltantes = COLUMNAS_REQUERIDAS - set(df.columns.str.lower())
    if faltantes:
        msg = (f"❌ Columnas faltantes: {', '.join(sorted(faltantes))}. "
               "Revisa el archivo o la lista COLUMNAS_REQUERIDAS.")
        sys.exit(msg)

def leer_archivo_excel(ruta: Path) -> pd.DataFrame:
    """Lee el Excel y devuelve un DataFrame con nombres de columna normalizados."""
    if not ruta.exists():
        sys.exit(f"❌ No se encontró el archivo: {ruta}")
    df = pd.read_excel(ruta, engine="openpyxl")
    df.columns = df.columns.str.lower().str.strip()
    return df

def agregar_semana(df: pd.DataFrame) -> pd.DataFrame:
    """Añade columna 'semana_iso' con año-semana ISO (YYYY-WW)."""
    df["fecha_de_entrega"] = pd.to_datetime(df["fecha_de_entrega"], errors="coerce")
    if df["fecha_de_entrega"].isna().any():
        print("⚠️ Hay fechas no válidas que serán ignoradas.")
    df = df.dropna(subset=["fecha_de_entrega", "toneladas_en_el_destino"])
    df["toneladas_en_el_destino"] = pd.to_numeric(
        df["toneladas_en_el_destino"], errors="coerce"
    )
    df = df.dropna(subset=["toneladas_en_el_destino"])
    # año ISO + semana ISO → p.ej. 2025-24
    iso = df["fecha_de_entrega"].dt.isocalendar()
    df["semana_iso"] = iso["year"].astype(str) + "-" + iso["week"].astype(str).str.zfill(2)
    return df

def exportar_csv(df_agrupado: pd.DataFrame, ruta: Path) -> None:
    """Guarda el DataFrame en CSV UTF-8."""
    ruta.parent.mkdir(parents=True, exist_ok=True)
    df_agrupado.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"✅ Archivo exportado a: {ruta}")

# 🚀 3) Ejecución principal --------------------------------------------------------
def main() -> None:
    print("📖 Leyendo archivo Excel…")
    df = leer_archivo_excel(RUTA_EXCEL)

    print("🔎 Validando columnas…")
    validar_columnas(df)

    print("📅 Agregando columna de semana ISO…")
    df = agregar_semana(df)

    print("🧮 Calculando toneladas recibidas por semana…")
    resumen = (
        df.groupby("semana_iso", as_index=False)["toneladas_en_el_destino"]
          .sum()
          .rename(columns={"toneladas_en_el_destino": "toneladas_recibidas"})
          .sort_values("semana_iso")
    )

    # Muestra rápida por consola
    print("\n=== Toneladas recibidas por semana ===")
    print(resumen.to_string(index=False))

    print("\n💾 Exportando CSV…")
    exportar_csv(resumen, RUTA_SALIDA)

    print("\n🚀 Proceso finalizado con éxito.")

if __name__ == "__main__":
    print(f"🗓️  Ejecución: {date.today().isoformat()}")
    main()
