#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exporta la vista materializada mv_meta_y_avance_ceda_estatus_2025
"""

import os
import pandas as pd
from conexion import engine  # usa tu conexión centralizada

# === Rutas de salida ===
BASE_DIR = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS"
os.makedirs(BASE_DIR, exist_ok=True)

VISTAS = {
    "mv_meta_y_avance_ceda_estatus_2025": os.path.join(BASE_DIR, "mv_meta_y_avance_ceda_estatus_2025.csv"),
}

def exportar_vista(nombre_vista: str, ruta_salida: str):
    """Lee la vista desde PostgreSQL y exporta a CSV UTF-8-sig (Excel friendly)."""
    print(f"➡️ Exportando {nombre_vista} ...")
    query = f'SELECT * FROM public."{nombre_vista}"'
    df = pd.read_sql(query, engine)

    # Exportar a CSV con BOM UTF-8 para compatibilidad con Excel México
    df.to_csv(ruta_salida, index=False, encoding="utf-8-sig")
    print(f"   ✅ {nombre_vista} exportada a: {ruta_salida} ({len(df)} filas)")

def main():
    for vista, ruta in VISTAS.items():
        exportar_vista(vista, ruta)
    print("🚀 Exportación completa.")

if __name__ == "__main__":
    main()