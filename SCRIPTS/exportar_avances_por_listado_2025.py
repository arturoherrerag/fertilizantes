#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exporta las vistas materializadas avance_apoyados_2025_listado_ceda y
avance_apoyados_2025_listado_estado a CSV en UTF-8 (México).
"""

import os
import pandas as pd
from conexion import engine  # usa tu conexión centralizada

# === Rutas de salida ===
BASE_DIR = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS"
os.makedirs(BASE_DIR, exist_ok=True)

VISTAS = {
    "avance_apoyados_2025_listado_ceda": os.path.join(BASE_DIR, "avance_apoyados_2025_listado_ceda.csv"),
    "avance_apoyados_2025_listado_estado": os.path.join(BASE_DIR, "avance_apoyados_2025_listado_estado.csv"),
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