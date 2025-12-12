#!/usr/bin/env python3
"""
actualizar_directorio_red.py
Complementa/directorio con:
  1) RESPONSABLE DE CEDA (uno por id_ceda_agricultura)
  2) COORDINADOR OPERATIVO DE ZONA (uno por estado+zona_operativa)
Flujo:
  - Borra del directorio ambos cargos
  - Vuelve a generarlos desde red_distribucion
Uso:
  $ python actualizar_directorio_red.py
Requiere: conexion.py (exporta `engine`)
"""
import pandas as pd
from sqlalchemy import text
from conexion import engine

# Consulta base sobre red_distribucion
Q = """
SELECT
  estado,
  coordinacion_estatal      AS unidad_operativa,
  zona_operativa,
  id_ceda_agricultura,
  responsable_cedas,
  rc_correo,
  rc_telefono,
  coordinador_operativo,
  coz_correo,
  coz_telefono
FROM red_distribucion;
"""

import pandas as pd

import pandas as pd

import pandas as pd

def build_responsables(df_red):
    # 1) Filtramos todos los CEDAs con id_ceda_agricultura (aunque responsable_cedas esté vacío)
    df = df_red.loc[
        df_red["id_ceda_agricultura"].notna(),
        [
            "estado",
            "unidad_operativa",   # tu alias de coordinacion_estatal
            "zona_operativa",
            "id_ceda_agricultura",
            "responsable_cedas",
            "rc_correo",
            "rc_telefono"
        ]
    ]

    # 2) Añadimos las columnas fijas y mapeamos responsable_cedas → nombre_completo, etc.
    df = df.assign(
        cargo="RESPONSABLE DE CEDA",
        curp=None,
        nombre_completo=df["responsable_cedas"].fillna(""),
        correo_electronico=df["rc_correo"].fillna(""),
        telefono=df["rc_telefono"].fillna(""),
        comentarios=None,
        fecha_actualizacion=pd.Timestamp.now()
    )

    # 3) Seleccionamos en una lista EXACTA las columnas finales en el orden deseado
    final_cols = [
        "estado",
        "unidad_operativa",
        "zona_operativa",
        "id_ceda_agricultura",
        "cargo",
        "curp",
        "nombre_completo",
        "correo_electronico",
        "telefono",
        "comentarios",
        "fecha_actualizacion"
    ]
    return df[final_cols]


def build_coordinadores(df_red):
    # Tomar una sola fila por (estado,zona_operativa)
    df = (
        df_red.dropna(subset=["coordinador_operativo"])
              .drop_duplicates(subset=["estado","zona_operativa"], keep="first")
    )
    return df.assign(
        unidad_operativa = df["unidad_operativa"],
        id_ceda_agricultura = "N/A",
        cargo="COORDINADOR OPERATIVO DE ZONA",
        curp=None,
        nombre_completo=df["coordinador_operativo"],
        correo_electronico=df["coz_correo"],
        telefono=df["coz_telefono"],
        comentarios=None,
        fecha_actualizacion=pd.Timestamp.now()
    )[
        ["estado","unidad_operativa","zona_operativa",
         "id_ceda_agricultura","cargo","curp",
         "nombre_completo","correo_electronico",
         "telefono","comentarios","fecha_actualizacion"]
    ]

def main():
    # 1) Leer tabla red_distribucion
    df_red = pd.read_sql(Q, engine)

    # 2) Generar DataFrames
    df_rc  = build_responsables(df_red)
    df_coz = build_coordinadores(df_red)
    df_all = pd.concat([df_rc, df_coz], ignore_index=True)

    with engine.begin() as conn:
        # A) Eliminar registros antiguos
        conn.execute(text("""
            DELETE FROM directorio
              WHERE cargo IN
                ('RESPONSABLE DE CEDA','COORDINADOR OPERATIVO DE ZONA');
        """))
        # B) Insertar los nuevos
        df_all.to_sql(
            "directorio",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000
        )

    print(f"🔄 Directorio red_distribucion actualizado: {len(df_all):,} filas agregadas.")

if __name__ == "__main__":
    main()
