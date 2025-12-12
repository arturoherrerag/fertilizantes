#!/usr/bin/env python3
import pandas as pd
import hashlib
import datetime
import re
import unicodedata
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from sqlalchemy import text
from conexion import engine  # tu conexión centralizada

# --- Configuración ------------------------------------------------------
CREDS_FILE     = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/inventarios-2025-pf-69b1417ea0df.json"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEET_ID = "1YLmnolQ4TPab8LcVcAHTTqgYECddMfNAJGqohvBPZLM"
# Ahora incluye hasta la columna AS para observaciones y otra_persona
RANGE          = "'Respuestas de formulario 1'!A1:AS"
TABLE_NAME     = "inventarios_diarios_ceda_2025_campo"


def normalize_columns(cols):
    new_cols = []
    for col in cols:
        c = col.lower().strip()
        c = unicodedata.normalize('NFD', c).encode('ascii', 'ignore').decode('utf-8')
        c = re.sub(r"[^\w\s]", '', c)
        c = re.sub(r"\s+", '_', c)
        c = re.sub(r"_+", '_', c)
        new_cols.append(c)
    return new_cols


def main():
    # 1) Autenticación con Google Sheets API
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    # 2) Obtener datos brutos
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE
    ).execute()
    values = resp.get("values", [])
    if not values:
        print("⚠️ No hay datos para importar.")
        return

    # 3) Encabezados y filas
    headers, rows = values[0], values[1:]
    n_cols = len(headers)
    padded_rows = [row + [''] * (n_cols - len(row)) for row in rows]
    df = pd.DataFrame(padded_rows, columns=headers)

    # 4) Normalizar nombres de columnas
    df.columns = normalize_columns(df.columns)

    # 5) Consolidar columnas de selección de CEDA
    centro_cols = [col for col in df.columns if col.startswith('selecciona_tu_centro_de_distribucion')]
    df['id_ceda_agricultura'] = df[centro_cols].replace('', pd.NA).bfill(axis=1).iloc[:, 0]
    df.drop(columns=centro_cols, inplace=True)

    # 6) Renombrar campos fijos y nuevos
    rename_map = {
        'marca_temporal': 'fecha_y_hora',
        'direccion_de_correo_electronico': 'correo_electronico',
        'selecciona_la_fecha_del_dia_a_registrar': 'fecha_registro',
        'selecciona_tu_estado': 'estado',
        'quien_o_quienes_fueron_los_responsables_de_realizar_el_levantamiento_del_inventario': 'quien_reporta',
        'nombre_del_coz': 'nombre_de_coz',
        'nombre_del_rc': 'nombre_del_rc',
        'nombre_del_ce': 'nombre_del_ce',
        'numero_de_bultos_de_dap_en_el_centro_de_distribucion': 'bultos_dap',
        'numero_de_bultos_de_urea_en_el_centro_de_distribucion': 'bultos_urea',
        'observaciones': 'observaciones',
        'nombre_de_la_persona_que_realizo_el_levantamiento': 'otra_persona'
    }
    df.rename(columns=rename_map, inplace=True)

    # 7) Convertir tipos de bultos
    if 'bultos_dap' in df.columns:
        df['bultos_dap'] = pd.to_numeric(df['bultos_dap'], errors='coerce').fillna(0).astype(int)
    if 'bultos_urea' in df.columns:
        df['bultos_urea'] = pd.to_numeric(df['bultos_urea'], errors='coerce').fillna(0).astype(int)

    # 8) Convertir fechas
    if 'fecha_y_hora' in df.columns:
        df['fecha_y_hora'] = pd.to_datetime(df['fecha_y_hora'], errors='coerce')
    if 'fecha_registro' in df.columns:
        df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], errors='coerce').dt.date

    # 9) Seleccionar columnas finales
    cols_to_keep = [
        'fecha_y_hora', 'correo_electronico', 'fecha_registro', 'estado',
        'id_ceda_agricultura', 'bultos_dap', 'bultos_urea',
        'quien_reporta', 'nombre_de_coz', 'nombre_del_rc', 'nombre_del_ce',
        'observaciones', 'otra_persona'
    ]
    missing = [c for c in cols_to_keep if c not in df.columns]
    if missing:
        print(f"⚠️ Columnas faltantes tras procesamiento: {missing}")
    df_final = df[cols_to_keep]

    # 10) Generar hash de fila
    df_final['hash_respuesta'] = (
        df_final.astype(str).agg('|'.join, axis=1)
          .apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    )

    # 11) Insertar en PostgreSQL y eliminar duplicados
    with engine.begin() as conn:
        df_final.to_sql(
            name=TABLE_NAME,
            con=conn,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=5000
        )
        conn.execute(text(f"""
            DELETE FROM {TABLE_NAME} a
            USING {TABLE_NAME} b
            WHERE a.ctid < b.ctid AND a.hash_respuesta = b.hash_respuesta;
        """))

    # 12) Log de ejecución
    ahora = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ Importadas {len(df_final)} filas en '{TABLE_NAME}' — {ahora}")

if __name__ == "__main__":
    main()
