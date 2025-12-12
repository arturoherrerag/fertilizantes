#!/usr/bin/env python3
import pandas as pd
import hashlib
import datetime
import re
import unicodedata
from io import StringIO
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from sqlalchemy import text
from conexion import engine, psycopg_conn

import traceback
import sys
import os

# --- Configuración ------------------------------------------------------
CREDS_FILE     = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/inventarios-2025-pf-69b1417ea0df.json"
SCOPES         = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEET_ID = "1YLmnolQ4TPab8LcVcAHTTqgYECddMfNAJGqohvBPZLM"
RANGE          = "'Respuestas de formulario 1'!A1:AT"
TABLE_NAME     = "inventarios_diarios_ceda_2025_campo"
LOG_FILE       = "error_inventarios.txt"

# -----------------------------------------------------------------------
def normalize_columns(cols):
    new_cols = []
    for col in cols:
        c = col.lower().strip()
        c = unicodedata.normalize("NFD", c).encode("ascii", "ignore").decode("utf-8")
        c = re.sub(r"[^\w\s]", "", c)
        c = re.sub(r"\s+", "_", c)
        c = re.sub(r"_+", "_", c)
        new_cols.append(c)
    return new_cols

# -----------------------------------------------------------------------
def main():
    # 1) autenticación Google Sheets
    creds   = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)

    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE
    ).execute()
    values = resp.get("values", [])
    if not values:
        print("⚠️  No hay datos para importar.")
        return

    headers, rows = values[0], values[1:]
    n_cols = len(headers)
    padded = [row + [""] * (n_cols - len(row)) for row in rows]
    df = pd.DataFrame(padded, columns=headers)

    # 2) normalizar headers
    df.columns = normalize_columns(df.columns)

    # 3) consolidar centro de distribución
    centro_cols = [c for c in df.columns if c.startswith("selecciona_tu_centro_de_distribucion")]
    df["id_ceda_agricultura"] = (
        df[centro_cols].replace("", pd.NA).bfill(axis=1).iloc[:, 0]
    )
    df.drop(columns=centro_cols, inplace=True)

    # 4) renombrar columnas
    rename_map = {
        "marca_temporal": "fecha_y_hora",
        "direccion_de_correo_electronico": "correo_electronico",
        "selecciona_la_fecha_del_dia_a_registrar": "fecha_registro",
        "selecciona_tu_estado": "estado",
        "quien_o_quienes_fueron_los_responsables_de_realizar_el_levantamiento_del_inventario": "quien_reporta",
        "nombre_del_coz": "nombre_de_coz",
        "nombre_del_rc": "nombre_del_rc",
        "nombre_del_ce": "nombre_del_ce",
        "numero_de_bultos_de_dap_en_el_centro_de_distribucion": "bultos_dap",
        "numero_de_bultos_de_urea_en_el_centro_de_distribucion": "bultos_urea",
        "observaciones": "observaciones",
        "nombre_de_la_persona_que_realizo_el_levantamiento": "otra_persona",
        "nombre_de_responsable_en_direccion_general": "personal_direccion",
    }
    df.rename(columns=rename_map, inplace=True)

    # 5) convertir tipos
    if "bultos_dap" in df.columns:
        df["bultos_dap"] = pd.to_numeric(df["bultos_dap"], errors="coerce").fillna(0).astype(int)
    if "bultos_urea" in df.columns:
        df["bultos_urea"] = pd.to_numeric(df["bultos_urea"], errors="coerce").fillna(0).astype(int)

    if "fecha_y_hora" in df.columns:
        df["fecha_y_hora"] = pd.to_datetime(df["fecha_y_hora"], errors="coerce", dayfirst=True)
    if "fecha_registro" in df.columns:
        df["fecha_registro"] = pd.to_datetime(df["fecha_registro"], errors="coerce", dayfirst=True).dt.date

    # 6) columnas finales
    keep = [
        "fecha_y_hora", "correo_electronico", "fecha_registro", "estado",
        "id_ceda_agricultura", "bultos_dap", "bultos_urea",
        "quien_reporta", "nombre_de_coz", "nombre_del_rc", "nombre_del_ce",
        "observaciones", "otra_persona", "personal_direccion",
    ]
    df_final = df[keep]

    # 7) eliminar registros sin fecha
    df_final = df_final[df_final["fecha_y_hora"].notnull()]
    if df_final.empty:
        print("⚠️  Todos los registros tienen fecha_y_hora vacía. No se insertará nada.")
        return

    # 8) clave CEDA en MAYÚSCULAS
    df_final["id_ceda_agricultura"] = df_final["id_ceda_agricultura"].astype(str).str.strip().str.upper()

    # 9) normalización ligera
    for col in df_final.columns:
        if col != "id_ceda_agricultura" and df_final[col].dtype == "object":
            df_final[col] = df_final[col].astype(str).str.strip().str.lower()

    # 10) hash de fila
    df_final["hash_respuesta"] = (
        df_final.astype(str).agg("|".join, axis=1)
        .apply(lambda x: hashlib.md5(x.encode()).hexdigest())
    )

    # 11) filtrar duplicados contra la BD
    with engine.connect() as conn:
        existentes = pd.read_sql(f"SELECT hash_respuesta FROM {TABLE_NAME}", conn)["hash_respuesta"]
    df_nuevos = df_final[~df_final["hash_respuesta"].isin(existentes)]

    # 💡 Eliminar duplicados internos (mismo hash), conservar el último
    df_nuevos = df_nuevos.drop_duplicates(subset="hash_respuesta", keep="last")

    if df_nuevos.empty:
        print("ℹ️  No hay registros nuevos para insertar.")
        return

    # 12) COPY FROM STDIN
    buf = StringIO()
    df_nuevos.to_csv(buf, sep="|", index=False, header=False, na_rep="")
    buf.seek(0)

    cols_pg = ", ".join(df_nuevos.columns)
    with psycopg_conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {TABLE_NAME} ({cols_pg}) FROM STDIN WITH (FORMAT CSV, DELIMITER '|')",
            buf
        )
        psycopg_conn.commit()

    print(f"✅ Insertadas {len(df_nuevos)} filas nuevas en '{TABLE_NAME}' — {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

# -----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("❌ Se produjo un error en la ejecución del script:\n\n")
            traceback.print_exc(file=f)
        print(f"⚠️  Ocurrió un error. Revisa el archivo: {LOG_FILE}")
