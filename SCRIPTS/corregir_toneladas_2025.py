#!/usr/bin/env python3
from conexion import engine
import pandas as pd
from sqlalchemy import text

print("🔄 Comparando y actualizando registros entre derechohabientes y derechohabientes_padrones_2025...")

# Consulta para detectar registros distintos
query = """
SELECT
    d.acuse_estatal,
    p.dap_ton,
    p.urea_ton
FROM derechohabientes d
JOIN derechohabientes_padrones_2025 p ON d.acuse_estatal = p.acuse_estatal
WHERE d.ton_dap_entregada IS DISTINCT FROM p.dap_ton
   OR d.ton_urea_entregada IS DISTINCT FROM p.urea_ton;
"""

df = pd.read_sql(query, engine)

if df.empty:
    print("✅ No hay registros que necesiten actualización.")
else:
    print(f"⚠️  Se encontraron {len(df)} registros con diferencias. Procediendo con la actualización...")

    with engine.begin() as conn:  # begin() incluye automáticamente commit
        for _, row in df.iterrows():
            acuse = row['acuse_estatal']
            dap_ton = row['dap_ton'] or 0
            urea_ton = row['urea_ton'] or 0
            dap_actual = dap_ton / 0.025
            urea_actual = urea_ton / 0.025

            update_stmt = text("""
                UPDATE derechohabientes
                SET
                    ton_dap_entregada = :dap_ton,
                    ton_urea_entregada = :urea_ton,
                    dap_anio_actual = :dap_actual,
                    urea_anio_actual = :urea_actual
                WHERE acuse_estatal = :acuse_estatal;
            """)
            conn.execute(update_stmt, {
                'dap_ton': dap_ton,
                'urea_ton': urea_ton,
                'dap_actual': dap_actual,
                'urea_actual': urea_actual,
                'acuse_estatal': acuse
            })

    print("✅ Actualización completada con éxito.")
