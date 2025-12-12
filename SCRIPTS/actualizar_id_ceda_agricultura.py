# actualizar_id_ceda_agricultura.py

from conexion import psycopg_conn

# Los dos bloques SQL que ejecutarás en orden
sql_updates = [
    """
    BEGIN;

    UPDATE derechohabientes_padrones_compilado_2025 AS dpc
    SET id_ceda_agricultura = TRIM(dp.id_ceda)
    FROM derechohabientes_padrones_2025 AS dp
    WHERE dp.acuse_estatal = dpc.acuse_estatal
      -- No sobreescribir con null/cadena vacía
      AND NULLIF(TRIM(dp.id_ceda), '') IS NOT NULL
      -- Evitar trabajo innecesario si ya coincide
      AND dpc.id_ceda_agricultura IS DISTINCT FROM TRIM(dp.id_ceda);

    COMMIT;
    """,
    """
    BEGIN;

    WITH dh_norm AS (
      SELECT DISTINCT ON (TRIM(UPPER(acuse_estatal)))
             TRIM(UPPER(acuse_estatal)) AS acuse_n,
             TRIM(cdf_entrega)::text    AS cdf_txt
      FROM derechohabientes
      WHERE NULLIF(TRIM(cdf_entrega)::text, '') IS NOT NULL
      ORDER BY TRIM(UPPER(acuse_estatal))
    ),
    cand AS (
      SELECT dpc.acuse_estatal, dh.cdf_txt
      FROM derechohabientes_padrones_compilado_2025 dpc
      JOIN dh_norm dh
        ON dh.acuse_n = TRIM(UPPER(dpc.acuse_estatal))
      WHERE dpc.id_ceda_agricultura::text IS DISTINCT FROM dh.cdf_txt
    )
    UPDATE derechohabientes_padrones_compilado_2025 dpc
    SET id_ceda_agricultura = cand.cdf_txt
    FROM cand
    WHERE dpc.acuse_estatal = cand.acuse_estatal;

    COMMIT;
    """
]

def main():
    try:
        with psycopg_conn.cursor() as cur:
            for query in sql_updates:
                cur.execute(query)
        psycopg_conn.commit()
        print("✅ Actualización de id_ceda_agricultura completada correctamente.")
    except Exception as e:
        psycopg_conn.rollback()
        print("❌ Error durante la actualización:", e)

if __name__ == "__main__":
    main()