-- Se actualiza el id_ceda_agricultura de la tabla derechohabientes_padrones_compilado_2025
-- con el id_ceda de erechohabientes_padrones_2025 o con el cdf_entreta de la tabla derechohabientes


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


-- refrescar vistas materializadas

REFRESH MATERIALIZED VIEW mv_metas_ceda_estatus_2025;
REFRESH MATERIALIZED VIEW mv_avances_diarios_ceda_estatus_2025;
REFRESH MATERIALIZED VIEW mv_meta_vs_avance_diario_ceda_estatus_2025;