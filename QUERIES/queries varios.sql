
-- para encontrar datos en una base que no están en otra

SELECT *
FROM derechohabientes dh
WHERE NOT EXISTS (
  SELECT 1
  FROM derechohabientes_padrones_compilado_2025 dpc
  WHERE dpc.acuse_estatal = dh.acuse_estatal
);