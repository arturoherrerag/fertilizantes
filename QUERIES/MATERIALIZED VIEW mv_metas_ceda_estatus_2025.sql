{\rtf1\ansi\ansicpg1252\cocoartf2865
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww12920\viewh17980\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 select\
*\
from\
mv_metas_ceda_estatus_2025;\
\
\
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_metas_ceda_estatus_2025 AS\
WITH dpc_unique AS (\
  -- Blindaje por si hubiera registros repetidos por acuse\
  SELECT DISTINCT ON (dpc.acuse_estatal)\
         dpc.acuse_estatal,\
         dpc.id_ceda_agricultura,\
         dpc.estatus,\
         CASE WHEN dpc.estatus ILIKE '%Nuevo Ingreso%' THEN 'NUEVO INGRESO'\
              ELSE 'ACCESO DIRECTO' END AS incorporacion,\
         dpc.fecha_de_publicacion,\
         dpc.superficie,\
         dpc."dap_(ton)"  AS dap_ton,\
         dpc."urea_(ton)" AS urea_ton\
  FROM derechohabientes_padrones_compilado_2025 dpc\
  ORDER BY dpc.acuse_estatal\
),\
rd_dedup AS (\
  SELECT DISTINCT ON (id_ceda_agricultura)\
         coordinacion_estatal AS unidad_operativa,\
         estado,\
         zona_operativa,\
         id_ceda_agricultura,\
         nombre_cedas\
  FROM red_distribucion\
  ORDER BY id_ceda_agricultura\
)\
SELECT\
  rd.unidad_operativa,\
  rd.estado,\
  rd.zona_operativa,\
  rd.id_ceda_agricultura,\
  rd.nombre_cedas,\
  u.estatus,\
  u.incorporacion,\
  u.fecha_de_publicacion,\
  COUNT(*)                                 AS meta_dh,\
  SUM(u.superficie)                        AS meta_superficie,\
  SUM(u.dap_ton)                           AS meta_dap_ton,\
  SUM(u.urea_ton)                          AS meta_urea_ton,\
  SUM(u.dap_ton + u.urea_ton)              AS meta_total_ton\
FROM dpc_unique u\
LEFT JOIN rd_dedup rd ON rd.id_ceda_agricultura = u.id_ceda_agricultura\
GROUP BY\
  rd.unidad_operativa, rd.estado, rd.zona_operativa, rd.id_ceda_agricultura, rd.nombre_cedas,\
  u.estatus, u.incorporacion, u.fecha_de_publicacion;\
\
CREATE INDEX IF NOT EXISTS ix_mv_metas_ceda_estatus_2025_keys\
  ON mv_metas_ceda_estatus_2025 (id_ceda_agricultura, estado, unidad_operativa, zona_operativa, estatus, incorporacion, fecha_de_publicacion);}