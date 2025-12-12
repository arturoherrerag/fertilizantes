import pandas as pd
from conexion import engine  # ✅ Conexión centralizada

# Consulta SQL
query = """
SELECT
    rd.id_ceda_agricultura,
    COALESCE(ps.dap_por_suministrar, 3) AS dap_atender_sigap,
    COALESCE(ps.urea_por_suministrar, 3) AS urea_atender_sigap,
    COALESCE(ps.catr_dap, 3) AS cap_dap_disp_sigap,
    COALESCE(ps.catr_urea, 3) AS cap_urea_disp_sigap,
    COALESCE(ps.itr_dap, 3) AS dap_inv_sigap,
    COALESCE(ps.itr_urea, 3) AS urea_inv_sigap
FROM red_distribucion rd
LEFT JOIN pedidos_sigap ps
    ON rd.id_ceda_agricultura = ps.folio_cdf
ORDER BY rd.id_ceda_agricultura;
"""

# Leer los datos en un DataFrame
df = pd.read_sql(query, engine)

# Ruta del archivo CSV
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/pedidos_sigap_td.csv"

# Exportar a CSV con codificación UTF-8
df.to_csv(ruta_salida, index=False, encoding='utf-8-sig')

print("✅ Archivo 'pedidos_sigap_td.csv' exportado correctamente.")
