import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import text
from conexion import engine

# === 1. Cargar divisiones estatales ===
path_geojson = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/MAPAS/georef-mexico-state@public.geojson"
estados = gpd.read_file(path_geojson)

# === 2. Cargar CEDAS desde PostgreSQL ===
query = """
SELECT id_ceda_agricultura, nombre_cedas, estado, latitud, longitud
FROM red_distribucion
WHERE latitud IS NOT NULL AND longitud IS NOT NULL;
"""
df_cedas = pd.read_sql(query, engine)

# === 3. Convertir CEDAS a GeoDataFrame ===
gdf_cedas = gpd.GeoDataFrame(
    df_cedas,
    geometry=gpd.points_from_xy(df_cedas["longitud"], df_cedas["latitud"]),
    crs="EPSG:4326"
)

# === 4. Crear figura ===
fig, ax = plt.subplots(figsize=(10, 10))

# Rellenar los estados con color #1e5b4f y bordes blancos
estados.plot(ax=ax, facecolor="#e6d194", edgecolor="white", linewidth=0.7)

# Dibujar los CEDAS encima
gdf_cedas.plot(ax=ax, color="#9b2247", markersize=10, alpha=0.9)

# Limpiar gráfico
ax.set_axis_off()
ax.set_xlim(-118, -86)
ax.set_ylim(14.5, 33.5)

# === 5. Guardar como imagen final ===
ruta_salida = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/MAPAS/cedas_mapa_estatal_relleno.png"
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight')
plt.close()

print(f"✅ Mapa generado con fondo interno en: {ruta_salida}")
