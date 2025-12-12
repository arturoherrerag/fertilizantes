import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# Rutas
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entregas_semanales_2025.csv"
ruta_salida = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_entregas_semanales.png"

# Leer datos
df = pd.read_csv(ruta_csv)
df = df[df['derechohabientes_apoyados'] > 0]

# Etiquetas del eje X: "abr - S14"
df['etiqueta_x'] = df['nombre_mes'].str[:3] + ' - S - ' + df['semana'].astype(str)

# 🎯 KPIs generales
total_entregado = df['derechohabientes_apoyados'].sum()
maximo_entrega = df['derechohabientes_apoyados'].max()
promedio_entrega = df['derechohabientes_apoyados'].mean()
meta_total = 2062239
porcentaje_cumplido = (total_entregado / meta_total) * 100

# 🎨 Crear gráfico
fig, ax = plt.subplots(figsize=(12.6, 6.3))
plt.style.use('default')
ax.set_facecolor('white')
fig.patch.set_facecolor('none')

# Línea principal
ax.plot(df['etiqueta_x'], df['derechohabientes_apoyados'],
        color='#9b2247', linewidth=2.5, marker='o', markersize=5)

# Valores destacados
for idx, row in df.iterrows():
    ax.text(
        row['etiqueta_x'],
        row['derechohabientes_apoyados'] + 1500,
        f"{int(row['derechohabientes_apoyados']):,}",
        color='black',
        fontsize=8.5,
        ha='center',
        va='bottom',
        rotation=0
    )

# Ejes y estilo
ax.set_xlabel('Semana', fontsize=12, color='black')
ax.set_ylabel('Derechohabientes Apoyados', fontsize=12, color='black')
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'))
ax.tick_params(axis='y', colors='black')
ax.spines['top'].set_visible(False)
ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.5)
ax.set_xticks(range(len(df)))
ax.set_xticklabels(df['etiqueta_x'], fontsize=8, ha='center', color='black', rotation=45)

# 🔷 Panel superior con KPIs
color_dato = '#9b2247'
color_etiqueta = '#444444'

fig.text(0.13, 0.95,
         f"{int(total_entregado):,} ({porcentaje_cumplido:.2f}%)",
         fontsize=14, fontweight='bold', color=color_dato, ha='center')
fig.text(0.13, 0.91, "Total Atendidos", fontsize=10, fontweight='bold', color=color_etiqueta, ha='center')

fig.text(0.5, 0.95,
         f"{int(maximo_entrega):,}",
         fontsize=14, fontweight='bold', color=color_dato, ha='center')
fig.text(0.5, 0.91, "Máximo de Entrega", fontsize=10, fontweight='bold', color=color_etiqueta, ha='center')

fig.text(0.87, 0.95,
         f"{int(promedio_entrega):,}",
         fontsize=14, fontweight='bold', color=color_dato, ha='center')
fig.text(0.87, 0.91, "Promedio Entrega / Semana", fontsize=10, fontweight='bold', color=color_etiqueta, ha='center')

# Guardar gráfico
plt.tight_layout(rect=[0, 0, 1, 0.89])
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight', transparent=True)
plt.close()

print(f"✅ Gráfico de entregas semanales exportado en: {ruta_salida}")
