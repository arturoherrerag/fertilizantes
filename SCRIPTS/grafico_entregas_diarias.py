import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime

# Diccionarios
DIAS = {0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vier", 5: "sáb", 6: "dom"}
MESES = {1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
         7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"}

# Rutas
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entregas_diarias_2025.csv"
ruta_salida = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_entregas_diarias.png"

# Leer datos
df = pd.read_csv(ruta_csv)
df['fecha'] = pd.to_datetime(df['fecha'])
df = df[df['fecha'].notnull()]
df = df[df['derechohabientes_apoyados'] > 0]

# Clasificación por día
df['dia_semana'] = df['fecha'].dt.dayofweek
df['semana'] = df['fecha'].dt.isocalendar().week
df['año'] = df['fecha'].dt.isocalendar().year

# ➕ Obtener viernes
df['es_viernes'] = df['dia_semana'] == 4
etiquetas_fecha = sorted(df[df['es_viernes']]['fecha'].tolist())

# ➕ Obtener mayor valor lunes a jueves por semana
df_lj = df[df['dia_semana'].isin([0, 1, 2, 3])]
max_por_semana = df_lj.loc[df_lj.groupby(['año', 'semana'])['derechohabientes_apoyados'].idxmax()]

# 📌 Fechas a etiquetar
fechas_valor = set(etiquetas_fecha) | set(max_por_semana['fecha'])
df_valores = df[df['fecha'].isin(fechas_valor)]

# Etiquetas X en dos líneas
etiquetas_formato = [
    f"{DIAS[f.weekday()]}\n{f.day:02d}-{MESES[f.month]}" for f in etiquetas_fecha
]

# 🎯 Indicadores generales
total_entregado = df['derechohabientes_apoyados'].sum()
maximo_entrega = df['derechohabientes_apoyados'].max()
promedio_entrega = df['derechohabientes_apoyados'].mean()
meta_total = 2062239
porcentaje_cumplido = (total_entregado / meta_total) * 100

# 🎨 Crear gráfico
fig, ax = plt.subplots(figsize=(12.6, 6.3))  # ligeramente más alto para panel
plt.style.use('default')
ax.set_facecolor('white')
fig.patch.set_facecolor('none')

# Línea principal
ax.plot(df['fecha'], df['derechohabientes_apoyados'],
        color='#9b2247', linewidth=2.5, marker='o', markersize=5)

# Mostrar valores seleccionados
for _, fila in df_valores.iterrows():
    ax.text(
        fila['fecha'],
        fila['derechohabientes_apoyados'] + 500,
        f"{int(fila['derechohabientes_apoyados']):,}".replace(",", ","),
        color='black',
        fontsize=8.5,
        ha='center',
        va='bottom',
        rotation=90
    )

# Líneas verticales para los viernes
for fecha in etiquetas_fecha:
    ax.axvline(x=fecha, color='gray', linestyle='--', linewidth=0.5, alpha=0.5)

# Ejes y estilo
ax.set_xlabel('Fecha de Entrega', fontsize=12, color='black')
ax.set_ylabel('Derechohabientes Apoyados', fontsize=12, color='black')
ax.set_xlim([df['fecha'].min(), df['fecha'].max()])
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'.replace(",", ",")))
ax.tick_params(axis='y', colors='black')
ax.spines['top'].set_visible(False)
ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.5)

# Etiquetas X solo viernes
ax.set_xticks(etiquetas_fecha)
ax.set_xticklabels(etiquetas_formato, fontsize=8, ha='center', color='black')

# 🔷 Panel superior con KPIs (colores y estilos ajustados)
color_dato = '#9b2247'
color_etiqueta = '#444444'

fig.text(0.13, 0.95,
         f"{int(total_entregado):,}".replace(",", ",") + f" ({porcentaje_cumplido:.2f}%)",
         fontsize=14, fontweight='bold', color=color_dato, ha='center')
fig.text(0.13, 0.91, "Total Atendidos", fontsize=10, fontweight='bold', color=color_etiqueta, ha='center')

fig.text(0.5, 0.95,
         f"{int(maximo_entrega):,}".replace(",", ","),
         fontsize=14, fontweight='bold', color=color_dato, ha='center')
fig.text(0.5, 0.91, "Máximo de Entrega", fontsize=10, fontweight='bold', color=color_etiqueta, ha='center')

fig.text(0.87, 0.95,
         f"{int(promedio_entrega):,}".replace(",", ","),
         fontsize=14, fontweight='bold', color=color_dato, ha='center')
fig.text(0.87, 0.91, "Promedio Entrega / Día", fontsize=10, fontweight='bold', color=color_etiqueta, ha='center')

# Guardar gráfico
plt.tight_layout(rect=[0, 0, 1, 0.89])  # deja espacio arriba para KPIs
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight', transparent=True)
plt.close()

print(f"✅ Gráfico de entregas diarias exportado en: {ruta_salida}")
