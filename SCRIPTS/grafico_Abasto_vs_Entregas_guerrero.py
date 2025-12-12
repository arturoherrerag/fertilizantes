import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime

# 📌 Estado
estado = "GUERRERO"
nombre_archivo = estado.lower().replace("á", "a").replace("é", "e").replace("í", "i") \
                               .replace("ó", "o").replace("ú", "u").replace("ñ", "n")

# 📂 Rutas
ruta_abasto = f"/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/abasto_{nombre_archivo}.csv"
ruta_entregas = f"/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entregas_{nombre_archivo}.csv"
ruta_salida = f"/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_abasto_{nombre_archivo}.png"

# 📅 Diccionarios etiquetas
DIAS = {0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vier", 5: "sáb", 6: "dom"}
MESES = {1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
         7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"}

# 🧪 Cargar datos
df_abasto = pd.read_csv(ruta_abasto)
df_entregas = pd.read_csv(ruta_entregas)

df_abasto['fecha'] = pd.to_datetime(df_abasto['fecha'])
df_entregas['fecha'] = pd.to_datetime(df_entregas['fecha'])

# ❗ Excluir datos vacíos
df_abasto = df_abasto[df_abasto['total_recibido_2025_ton'] > 0]
df_entregas = df_entregas[df_entregas['total_ton_entregada'] > 0]

# 🔗 Unir datos
df = pd.merge(
    df_abasto[['fecha', 'total_recibido_2025_ton']],
    df_entregas[['fecha', 'total_ton_entregada']],
    on='fecha', how='outer'
).fillna(0).sort_values('fecha')

df['dia_semana'] = df['fecha'].dt.dayofweek
df['semana'] = df['fecha'].dt.isocalendar().week

# 📌 Etiquetas X (viernes)
etiquetas_fecha = df[df['dia_semana'] == 4]['fecha'].tolist()
etiquetas_formato = [f"{DIAS[f.weekday()]}\n{f.day:02d}-{MESES[f.month]}" for f in etiquetas_fecha]

# 🔺 Días con valor alto de lunes a jueves
lunes_jueves = df[df['dia_semana'].isin([0, 1, 2, 3])]
idx_max = (lunes_jueves.assign(suma=lunes_jueves['total_recibido_2025_ton'] + lunes_jueves['total_ton_entregada'])
           .groupby('semana')['suma'].idxmax())
fechas_anotar = sorted(set(etiquetas_fecha + df.loc[idx_max, 'fecha'].tolist()))

# 🎨 Crear gráfico
fig, ax = plt.subplots(figsize=(12.6, 6.5))
ax.set_facecolor('white')
plt.gcf().patch.set_facecolor('none')

# Áreas y líneas
plt.fill_between(df['fecha'], df['total_recibido_2025_ton'], color='#1e5b4f', alpha=0.25)
plt.plot(df['fecha'], df['total_recibido_2025_ton'], color='#1e5b4f', linewidth=2.5, marker='o', label='Recibido (ton)')

plt.fill_between(df['fecha'], df['total_ton_entregada'], color='#a57f2c', alpha=0.25)
plt.plot(df['fecha'], df['total_ton_entregada'], color='#a57f2c', linewidth=2.5, marker='o', label='Entregado (ton)')

# Líneas viernes
for f in etiquetas_fecha:
    plt.axvline(x=f, color='gray', linestyle='--', linewidth=0.5, alpha=0.5)

# Anotar valores relevantes (si no exceden 6000)
for _, fila in df[df['fecha'].isin(fechas_anotar)].iterrows():
    y = max(fila['total_recibido_2025_ton'], fila['total_ton_entregada'])
    if y <= 6000:
        plt.text(fila['fecha'], y + 300, f"{int(y):,}", fontsize=8, ha='center', va='bottom', rotation=90)

# Anotar valor máximo
fila_max = df.loc[(df['total_recibido_2025_ton'] + df['total_ton_entregada']).idxmax()]
valor_max = max(fila_max['total_recibido_2025_ton'], fila_max['total_ton_entregada'])
if valor_max <= 6000:
    plt.annotate(
        f"{int(valor_max):,}",
        xy=(fila_max['fecha'], 5800), xytext=(fila_max['fecha'], 6000),
        textcoords='data',
        arrowprops=dict(facecolor='black', arrowstyle='->', lw=0.8),
        ha='center', fontsize=9, color='black'
    )

# KPIs
recibido = int(df['total_recibido_2025_ton'].sum())
entregado = int(df['total_ton_entregada'].sum())
inventario = recibido - entregado

plt.text(0.50, 1.13, f"{recibido:,}", transform=ax.transAxes, ha='left', fontsize=13, color='#1e5b4f', weight='bold')
plt.text(0.50, 1.08, "Recibido Total (ton)", transform=ax.transAxes, ha='left', fontsize=10.5)

plt.text(0.70, 1.13, f"{entregado:,}", transform=ax.transAxes, ha='left', fontsize=13, color='#a57f2c', weight='bold')
plt.text(0.70, 1.08, "Entregado Total (ton)", transform=ax.transAxes, ha='left', fontsize=10.5)

plt.text(0.90, 1.13, f"{inventario:,}", transform=ax.transAxes, ha='left', fontsize=13, color='#c60052', weight='bold')
plt.text(0.90, 1.08, "Inventarios (ton)", transform=ax.transAxes, ha='left', fontsize=10.5)

# Ejes
plt.xlabel("Fecha")
plt.ylabel("Toneladas")
plt.xlim(df['fecha'].min(), df['fecha'].max())
plt.ylim([0, 6000])
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'))
ax.set_xticks(etiquetas_fecha)
ax.set_xticklabels(etiquetas_formato, fontsize=8, ha='center')
plt.yticks(color='black')
ax.spines['top'].set_visible(False)
ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.5)

# Leyenda y guardar
plt.legend(loc='upper left', bbox_to_anchor=(0, 1.13), fontsize=9)
plt.tight_layout()
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight', transparent=True)
plt.close()

print(f"✅ Gráfico de GUERRERO con límite 6,000 generado en: {ruta_salida}")
