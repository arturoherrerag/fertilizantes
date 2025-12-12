import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime

# 📌 Estado a graficar
estado = "VERACRUZ"
nombre_archivo = estado.lower().replace("á", "a").replace("é", "e").replace("í", "i") \
                               .replace("ó", "o").replace("ú", "u").replace("ñ", "n")

# 📂 Rutas de entrada y salida
ruta_csv = f"/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/envios_{nombre_archivo}.csv"
ruta_salida = f"/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_envios_{nombre_archivo}.png"

# 🧠 Diccionarios etiquetas
DIAS = {0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vier", 5: "sáb", 6: "dom"}
MESES = {1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
         7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"}

# 📊 Leer datos
df = pd.read_csv(ruta_csv)
df['fecha'] = pd.to_datetime(df['fecha'])
df = df[df['fecha'].notnull()]
df = df[df['total_ton_enviadas'] > 0]

# 🕒 Fecha previa (penúltimo día)
if len(df) >= 2:
    fecha_previa = df['fecha'].sort_values().iloc[-2]
else:
    fecha_previa = df['fecha'].max()

# 📆 Etiquetas viernes y picos
df['es_viernes'] = df['fecha'].dt.dayofweek == 4
etiquetas_fecha = df[df['es_viernes']]['fecha'].tolist()
if fecha_previa not in etiquetas_fecha:
    etiquetas_fecha.append(fecha_previa)

etiquetas_formato = [
    f"{DIAS[f.weekday()]}\n{f.day:02d}-{MESES[f.month]}" for f in etiquetas_fecha
]

umbral = df['total_ton_enviadas'].quantile(0.75)
fechas_valor = sorted(set(etiquetas_fecha + df[df['total_ton_enviadas'] >= umbral]['fecha'].tolist()))
df_valores = df[df['fecha'].isin(fechas_valor)]

# 🎨 Crear gráfico
plt.figure(figsize=(12.6, 5.9))
plt.style.use('default')
ax = plt.gca()
ax.set_facecolor('white')
plt.gcf().patch.set_facecolor('none')

# Áreas sombreadas
plt.fill_between(df['fecha'], df['total_ton_enviadas'], color='#a57f2c', alpha=0.35)
plt.fill_between(df['fecha'], df['urea_ton_enviadas'], color='#6bbf59', alpha=0.35)
plt.fill_between(df['fecha'], df['dap_ton_enviadas'], color='#204e4a', alpha=0.35)

# Líneas con puntos
plt.plot(df['fecha'], df['total_ton_enviadas'], color='#a57f2c', linewidth=2.2, marker='o', markersize=4, label='Total Enviado (ton)')
plt.plot(df['fecha'], df['urea_ton_enviadas'], color='#6bbf59', linewidth=1.8, linestyle='--', marker='o', markersize=3, label='UREA Enviada (ton)')
plt.plot(df['fecha'], df['dap_ton_enviadas'], color='#204e4a', linewidth=1.8, linestyle='--', marker='o', markersize=3, label='DAP Enviado (ton)')

# Líneas viernes
for fecha in etiquetas_fecha:
    plt.axvline(x=fecha, color='gray', linestyle='--', linewidth=0.5, alpha=0.5)

# Anotar valores
for _, fila in df_valores.iterrows():
    plt.text(
        fila['fecha'],
        fila['total_ton_enviadas'] + 300,
        f"{int(fila['total_ton_enviadas']):,}",
        color='black',
        fontsize=8.3,
        ha='center',
        va='bottom',
        rotation=90
    )

# Ejes y estilo
plt.xlabel("Fecha", fontsize=12, color='black')
plt.ylabel("Toneladas Enviadas", fontsize=12, color='black')
plt.xlim([df['fecha'].min(), df['fecha'].max()])
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'))
plt.yticks(color='black')
ax.spines['top'].set_visible(False)
ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.4)

# Eje X
ax.set_xticks(etiquetas_fecha)
ax.set_xticklabels(etiquetas_formato, fontsize=8, ha='center', color='black')

# Leyenda
plt.legend(
    loc='upper left',
    bbox_to_anchor=(0, 1.25),
    facecolor='none',
    edgecolor='none',
    labelcolor='black',
    fontsize=9
)

# Guardar gráfico
plt.tight_layout()
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight', transparent=True)
plt.close()

print(f"✅ Gráfico generado: {ruta_salida}")
