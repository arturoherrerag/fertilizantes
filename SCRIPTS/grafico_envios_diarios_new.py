import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime

# Diccionarios para etiquetas
DIAS = {0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vier", 5: "sáb", 6: "dom"}
MESES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
}

# Rutas
ruta_csv = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/envios_diarios_2025.csv"
ruta_salida = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_envios_diarios_powerbi.png"

# Leer datos
df = pd.read_csv(ruta_csv)
df['fecha'] = pd.to_datetime(df['fecha'])
df = df[df['fecha'].notnull()]
df = df[df['total_ton_enviadas'] > 0]

# Fecha previa
fecha_previa = df['fecha'].sort_values().iloc[-2]

# Etiquetas de viernes + penúltima
df['es_viernes'] = df['fecha'].dt.dayofweek == 4
etiquetas_fecha = df[df['es_viernes']]['fecha'].tolist()

# Quitar el último viernes si no es la última fecha del dataset
ultimo_viernes = max(etiquetas_fecha) if etiquetas_fecha else None
ultima_fecha = df['fecha'].max()
if ultimo_viernes and ultimo_viernes != ultima_fecha:
    etiquetas_fecha = [f for f in etiquetas_fecha if f != ultimo_viernes]

# Asegurar que esté la penúltima fecha (por si no es viernes)
if fecha_previa not in etiquetas_fecha:
    etiquetas_fecha.append(fecha_previa)

# Etiquetas formateadas
etiquetas_formato = [
    f"{DIAS[f.weekday()]}\n{f.day:02d}-{MESES[f.month]}" for f in etiquetas_fecha
]

# Fechas para mostrar valores (picos y viernes)
umbral = df['total_ton_enviadas'].quantile(0.75)
fechas_valor = sorted(set(etiquetas_fecha + df[df['total_ton_enviadas'] >= umbral]['fecha'].tolist()))
df_valores = df[df['fecha'].isin(fechas_valor)]

# Crear gráfico
plt.figure(figsize=(12.6, 5.9))
plt.style.use('default')
ax = plt.gca()
ax.set_facecolor('white')
plt.gcf().patch.set_facecolor('none')

# Áreas
plt.fill_between(df['fecha'], df['total_ton_enviadas'], color='#a57f2c', alpha=0.35)
plt.fill_between(df['fecha'], df['urea_ton_enviadas'], color='#6bbf59', alpha=0.35)
plt.fill_between(df['fecha'], df['dap_ton_enviadas'], color='#204e4a', alpha=0.35)

# Calcular KPIs
total_dap = df['dap_ton_enviadas'].sum()
total_urea = df['urea_ton_enviadas'].sum()
total_envio = df['total_ton_enviadas'].sum()

# Líneas con puntos
plt.plot(df['fecha'], df['total_ton_enviadas'], color='#a57f2c', linewidth=2.2, marker='o', markersize=4)
plt.plot(df['fecha'], df['urea_ton_enviadas'], color='#6bbf59', linewidth=1.8, linestyle='--', marker='o', markersize=3)
plt.plot(df['fecha'], df['dap_ton_enviadas'], color='#204e4a', linewidth=1.8, linestyle='--', marker='o', markersize=3)

# Líneas invisibles para leyenda personalizada
linea_total, = plt.plot([], [], color='#a57f2c', linewidth=2.2, marker='o', markersize=4)
linea_urea, = plt.plot([], [], color='#6bbf59', linewidth=1.8, linestyle='--', marker='o', markersize=3)
linea_dap, = plt.plot([], [], color='#204e4a', linewidth=1.8, linestyle='--', marker='o', markersize=3)

# Texto de leyenda con separadores y negritas solo en Total Enviado
label_total = r'Total Enviado (ton): $\bf{' + f'{total_envio:,.0f}' + r'}$'
label_urea = f'UREA Enviada (ton): {total_urea:,.0f}'
label_dap = f'DAP Enviada (ton): {total_dap:,.0f}'

# Insertar literal el separador "|" entre columnas, usando padding
ax.legend(
    handles=[linea_total, linea_urea, linea_dap],
    labels=[
        label_total,
        '|  ' + label_urea,
        '|  ' + label_dap
    ],
    loc='upper center',
    bbox_to_anchor=(0.5, 1.22),
    ncol=3,
    frameon=False,
    fontsize=10,
    labelcolor='black',
    columnspacing=4.5,
    handletextpad=2.5,
    borderpad=0.8
)


# Líneas verticales para viernes
for fecha in etiquetas_fecha:
    plt.axvline(x=fecha, color='gray', linestyle='--', linewidth=0.5, alpha=0.5)

# Anotar valores
for _, fila in df_valores.iterrows():
    plt.text(
        fila['fecha'],
        fila['total_ton_enviadas'] + 300,
        f"{int(fila['total_ton_enviadas']):,}".replace(",", ","),
        color='black',
        fontsize=8.3,
        ha='center',
        va='bottom',
        rotation=90
    )

# Ejes
plt.xlabel("Fecha", fontsize=12, color='black')
plt.ylabel("Toneladas Enviadas", fontsize=12, color='black')
plt.xlim([df['fecha'].min(), df['fecha'].max()])
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'.replace(",", ",")))
plt.yticks(color='black')
ax.spines['top'].set_visible(False)
ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.4)

# Etiquetas eje X
ax.set_xticks(etiquetas_fecha)
ax.set_xticklabels(etiquetas_formato, fontsize=8, ha='center', color='black')

# Guardar
plt.tight_layout()
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight', transparent=True)
plt.close()

print(f"✅ Gráfico estilo Power BI generado en: {ruta_salida}")
