import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime

# Diccionarios español
DIAS = {0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vier", 5: "sáb", 6: "dom"}
MESES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
}

# Rutas
ruta_envios = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/envios_diarios_2025.csv"
ruta_entregas = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entregas_diarias_2025.csv"
ruta_salida = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_comparativo_abasto_entregas.png"

# Cargar datos
df_envios = pd.read_csv(ruta_envios)
df_entregas = pd.read_csv(ruta_entregas)

# Fechas y limpieza
df_envios['fecha'] = pd.to_datetime(df_envios['fecha'])
df_entregas['fecha'] = pd.to_datetime(df_entregas['fecha'])
df_envios = df_envios[df_envios['total_ton_enviadas'] > 0]
df_entregas = df_entregas[df_entregas['total_ton_entregada'] > 0]

# Unir en base a fechas
df = pd.merge(
    df_envios[['fecha', 'total_ton_enviadas']],
    df_entregas[['fecha', 'total_ton_entregada']],
    on='fecha',
    how='outer'
)
df = df.sort_values('fecha').fillna(0)

# Penúltima fecha válida
fecha_previa = df['fecha'].sort_values().iloc[-2]

# Fechas clave (viernes + fecha previa)
df['es_viernes'] = df['fecha'].dt.dayofweek == 4
etiquetas_fecha = df[df['es_viernes']]['fecha'].tolist()
if fecha_previa not in etiquetas_fecha:
    etiquetas_fecha.append(fecha_previa)

# Picos altos
umbral_envios = df['total_ton_enviadas'].quantile(0.75)
umbral_entregas = df['total_ton_entregada'].quantile(0.75)
picos = df[
    (df['fecha'].isin(etiquetas_fecha)) |
    (df['total_ton_enviadas'] >= umbral_envios) |
    (df['total_ton_entregada'] >= umbral_entregas)
]

# Etiquetas eje X
etiquetas_formato = [
    f"{DIAS[f.weekday()]}\n{f.day:02d}-{MESES[f.month]}" for f in etiquetas_fecha
]

# Crear gráfico
plt.figure(figsize=(12.6, 5.9))
plt.style.use('default')
ax = plt.gca()
ax.set_facecolor('white')
plt.gcf().patch.set_facecolor('none')

# Área + línea de envíos
plt.fill_between(df['fecha'], df['total_ton_enviadas'], color='#1e5b4f', alpha=0.25)
plt.plot(df['fecha'], df['total_ton_enviadas'], color='#1e5b4f', linewidth=2.5, marker='o', markersize=4, label='Fertilizante Recibido (ton)')

# Área + línea de entregas
plt.fill_between(df['fecha'], df['total_ton_entregada'], color='#a57f2c', alpha=0.25)
plt.plot(df['fecha'], df['total_ton_entregada'], color='#a57f2c', linewidth=2.5, marker='o', markersize=4, label='Fertilizante Entregado (ton)')

# Líneas verticales
for fecha in etiquetas_fecha:
    plt.axvline(x=fecha, color='gray', linestyle='--', linewidth=0.5, alpha=0.5)

# Anotar valores
for _, fila in picos.iterrows():
    y_max = max(fila['total_ton_enviadas'], fila['total_ton_entregada'])
    plt.text(
        fila['fecha'],
        y_max + 300,
        f"{int(y_max):,}".replace(",", ","),
        color='black',
        fontsize=8.5,
        ha='center',
        va='bottom',
        rotation=90
    )

# Ejes
plt.xlabel("Fecha", fontsize=12, color='black')
plt.ylabel("Fertilizante Recibido (ton) y Fertilizante Entregado (ton)", fontsize=12, color='black')
plt.xlim([df['fecha'].min(), df['fecha'].max()])
ax.get_yaxis().set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'.replace(",", ",")))
plt.yticks(color='black')
ax.spines['top'].set_visible(False)
ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.5)

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

# Guardar
plt.tight_layout()
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight', transparent=True)
plt.close()

print(f"✅ Gráfico comparativo abasto vs entregas exportado en: {ruta_salida}")
