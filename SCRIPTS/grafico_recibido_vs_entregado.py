import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# Diccionarios español
DIAS = {0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vier", 5: "sáb", 6: "dom"}
MESES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
}

# Rutas
ruta_envios = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/abasto_y_remanente_x_dia_sin_transito_2025.csv"
ruta_entregas = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entregas_diarias_2025.csv"
ruta_salida = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_comparativo_abasto_entregas.png"

# Cargar datos
df_envios = pd.read_csv(ruta_envios)
df_entregas = pd.read_csv(ruta_entregas)

# Fechas y limpieza
df_envios['fecha'] = pd.to_datetime(df_envios['fecha'])
df_entregas['fecha'] = pd.to_datetime(df_entregas['fecha'])
df_envios = df_envios[df_envios['total_recibido_2025_ton'] > 0]
df_entregas = df_entregas[df_entregas['total_ton_entregada'] > 0]

# Unir en base a fechas
df = pd.merge(
    df_envios[['fecha', 'total_recibido_2025_ton']],
    df_entregas[['fecha', 'total_ton_entregada']],
    on='fecha',
    how='outer'
)
df = df.sort_values('fecha').fillna(0)

# Agregar día de la semana y semana ISO
df['dia_semana'] = df['fecha'].dt.dayofweek
df['semana'] = df['fecha'].dt.isocalendar().week

# Etiquetas en eje X (solo viernes)
fechas_viernes = df[df['dia_semana'] == 4]['fecha'].tolist()
etiquetas_fecha = fechas_viernes
etiquetas_formato = [
    f"{DIAS[f.weekday()]}\n{f.day:02d}-{MESES[f.month]}" for f in etiquetas_fecha
]

# Días con anotaciones
lunes_a_jueves = df[df['dia_semana'].isin([0, 1, 2, 3])]
idx_maximos = (lunes_a_jueves
               .assign(suma=lunes_a_jueves['total_recibido_2025_ton'] + lunes_a_jueves['total_ton_entregada'])
               .groupby('semana')['suma']
               .idxmax())
maximos_lj = df.loc[idx_maximos]
fechas_para_anotar = sorted(set(fechas_viernes + maximos_lj['fecha'].tolist()))

# Crear gráfico
fig, ax = plt.subplots(figsize=(12.6, 6.5))
plt.style.use('default')
ax.set_facecolor('white')
plt.gcf().patch.set_facecolor('none')

# Área + línea de envíos
plt.fill_between(df['fecha'], df['total_recibido_2025_ton'], color='#1e5b4f', alpha=0.25)
plt.plot(df['fecha'], df['total_recibido_2025_ton'], color='#1e5b4f', linewidth=2.5, marker='o', markersize=4, label='Fertilizante Recibido (ton)')

# Área + línea de entregas
plt.fill_between(df['fecha'], df['total_ton_entregada'], color='#a57f2c', alpha=0.25)
plt.plot(df['fecha'], df['total_ton_entregada'], color='#a57f2c', linewidth=2.5, marker='o', markersize=4, label='Fertilizante Entregado (ton)')

# Líneas verticales
for fecha in etiquetas_fecha:
    plt.axvline(x=fecha, color='gray', linestyle='--', linewidth=0.5, alpha=0.5)

# Anotar días clave: solo el valor más alto entre recibido y entregado
for _, fila in df[df['fecha'].isin(fechas_para_anotar)].iterrows():
    fecha = fila['fecha']
    recibido = fila['total_recibido_2025_ton']
    entregado = fila['total_ton_entregada']

    if recibido > entregado and recibido <= 15000:
        plt.text(fecha, recibido + 300, f"{int(recibido):,}".replace(",", ","),
                 color='#1e5b4f', fontsize=8.5, ha='center', va='bottom', rotation=90)
    elif entregado >= recibido and entregado <= 15000:
        plt.text(fecha, entregado + 300, f"{int(entregado):,}".replace(",", ","),
                 color='#a57f2c', fontsize=8.5, ha='center', va='bottom', rotation=90)

# Anotar valor más alto
fila_max = df.loc[(df['total_recibido_2025_ton'] + df['total_ton_entregada']).idxmax()]
fecha_max = fila_max['fecha']
valor_max = max(fila_max['total_recibido_2025_ton'], fila_max['total_ton_entregada'])
plt.annotate(
    f"{int(valor_max):,}".replace(",", ","),
    xy=(fecha_max, 15000), xytext=(fecha_max, 15200), textcoords='data',
    arrowprops=dict(facecolor='black', arrowstyle='->', lw=0.8),
    ha='center', fontsize=9, color='black'
)

# KPIs (en línea horizontal derecha)
recibido_total = int(df['total_recibido_2025_ton'].sum())
entregado_total = int(df['total_ton_entregada'].sum())
inventario_total = recibido_total - entregado_total

# Posiciones en el mismo nivel de la leyenda pero a la derecha
kpi_y_valor = 1.13
kpi_y_etiqueta = 1.08

plt.text(0.50, kpi_y_valor, f"{recibido_total:,}".replace(",", ","), transform=ax.transAxes,
         ha='left', fontsize=13, color='#1e5b4f', weight='bold')
plt.text(0.50, kpi_y_etiqueta, "Recibido Total (ton)", transform=ax.transAxes,
         ha='left', fontsize=10.5, color='#444444')

plt.text(0.70, kpi_y_valor, f"{entregado_total:,}".replace(",", ","), transform=ax.transAxes,
         ha='left', fontsize=13, color='#a57f2c', weight='bold')
plt.text(0.70, kpi_y_etiqueta, "Entregado Total (ton)", transform=ax.transAxes,
         ha='left', fontsize=10.5, color='#444444')

plt.text(0.90, kpi_y_valor, f"{inventario_total:,}".replace(",", ","), transform=ax.transAxes,
         ha='left', fontsize=13, color='#c60052', weight='bold')
plt.text(0.90, kpi_y_etiqueta, "Inventarios (ton)", transform=ax.transAxes,
         ha='left', fontsize=10.5, color='#444444')

# Ejes
plt.xlabel("Fecha", fontsize=12, color='black')
plt.ylabel("Fertilizante Recibido (ton) y Fertilizante Entregado (ton)", fontsize=12, color='black')
plt.xlim([df['fecha'].min(), df['fecha'].max()])
plt.ylim([0, 15000])
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
    bbox_to_anchor=(0, 1.13),
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
