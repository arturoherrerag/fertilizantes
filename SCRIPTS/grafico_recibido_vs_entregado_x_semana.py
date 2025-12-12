import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import timedelta

# ──────────────────────────────────────────────────────────────
# 1. Diccionarios en español (para etiquetas)
# ──────────────────────────────────────────────────────────────
DIAS = {0: "lun", 1: "mar", 2: "mié", 3: "jue", 4: "vie", 5: "sáb", 6: "dom"}
MESES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
}

# ──────────────────────────────────────────────────────────────
# 2. Rutas de entrada / salida
# ──────────────────────────────────────────────────────────────
ruta_envios  = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/abasto_y_remanente_x_dia_sin_transito_2025.csv"
ruta_entregas = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/entregas_diarias_2025.csv"
ruta_salida  = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_comparativo_abasto_entregas_semanal.png"

# ──────────────────────────────────────────────────────────────
# 3. Cargar y depurar datos diarios
# ──────────────────────────────────────────────────────────────
env = pd.read_csv(ruta_envios,  parse_dates=['fecha'])
ent = pd.read_csv(ruta_entregas, parse_dates=['fecha'])

env = env[env['total_recibido_2025_ton']   > 0]
ent = ent[ent['total_ton_entregada']       > 0]

df = (
    env[['fecha', 'total_recibido_2025_ton']]
    .merge(ent[['fecha', 'total_ton_entregada']], how='outer', on='fecha')
    .fillna(0)
)

# ──────────────────────────────────────────────────────────────
# 4. Agrupar por semana (semana que FINALIZA en sábado) 
#    → 'W-SAT' genera un registro por cada sábado; así
#      la semana operativa (lun–sáb) queda completa.
# ──────────────────────────────────────────────────────────────
df_sem = (
    df.set_index('fecha')
      .resample('W-SAT')                # corte al sábado
      .sum(min_count=1)                 # evita semanas vacías
      .reset_index()
)

# Añade número de semana ISO y una etiqueta amigable
df_sem['semana'] = df_sem['fecha'].dt.isocalendar().week
df_sem['etiqueta'] = df_sem['fecha'].apply(
    lambda f: f"{DIAS[f.weekday()]}\n{f.day:02d}-{MESES[f.month]}"
)

# ──────────────────────────────────────────────────────────────
# 5. KPI totales
# ──────────────────────────────────────────────────────────────
tot_recibido  = int(df_sem['total_recibido_2025_ton'].sum())
tot_entregado = int(df_sem['total_ton_entregada'].sum())
tot_inv       = tot_recibido - tot_entregado

# ──────────────────────────────────────────────────────────────
# 6. Gráfico comparativo semanal
# ──────────────────────────────────────────────────────────────
plt.style.use('default')
fig, ax = plt.subplots(figsize=(12.6, 6.5))
ax.set_facecolor('white')
fig.patch.set_facecolor('none')

# Área + línea de envíos
ax.fill_between(df_sem['fecha'], df_sem['total_recibido_2025_ton'],
                color='#1e5b4f', alpha=0.25)
ax.plot(df_sem['fecha'], df_sem['total_recibido_2025_ton'],
        color='#1e5b4f', linewidth=2.5, marker='o', markersize=4,
        label='Fertilizante Recibido (ton)')

# Área + línea de entregas
ax.fill_between(df_sem['fecha'], df_sem['total_ton_entregada'],
                color='#a57f2c', alpha=0.25)
ax.plot(df_sem['fecha'], df_sem['total_ton_entregada'],
        color='#a57f2c', linewidth=2.5, marker='o', markersize=4,
        label='Fertilizante Entregado (ton)')

# Líneas verticales en cada sábado (fin de semana operativa)
for f in df_sem['fecha']:
    ax.axvline(x=f, color='gray', linestyle='--', linewidth=0.5, alpha=0.4)

# Etiquetas en eje X (formato sab dd-mes)
ax.set_xticks(df_sem['fecha'])
ax.set_xticklabels(df_sem['etiqueta'], fontsize=8, ha='center', color='black')

# Anotar el valor semanal más alto entre recibido/entregado
fila_max = df_sem.loc[
    (df_sem['total_recibido_2025_ton'] + df_sem['total_ton_entregada']).idxmax()
]
valor_max = max(fila_max['total_recibido_2025_ton'],
                fila_max['total_ton_entregada'])
ax.annotate(f"{valor_max:,}".replace(",", ","),
            xy=(fila_max['fecha'], valor_max),
            xytext=(fila_max['fecha'], valor_max + 1000),
            textcoords='data',
            arrowprops=dict(facecolor='black', arrowstyle='->', lw=0.8),
            ha='center', fontsize=9, color='black')

# KPIs
kpi_y_valor   = 1.13
kpi_y_etiqueta = 1.08
ax.text(0.50, kpi_y_valor,   f"{tot_recibido:,}".replace(",", ","), transform=ax.transAxes,
        ha='left', fontsize=13, color='#1e5b4f',  weight='bold')
ax.text(0.50, kpi_y_etiqueta, "Recibido Total (ton)", transform=ax.transAxes,
        ha='left', fontsize=10.5, color='#444444')

ax.text(0.70, kpi_y_valor,   f"{tot_entregado:,}".replace(",", ","), transform=ax.transAxes,
        ha='left', fontsize=13, color='#a57f2c',  weight='bold')
ax.text(0.70, kpi_y_etiqueta, "Entregado Total (ton)", transform=ax.transAxes,
        ha='left', fontsize=10.5, color='#444444')

ax.text(0.90, kpi_y_valor,   f"{tot_inv:,}".replace(",", ","), transform=ax.transAxes,
        ha='left', fontsize=13, color='#c60052', weight='bold')
ax.text(0.90, kpi_y_etiqueta, "Inventarios (ton)", transform=ax.transAxes,
        ha='left', fontsize=10.5, color='#444444')

# Ejes y estilo
ax.set_xlabel("Semana (fin de semana operativa)", fontsize=12, color='black')
ax.set_ylabel("Toneladas", fontsize=12, color='black')
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'.replace(",", ",")))
ax.spines['top'].set_visible(False)
ax.yaxis.grid(True, linestyle='--', linewidth=0.5, alpha=0.5)
ax.set_ylim(0, df_sem[['total_recibido_2025_ton','total_ton_entregada']].max().max() * 1.2)

# Leyenda
ax.legend(loc='upper left', bbox_to_anchor=(0, 1.13),
          facecolor='none', edgecolor='none',
          labelcolor='black', fontsize=9)

# Guardar
plt.tight_layout()
plt.savefig(ruta_salida, dpi=300, bbox_inches='tight', transparent=True)
plt.close()
print(f"✅ Gráfico comparativo semanal exportado en: {ruta_salida}")
