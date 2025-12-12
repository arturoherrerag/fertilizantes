import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.dates as mdates
from datetime import datetime, timedelta

# 📁 Rutas
base_dir = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/TABLAS_DINAMICAS/"
salida_img = "/Users/Arturo/AGRICULTURA/INFORMES/Img_Temporales/grafico_comparativo_acumulado_2025.png"

archivos = {
    "proyeccion": "proyeccion_abasto_x_dia_2025.csv",
    "abasto": "abasto_y_remanente_x_dia_2025.csv",
    "entregas": "entregas_diarias_2025.csv",
    "pedidos": "pedidos_y_remanentes_por_dia_2025.csv"
}

def cargar_y_acumular(ruta, columna_valor, nuevo_nombre):
    df = pd.read_csv(ruta)
    df['fecha'] = pd.to_datetime(df['fecha'])
    df = df[df[columna_valor] > 0].sort_values('fecha')
    df[nuevo_nombre] = df[columna_valor].cumsum()
    return df[['fecha', nuevo_nombre]]

df_lineas = {
    "Abasto Proyectado": cargar_y_acumular(base_dir + archivos["proyeccion"], "abasto_proy_2025_ton", "Abasto Proyectado"),
    "Abasto Real": cargar_y_acumular(base_dir + archivos["abasto"], "total_recibido_2025_ton", "Abasto Real"),
    "Entregado": cargar_y_acumular(base_dir + archivos["entregas"], "total_ton_entregada", "Entregado"),
    "Remanente + Pedido": cargar_y_acumular(base_dir + archivos["pedidos"], "pedido_total_ton", "Remanente + Pedido")
}

colores = {
    "Abasto Proyectado": "#888888",
    "Remanente + Pedido": "#b68a1e",
    "Abasto Real": "#1e5b4f",
    "Entregado": "#662e3c"
}

# 📐 Figura de 27 cm de ancho (≈10.63 in)
fig = plt.figure(figsize=(10.63, 6.1))
ax = fig.add_axes([0.07, 0.1, 0.73, 0.8])  # Más espacio para gráfico

# Ocultar los bordes superior y derecho
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

valores_finales = {}
for nombre, df_l in df_lineas.items():
    color = colores[nombre]
    plt.plot(df_l["fecha"], df_l[nombre], label=nombre, color=color, linewidth=2.5, marker='o', markersize=3)
    plt.fill_between(df_l["fecha"], 0, df_l[nombre], color=color, alpha=0.15)

    punto_final = df_l.iloc[-1]
    valores_finales[nombre] = punto_final[nombre]

    etiqueta = f"{int(punto_final[nombre] / 1000):,} mil".replace(",", ".")
    plt.text(punto_final["fecha"], punto_final[nombre], etiqueta,
             color=color, fontsize=9, ha='center', va='bottom', fontweight='bold')

meses_es = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"
}
def formato_fecha_es(fecha, _):
    if isinstance(fecha, (int, float)):
        fecha = mdates.num2date(fecha)
    return f"{meses_es[fecha.month]}\n{fecha.year}"

ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(FuncFormatter(formato_fecha_es))
plt.xticks(rotation=0)
plt.xlabel("Fecha", fontsize=12)
plt.ylabel("Toneladas de Fertilizante", fontsize=12)
plt.grid(True, linestyle="--", alpha=0.5)
ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x / 1000):,} mil".replace(",", ".")))
ax.set_facecolor("white")
plt.gcf().patch.set_facecolor("none")

plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.12), ncol=4, fontsize=9, frameon=False)

ayer = datetime.today().date() - timedelta(days=1)
valores_ayer = {}

for nombre, df_l in df_lineas.items():
    df_l["fecha"] = pd.to_datetime(df_l["fecha"]).dt.date
    df_filtrado = df_l[df_l["fecha"] <= ayer]
    valores_ayer[nombre] = df_filtrado.iloc[-1][nombre] if not df_filtrado.empty else 0

remanente_y_pedido = valores_ayer.get("Remanente + Pedido", 0)
abasto_y_remanente = valores_ayer.get("Abasto Real", 0)
proyectado = valores_ayer.get("Abasto Proyectado", 0)
entregado = valores_ayer.get("Entregado", 0)

brechas = {
    "Remanente y Pedido": remanente_y_pedido,
    "Abasto y Remanente": abasto_y_remanente,
    "[Abasto Proyectado]": proyectado,
    "Entregado": entregado,
    "Abasto Real - Proyectado": abasto_y_remanente - proyectado,
    "Entregado - Proyectado": entregado - proyectado,
    "Pedidos por Atender": remanente_y_pedido - abasto_y_remanente,
    "Inventarios más tránsito": abasto_y_remanente - entregado
}

# 📊 Panel ampliado
panel_x = 1.035
top_y = 0.92
linea = 0

def draw_panel_text(text, size=10, color="black", bold=False, box=False, bg="#444444", fg="white", extra_line_gap=0):
    global linea
    y = top_y - 0.043 * linea
    weight = 'bold' if bold else 'normal'
    if box:
        plt.text(panel_x, y, text, fontsize=size, fontweight=weight, color=fg,
                 transform=ax.transAxes, verticalalignment='top',
                 bbox=dict(facecolor=bg, edgecolor='none', boxstyle='round,pad=0.3'))
    else:
        plt.text(panel_x, y, text, fontsize=size, fontweight=weight, color=color,
                 transform=ax.transAxes, verticalalignment='top')
    linea += 1 + extra_line_gap

draw_panel_text("Estadísticas al día de\nAyer (ton.)", bold=True, box=True, extra_line_gap=1)

for clave in ["Remanente y Pedido", "Abasto y Remanente", "[Abasto Proyectado]", "Entregado"]:
    draw_panel_text(f"{brechas[clave]:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), bold=True)
    draw_panel_text(clave, size=9)

draw_panel_text("Brechas entre Líneas\nal día de Ayer (ton.)", bold=True, box=True,
                bg="#b68a1e", fg="white", extra_line_gap=1)

for clave in ["Abasto Real - Proyectado", "Entregado - Proyectado", "Pedidos por Atender", "Inventarios más tránsito"]:
    draw_panel_text(f"{brechas[clave]:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), bold=True)
    draw_panel_text(clave, size=9)

# 💾 Exportar sin recorte
plt.savefig(salida_img, dpi=300, transparent=True)
plt.close()
print(f"✅ Gráfico final ajustado y guardado en: {salida_img}")
