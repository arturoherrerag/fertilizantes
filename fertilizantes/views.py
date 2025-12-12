from django.shortcuts import render
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest
from django.db import connection
from django.db.models import Sum
from decimal import Decimal
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_GET, require_POST
# ——— NUEVOS IMPORTS (ponlos junto a los demás) ———
from django.utils.timezone import now
# otras importaciones…
import csv, io, datetime                           # utilidades csv/BOM/fecha
from datetime import date, timedelta
from django.http import StreamingHttpResponse      # para la descarga csv en streaming
import math  # ⬅️ Asegúrate de tener esto al inicio del archivo
import pandas as pd
from .conexion import engine  # 👈 Nota el punto (import relativo)
from .forms import ComentarioCEDAForm
from .models import ComentarioCEDA
from django.shortcuts import redirect
from .conexion import engine, psycopg_conn
from sqlalchemy import text

from .models import VwDerechohabientesConContexto as DH  # modelo de la vista SQL

import subprocess

from django.utils.encoding import smart_str
import json

from django.utils.dateparse import parse_date

import base64
import tempfile
from pathlib import Path

from PIL import Image, ImageOps, ImageFilter
import pytesseract

try:
    from pdf2image import convert_from_bytes
    HAS_PDF = True
except Exception:
    HAS_PDF = False


# Campos disponibles para mostrar/descargar (nombre_db , etiqueta legible)
CAMPOS_DH = [
    ("acuse_estatal", "Acuse estatal"),
    ("clave_estado_predio_capturada", "Clave estado predio"),
    ("estado_predio_capturada", "Estado del predio"),
    ("clave_municipio_predio_capturada", "Clave municipio predio"),
    ("municipio_predio_capturada", "Municipio del predio"),
    ("clave_localidad_predio_capturada", "Clave localidad predio"),
    ("localidad_predio_capturada", "Localidad del predio"),
    ("id_nu_solicitud", "ID NU Solicitud"),
    ("cdf_entrega", "CDF entrega"),
    ("id_cdf_entrega", "ID CDF entrega"),
    ("curp_solicitud", "CURP solicitud"),
    ("curp_renapo", "CURP RENAPO"),
    ("curp_historica", "CURP histórica"),
    ("sn_primer_apellido", "Primer apellido"),
    ("sn_segundo_apellido", "Segundo apellido"),
    ("ln_nombre", "Nombre"),
    ("es_pob_indigena", "¿Población indígena?"),
    ("cultivo", "Cultivo"),
    ("ton_dap_entregada", "DAP entregado (ton)"),
    ("ton_urea_entregada", "UREA entregado (ton)"),
    ("fecha_entrega", "Fecha de entrega"),
    ("folio_persona", "Folio persona"),
    ("cuadernillo", "Cuadernillo"),
    ("nombre_ddr", "Nombre DDR"),
    ("clave_ddr", "Clave DDR"),
    ("nombre_cader_ventanilla", "CADER/Ventanilla"),
    ("clave_cader_ventanilla", "Clave CADER"),
    ("dap_anio_actual", "DAP año actual (ton)"),
    ("urea_anio_actual", "UREA año actual (ton)"),
    ("dap_remanente", "DAP remanente (ton)"),
    ("urea_remanente", "UREA remanente (ton)"),
    ("superficie_apoyada", "Superficie apoyada (ha)"),
    ("unidad_operativa", "Unidad operativa"),
    ("estado", "Estado"),
    ("id_ceda_agricultura", "ID CEDA Agricultura"),
]

def _aplicar_filtros_get(request, columnas):
    """
    Construye el WHERE dinámico y devuelve:
      where_sql   → cadena ' WHERE …' o vacía
      params      → lista para cursor.execute
      ctx_filtros → dict con valores seleccionados (para el template)
    """
    filtros, params, ctx = [], [], {}

    for col in columnas:
        v = request.GET.get(col)
        if v:
            filtros.append(f"{col} = %s")
            params.append(v)
        ctx[f"{col}_seleccionado"] = v

    # Rango de fechas opcional
    fi = request.GET.get("fecha_ini")
    ff = request.GET.get("fecha_fin")
    if fi:
        filtros.append("fecha >= %s")
        params.append(fi)
    if ff:
        filtros.append("fecha <= %s")
        params.append(ff)
    ctx.update({"fecha_ini": fi, "fecha_fin": ff})

    where_sql = " WHERE " + " AND ".join(filtros) if filtros else ""
    return where_sql, params, ctx


@login_required
def inicio(request):
    return render(request, 'fertilizantes/inicio.html')


# ---------- Actualización ----------
@login_required
def actualizar_bases(request):
    mensaje = ''
    
    if request.method == 'POST':
        if 'actualizar_todo' in request.POST:
            resultado = subprocess.run(
                ["/Users/Arturo/AGRICULTURA/FERTILIZANTES/ENTORNO/env/bin/python",
                 "/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/actualizar_todo.py"],
                capture_output=True,
                text=True
            )
            mensaje = resultado.stdout or resultado.stderr

        elif 'integrar_informe' in request.POST:
            resultado = subprocess.run(
                ["/Users/Arturo/AGRICULTURA/FERTILIZANTES/ENTORNO/env/bin/python",
                 "/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/Integrar_informe_2025.py"],
                capture_output=True,
                text=True
            )
            mensaje = resultado.stdout or resultado.stderr

    return render(request, 'fertilizantes/actualizacion.html', {'mensaje': mensaje})


# ---------- Sección Reportes ----------
@login_required
def reportes(request):
    return render(request, 'fertilizantes/reportes.html')

@login_required
def reporte_fletes_transito(request):
    return render(request, 'fertilizantes/reporte_fletes_transito.html')

@login_required
def reporte_inventarios_negativos(request):
    return render(request, 'fertilizantes/reporte_inventarios_negativos.html')

@login_required
def reporte_inventarios_negativos_remanente(request):
    return render(request, 'fertilizantes/reporte_inventarios_negativos_remanente.html')

@login_required
def reporte_entregas_nacionales(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM entregas_diarias_2025 LIMIT 100")
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
    return render(request, 'fertilizantes/reporte_entregas_nacionales.html', {'datos': datos})

# ---------- Visualización ----------

@login_required
def visualizacion(request):
    return render(request, 'fertilizantes/visualizacion.html')

@login_required
def dashboard_avance_nacional(request):
    return render(request, 'fertilizantes/visualizacion/dashboard_nacional.html', {
        "timestamp": int(now().timestamp())
    })

@login_required
def api_kpi_avance_nacional(request):
    filtros = {
        "unidad": request.GET.get("unidad_operativa"),
        "estado": request.GET.get("estado"),
        "ceda": request.GET.get("id_ceda"),
    }

    tipo_meta = request.GET.get("tipo_meta", "operativa")  # por defecto operativa

    condiciones = []
    params = []

    if filtros["unidad"]:
        condiciones.append("rd.coordinacion_estatal = %s")
        params.append(filtros["unidad"])
    if filtros["estado"]:
        condiciones.append("rd.estado = %s")
        params.append(filtros["estado"])
    if filtros["ceda"]:
        condiciones.append("rd.id_ceda_agricultura = %s")
        params.append(filtros["ceda"])

    where_clause = "WHERE " + " AND ".join(condiciones) if condiciones else ""

    usar_metas_oficiales = tipo_meta == "oficial" and not filtros["ceda"]

    if usar_metas_oficiales:
        meta_query = f"""
        WITH avance_estado AS (
            SELECT rd.estado,
                   SUM(a.dap_flete + a.urea_flete
                       + a.dap_transfer + a.urea_transfer
                       + a.dap_remanente + a.urea_remanente
                       - a.dap_transf_out - a.urea_transf_out
                       - a.dap_rem_out  - a.urea_rem_out)      AS abasto,
                   SUM(a.dap_dh + a.urea_dh)                   AS entregado,
                   SUM(a.dh_apoyados)                          AS dh_apoyados,
                   SUM(a.ha_apoyadas)                          AS ha_apoyadas
            FROM   avance_operativo_ceda_2025 a
            JOIN   red_distribucion rd ON rd.id_ceda_agricultura = a.id_ceda_agricultura
            {where_clause}                                     -- filtros (unidad, estado…)
            GROUP  BY rd.estado
        )

        SELECT
            SUM(m.total_ton)           AS meta_total_ton,
            SUM(av.abasto)             AS abasto,
            SUM(av.entregado)          AS entregado,
            SUM(av.dh_apoyados)        AS dh_apoyados,
            SUM(av.ha_apoyadas)        AS ha_apoyadas,
            SUM(m.derechohabientes)    AS meta_dh,
            SUM(m.superficie_ha)       AS meta_ha
        FROM   avance_estado av
        JOIN   metas_2025    m ON m.estado = av.estado;
        """    
    else:
        meta_query = f"""
            SELECT 
                SUM(a.meta_total_ton), 
                SUM(a.dap_flete + a.urea_flete + a.dap_transfer + a.urea_transfer + a.dap_remanente + a.urea_remanente - a.dap_transf_out - a.urea_transf_out - a.dap_rem_out - a.urea_rem_out),
                SUM(a.dap_dh + a.urea_dh),
                SUM(a.dh_apoyados),
                SUM(a.ha_apoyadas),
                SUM(a.meta_derechohabientes),
                SUM(a.meta_superficie_ha)
            FROM avance_operativo_ceda_2025 a
            JOIN red_distribucion rd ON a.id_ceda_agricultura = rd.id_ceda_agricultura
            {where_clause}
        """

    with connection.cursor() as cursor:
        cursor.execute(meta_query, params)
        meta, abasto, entregado, dh_apoyados, ha_apoyadas, meta_dh, meta_ha = cursor.fetchone()

    def porcentaje(a, m):
        if not m or m == 0:
            return 0.0
        return min(math.floor((a or 0) * 100 / m), 100)

    kpi = [
        {
            "id": "abasto",
            "titulo": "Abasto Recibido (ton)",
            "meta": meta,
            "avance": abasto,
        },
        {
            "id": "entregado",
            "titulo": "Fertilizante Entregado (ton)",
            "meta": meta,
            "avance": entregado,
        },
        {
            "id": "dh",
            "titulo": "Derechohabientes Apoyados",
            "meta": meta_dh or 2062239,
            "avance": dh_apoyados,
        },
        {
            "id": "superficie",
            "titulo": "Superficie Beneficiada (ha)",
            "meta": meta_ha or 3346768,
            "avance": ha_apoyadas,
        },
    ]

    for k in kpi:
        es_entero = k["id"] in ("dh", "superficie")
        meta = k["meta"] or 1
        avance = k["avance"] or 0
        pendiente = max(meta - avance, 0)

        k["pendiente"] = pendiente
        k["pct"] = porcentaje(avance, meta)

        if es_entero:
            k["meta_fmt"] = f"{int(meta):,}"
            k["avance_fmt"] = f"{int(avance):,}"
            k["pendiente_fmt"] = f"{int(pendiente):,}"
        else:
            k["meta_fmt"] = f"{meta:,.2f}"
            k["avance_fmt"] = f"{avance:,.2f}"
            k["pendiente_fmt"] = f"{pendiente:,.2f}"

        k["semaforo"] = (
            "verde" if k["pct"] >= 90 else
            "ambar" if k["pct"] >= 70 else
            "rojo"
        )

    return JsonResponse(kpi, safe=False)




@login_required
def api_filtros_kpi(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT coordinacion_estatal FROM red_distribucion ORDER BY coordinacion_estatal")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]

        cursor.execute("SELECT DISTINCT estado FROM red_distribucion ORDER BY estado")
        estados = [r[0] for r in cursor.fetchall() if r[0]]

    return JsonResponse({
        "unidades": unidades,
        "estados": estados
    })



@login_required
def resumen_estatal(request):
    return render(request, "fertilizantes/visualizacion/resumen_estatal.html")


@login_required
def api_tabla_resumen_por_estado(request):
    filtros = {
        "unidad": request.GET.get("unidad_operativa"),
        "estado": request.GET.get("estado"),
        "ceda": request.GET.get("id_ceda"),
    }
    tipo_meta = request.GET.get("tipo_meta", "operativa")

    condiciones = []
    params = []

    if filtros["unidad"]:
        condiciones.append("rd.coordinacion_estatal = %s")
        params.append(filtros["unidad"])
    if filtros["estado"]:
        condiciones.append("rd.estado = %s")
        params.append(filtros["estado"])
    if filtros["ceda"]:
        condiciones.append("rd.id_ceda_agricultura = %s")
        params.append(filtros["ceda"])

    where_clause = "WHERE " + " AND ".join(condiciones) if condiciones else ""

    usar_metas_oficiales = tipo_meta == "oficial" and not filtros["ceda"] and not filtros["unidad"]

    if usar_metas_oficiales:
        query = f"""
        WITH avance_estado AS (
            SELECT rd.estado,
                   SUM(a.dap_flete + a.urea_flete
                       + a.dap_transfer + a.urea_transfer
                       + a.dap_remanente + a.urea_remanente
                       - a.dap_transf_out - a.urea_transf_out
                       - a.dap_rem_out  - a.urea_rem_out)      AS abasto,
                   SUM(a.dap_dh + a.urea_dh)                   AS entregado,
                   SUM(a.dh_apoyados)                          AS dh_apoyados,
                   SUM(a.ha_apoyadas)                          AS ha_apoyadas
            FROM   avance_operativo_ceda_2025 a
            JOIN   red_distribucion rd ON rd.id_ceda_agricultura = a.id_ceda_agricultura
            {where_clause}
            GROUP  BY rd.estado
        )
        SELECT
            av.estado,
            COALESCE(SUM(m.total_ton), 0)         AS meta_total_ton,
            COALESCE(SUM(av.abasto), 0)           AS abasto,
            COALESCE(SUM(av.entregado), 0)        AS entregado,
            COALESCE(SUM(av.dh_apoyados), 0)      AS dh_apoyados,
            COALESCE(SUM(av.ha_apoyadas), 0)      AS ha_apoyadas,
            COALESCE(SUM(m.derechohabientes), 0)  AS meta_dh,
            COALESCE(SUM(m.superficie_ha), 0)     AS meta_ha
        FROM avance_estado av
        LEFT JOIN metas_2025 m ON m.estado = av.estado
        GROUP BY av.estado
        ORDER BY av.estado;
        """
    else:
        query = f"""
        SELECT
            rd.estado,
            SUM(a.meta_total_ton)               AS meta_total_ton,
            SUM(a.dap_flete + a.urea_flete
                + a.dap_transfer + a.urea_transfer
                + a.dap_remanente + a.urea_remanente
                - a.dap_transf_out - a.urea_transf_out
                - a.dap_rem_out - a.urea_rem_out) AS abasto,
            SUM(a.dap_dh + a.urea_dh)           AS entregado,
            SUM(a.dh_apoyados)                  AS dh_apoyados,
            SUM(a.ha_apoyadas)                  AS ha_apoyadas,
            SUM(a.meta_derechohabientes)        AS meta_dh,
            SUM(a.meta_superficie_ha)           AS meta_ha
        FROM avance_operativo_ceda_2025 a
        JOIN red_distribucion rd ON a.id_ceda_agricultura = rd.id_ceda_agricultura
        {where_clause}
        GROUP BY rd.estado
        ORDER BY rd.estado;
        """

    def porcentaje(avance, meta):
        if not meta or meta == 0:
            return 0
        return min(int((avance or 0) * 100 / meta), 100)

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    columnas = ["estado", "meta_total_ton", "abasto", "entregado", "dh_apoyados", "ha_apoyadas", "meta_dh", "meta_ha"]
    resultados = []

    for r in rows:
        row = dict(zip(columnas, r))
        resultados.append({
            "estado": row["estado"],
            "meta_total_ton": round(row["meta_total_ton"] or 0, 2),
            "abasto": round(row["abasto"] or 0, 2),
            "entregado": round(row["entregado"] or 0, 2),
            "pct_entregado": porcentaje(row["entregado"], row["meta_total_ton"]),
            "meta_dh": int(row["meta_dh"] or 0),
            "dh_apoyados": int(row["dh_apoyados"] or 0),
            "pct_dh": porcentaje(row["dh_apoyados"], row["meta_dh"]),
            "meta_ha": int(row["meta_ha"] or 0),
            "ha_apoyadas": int(row["ha_apoyadas"] or 0),
            "pct_ha": porcentaje(row["ha_apoyadas"], row["meta_ha"]),
        })

    return JsonResponse(resultados, safe=False)




# ---------- Vistas ----------    
@login_required
def vistas(request):
    return render(request, 'fertilizantes/vistas.html')

@login_required
def vista_fletes_transito(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM fletes_en_transito_resumen_estado ORDER BY max_dias_en_transito DESC")
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
    
    # Cálculo de totales
    totales = {
        'fletes_transito_dap': sum(d['fletes_transito_dap'] for d in datos),
        'fletes_transito_urea': sum(d['fletes_transito_urea'] for d in datos),
        'total_fletes_transito': sum(d['total_fletes_transito'] for d in datos),
        'ton_transito_dap': sum(d['ton_transito_dap'] for d in datos),
        'ton_transito_urea': sum(d['ton_transito_urea'] for d in datos),
        'total_ton_transito': sum(d['total_ton_transito'] for d in datos),
        'max_dias_en_transito': max(d['max_dias_en_transito'] for d in datos),
    }

    return render(request, 'fertilizantes/vista_fletes_transito.html', {
        'datos': datos,
        'totales': totales
    })



@login_required
def vista_fletes_transito_por_CEDA(request):
    filtro_unidad = request.GET.get('unidad_operativa', '')
    filtro_estado = request.GET.get('estado', '')
    filtro_zona = request.GET.get('zona_operativa', '')

    condiciones = []
    parametros = []

    if filtro_unidad:
        condiciones.append("unidad_operativa = %s")
        parametros.append(filtro_unidad)
    if filtro_estado:
        condiciones.append("estado = %s")
        parametros.append(filtro_estado)
    if filtro_zona:
        condiciones.append("zona_operativa = %s")
        parametros.append(filtro_zona)

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                unidad_operativa,
                estado,
                zona_operativa,
                id_ceda_agricultura,
                nombre_cedas,
                fletes_transito_dap,
                fletes_transito_urea,
                total_fletes_transito,
                ton_transito_dap,
                ton_transito_urea,
                max_dias_en_transito
            FROM fletes_en_transito_resumen
            {where_sql}
            ORDER BY max_dias_en_transito DESC
        """, parametros)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        # Totales generales
        total_dap = sum(f['fletes_transito_dap'] for f in datos)
        total_urea = sum(f['fletes_transito_urea'] for f in datos)
        total_fletes = sum(f['total_fletes_transito'] for f in datos)
        total_ton_dap = sum(f['ton_transito_dap'] for f in datos)
        total_ton_urea = sum(f['ton_transito_urea'] for f in datos)
        total_ton_total = total_ton_dap + total_ton_urea
        max_dias = max((f['max_dias_en_transito'] for f in datos), default=0)

        # Filtros disponibles
        cursor.execute("SELECT DISTINCT unidad_operativa FROM fletes_en_transito_resumen ORDER BY unidad_operativa")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]

        cursor.execute("""
            SELECT DISTINCT estado FROM fletes_en_transito_resumen
            {}
            ORDER BY estado
        """.format("WHERE unidad_operativa = %s" if filtro_unidad else ""),
        [filtro_unidad] if filtro_unidad else [])
        estados = [r[0] for r in cursor.fetchall() if r[0]]

        zona_query = "SELECT DISTINCT zona_operativa FROM fletes_en_transito_resumen"
        zona_cond = []
        zona_params = []

        if filtro_unidad:
            zona_cond.append("unidad_operativa = %s")
            zona_params.append(filtro_unidad)
        if filtro_estado:
            zona_cond.append("estado = %s")
            zona_params.append(filtro_estado)

        if zona_cond:
            zona_query += " WHERE " + " AND ".join(zona_cond)
        zona_query += " ORDER BY zona_operativa"

        cursor.execute(zona_query, zona_params)
        zonas = [r[0] for r in cursor.fetchall() if r[0]]

    return render(request, 'fertilizantes/vista_fletes_transito_por_CEDA.html', {
        'datos': datos,
        'unidades': unidades,
        'estados': estados,
        'zonas': zonas,
        'unidad_seleccionada': filtro_unidad,
        'estado_seleccionado': filtro_estado,
        'zona_seleccionada': filtro_zona,
        'totales': {
            'fletes_dap': total_dap,
            'fletes_urea': total_urea,
            'total_fletes_transito': total_fletes,
            'ton_dap': total_ton_dap,
            'ton_urea': total_ton_urea,
            'ton_total': total_ton_total,
            'max_dias': max_dias
        }
    })


@login_required
def vista_fletes_autorizados_en_transito(request):
    filtro_unidad = request.GET.get('unidad_operativa', '')
    filtro_estado = request.GET.get('estado', '')
    filtro_zona = request.GET.get('zona_operativa', '')

    condiciones = []
    params = []

    if filtro_unidad:
        condiciones.append("unidad_operativa = %s")
        params.append(filtro_unidad)
    if filtro_estado:
        condiciones.append("estado = %s")
        params.append(filtro_estado)
    if filtro_zona:
        condiciones.append("zona_operativa = %s")
        params.append(filtro_zona)

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    with connection.cursor() as cursor:
        # Consulta principal
        cursor.execute(f"""
            SELECT 
                unidad_operativa,
                estado,
                zona_operativa,
                id_ceda_agricultura,
                nombre_cedas,
                folio_del_flete,
                producto,
                toneladas_iniciales,
                fecha_de_salida,
                dias_en_transito
            FROM fletes_en_transito_detalle
            {where_sql}
            ORDER BY dias_en_transito DESC
        """, params)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        # Filtros disponibles
        cursor.execute("SELECT DISTINCT unidad_operativa FROM fletes_en_transito_detalle ORDER BY unidad_operativa")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]

        cursor.execute("""
            SELECT DISTINCT estado FROM fletes_en_transito_detalle
            {}
            ORDER BY estado
        """.format("WHERE unidad_operativa = %s" if filtro_unidad else ""),
        [filtro_unidad] if filtro_unidad else [])
        estados = [r[0] for r in cursor.fetchall() if r[0]]

        zona_query = "SELECT DISTINCT zona_operativa FROM fletes_en_transito_detalle"
        zona_cond = []
        zona_params = []

        if filtro_unidad:
            zona_cond.append("unidad_operativa = %s")
            zona_params.append(filtro_unidad)
        if filtro_estado:
            zona_cond.append("estado = %s")
            zona_params.append(filtro_estado)

        if zona_cond:
            zona_query += " WHERE " + " AND ".join(zona_cond)
        zona_query += " ORDER BY zona_operativa"

        cursor.execute(zona_query, zona_params)
        zonas = [r[0] for r in cursor.fetchall() if r[0]]

    total_fletes = len(datos)
    total_toneladas = sum(f['toneladas_iniciales'] for f in datos)
    max_dias = max((f['dias_en_transito'] for f in datos), default=0)

    return render(request, 'fertilizantes/vista_fletes_autorizados_en_transito.html', {
        'datos': datos,
        'total_fletes': total_fletes,
        'total_toneladas': total_toneladas,
        'max_dias': max_dias,
        'unidades': unidades,
        'estados': estados,
        'zonas': zonas,
        'unidad_seleccionada': filtro_unidad,
        'estado_seleccionado': filtro_estado,
        'zona_seleccionada': filtro_zona
    })


@login_required
def vista_inventario_diario_ceda(request):
    ceda = request.GET.get('ceda')
    datos = []
    resumen = None
    ceda_info = None

    if ceda:
        with connection.cursor() as cursor:
            # Consulta detalle por fecha (filtrado desde primera fecha con movimiento)
            cursor.execute("""
                SELECT 
                    fecha,
                    id_ceda_agricultura,
                    nombre_cedas,
                    coordinacion_estatal as unidad_operativa,
                    estado,
                    zona_operativa,
                    dap_ton_total_entrada,
                    urea_ton_total_entrada,
                    dap_ton_total_salida,
                    urea_ton_total_salida,
                    dap_ton_inventario_acumulado,
                    urea_ton_inventario_acumulado
                FROM inventario_acumulado_x_ceda_diario_2025
                WHERE id_ceda_agricultura = %s
                  AND fecha >= (
                      SELECT MIN(fecha)
                      FROM inventario_acumulado_x_ceda_diario_2025
                      WHERE id_ceda_agricultura = %s
                        AND (
                          dap_ton_total_entrada > 0 OR urea_ton_total_entrada > 0 OR
                          dap_ton_total_salida > 0 OR urea_ton_total_salida > 0
                        )
                  )
                  AND fecha <= CURRENT_DATE
                ORDER BY fecha DESC
            """, [ceda, ceda])
            columnas = [col[0] for col in cursor.description]
            datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

            if datos:
                ceda_info = {
                    'id': datos[0]['id_ceda_agricultura'],
                    'nombre': datos[0]['nombre_cedas'],
                    'unidad': datos[0]['unidad_operativa'],
                    'estado': datos[0]['estado'],
                    'zona': datos[0]['zona_operativa'],
                }

            # Inventario final del 31 diciembre
            cursor.execute("""
                SELECT 
                    dap_ton_inventario_acumulado,
                    urea_ton_inventario_acumulado
                FROM inventario_acumulado_x_ceda_diario_2025
                WHERE id_ceda_agricultura = %s
                AND fecha = '2025-12-31'
            """, [ceda])
            fila = cursor.fetchone()
            if fila:
                resumen = {'dap': fila[0], 'urea': fila[1], 'total': fila[0] + fila[1]}

    return render(request, 'fertilizantes/vista_inventario_diario_ceda.html', {
        'datos': datos,
        'resumen': resumen,
        'ceda': ceda,
        'ceda_info': ceda_info
    })



@login_required
def vista_inventarios_negativos_x_dia(request):
    unidad = request.GET.get('unidad_operativa')
    estado = request.GET.get('estado')

    filtros = []
    parametros = []

    if unidad:
        filtros.append("coordinacion_estatal = %s")
        parametros.append(unidad)

    if estado:
        filtros.append("estado = %s")
        parametros.append(estado)

    where = " AND ".join(filtros)
    if where:
        where = "WHERE " + where

    query = f"""
        SELECT 
            fecha,
            coordinacion_estatal AS unidad_operativa,
            estado,
            zona_operativa,
            id_ceda_agricultura,
            nombre_cedas,
            dap_ton_flete_entrada,
            urea_ton_flete_entrada,
            dap_ton_transfer_entrada,
            urea_ton_transfer_entrada,
            dap_ton_remanente_entrada,
            urea_ton_remanente_entrada,
            dap_ton_entrega_salida,
            urea_ton_entrega_salida,
            dap_ton_transfer_salida,
            urea_ton_transfer_salida,
            dap_ton_remanente_salida,
            urea_ton_remanente_salida,
            dap_ton_incidentes_salida,
            urea_ton_incidentes_salida,
            dap_ton_total_entrada,
            urea_ton_total_entrada,
            dap_ton_total_salida,
            urea_ton_total_salida,
            dap_ton_inventario_acumulado AS dap_inventario,
            urea_ton_inventario_acumulado AS urea_inventario
        FROM inventarios_negativos_x_ceda_diario_2025
        WHERE fecha <= CURRENT_DATE
        {('AND ' + ' AND '.join(filtros)) if filtros else ''}
        ORDER BY unidad_operativa, estado, zona_operativa, nombre_cedas, fecha DESC;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, parametros)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

    # Extraer listas únicas para los filtros dinámicos
    unidades = sorted(set([d['unidad_operativa'] for d in datos]))
    estados = sorted(set([d['estado'] for d in datos]))

    return render(request, 'fertilizantes/vista_inventarios_negativos_x_dia.html', {
        'datos': datos,
        'unidades': unidades,
        'estados': estados,
        'unidad_seleccionada': unidad,
        'estado_seleccionada': estado
    })




@login_required
def vista_inventarios_negativos_actuales(request):
    filtro_unidad = request.GET.get('unidad_operativa', '')
    filtro_estado = request.GET.get('estado', '')
    filtro_zona = request.GET.get('zona_operativa', '')

    condiciones = []
    parametros = []

    if filtro_unidad:
        condiciones.append("unidad_operativa = %s")
        parametros.append(filtro_unidad)
    if filtro_estado:
        condiciones.append("estado = %s")
        parametros.append(filtro_estado)
    if filtro_zona:
        condiciones.append("zona_operativa = %s")
        parametros.append(filtro_zona)

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    with connection.cursor() as cursor:
        # Datos filtrados
        cursor.execute(f"""
            SELECT *
            FROM inventarios_negativos_2025
            {where_sql}
            ORDER BY unidad_operativa, estado, zona_operativa
        """, parametros)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        # Filtros disponibles
        cursor.execute("SELECT DISTINCT unidad_operativa FROM inventarios_negativos_2025 ORDER BY unidad_operativa")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]

        cursor.execute("""
            SELECT DISTINCT estado FROM inventarios_negativos_2025
            {}
            ORDER BY estado
        """.format("WHERE unidad_operativa = %s" if filtro_unidad else ""),
        [filtro_unidad] if filtro_unidad else [])
        estados = [r[0] for r in cursor.fetchall() if r[0]]

        zona_query = "SELECT DISTINCT zona_operativa FROM inventarios_negativos_2025"
        zona_cond = []
        zona_params = []

        if filtro_unidad:
            zona_cond.append("unidad_operativa = %s")
            zona_params.append(filtro_unidad)
        if filtro_estado:
            zona_cond.append("estado = %s")
            zona_params.append(filtro_estado)

        if zona_cond:
            zona_query += " WHERE " + " AND ".join(zona_cond)
        zona_query += " ORDER BY zona_operativa"

        cursor.execute(zona_query, zona_params)
        zonas = [r[0] for r in cursor.fetchall() if r[0]]

    return render(request, 'fertilizantes/vista_inventarios_negativos_actuales.html', {
        'datos': datos,
        'unidades': unidades,
        'estados': estados,
        'zonas': zonas,
        'unidad_seleccionada': filtro_unidad,
        'estado_seleccionado': filtro_estado,
        'zona_seleccionada': filtro_zona
    })



@login_required
def vista_resumen_remanente_estado(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM resumen_remanente_estado_2025")
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
    return render(request, 'fertilizantes/vista_resumen_remanente_estado.html', {'datos': datos})


@login_required
def vista_cedas_con_remanentes(request):
    filtro_unidad = request.GET.get('unidad_operativa', '')
    filtro_estado = request.GET.get('estado', '')
    filtro_zona = request.GET.get('zona_operativa', '')

    condiciones = []
    parametros = []

    if filtro_unidad:
        condiciones.append("coordinacion_estatal = %s")
        parametros.append(filtro_unidad)
    if filtro_estado:
        condiciones.append("estado = %s")
        parametros.append(filtro_estado)
    if filtro_zona:
        condiciones.append("zona_operativa = %s")
        parametros.append(filtro_zona)

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    with connection.cursor() as cursor:
        # Datos principales filtrados
        cursor.execute(f"""
            SELECT * FROM cedas_con_remanentes_2025
            {where_sql}
            ORDER BY coordinacion_estatal, estado, zona_operativa
        """, parametros)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        # 📊 Calcular resumen
        total_cedas = len(datos)
        total_dap = sum(row['dap_ton_remanente_inventario'] for row in datos)
        total_urea = sum(row['urea_ton_remanente_inventario'] for row in datos)
        total_toneladas = total_dap + total_urea

        # Filtros dinámicos
        cursor.execute("SELECT DISTINCT coordinacion_estatal FROM cedas_con_remanentes_2025 ORDER BY coordinacion_estatal")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]

        cursor.execute("""
            SELECT DISTINCT estado FROM cedas_con_remanentes_2025
            {}
            ORDER BY estado
        """.format("WHERE coordinacion_estatal = %s" if filtro_unidad else ""),
        [filtro_unidad] if filtro_unidad else [])
        estados = [r[0] for r in cursor.fetchall() if r[0]]

        zona_query = "SELECT DISTINCT zona_operativa FROM cedas_con_remanentes_2025"
        zona_cond = []
        zona_params = []

        if filtro_unidad:
            zona_cond.append("coordinacion_estatal = %s")
            zona_params.append(filtro_unidad)
        if filtro_estado:
            zona_cond.append("estado = %s")
            zona_params.append(filtro_estado)

        if zona_cond:
            zona_query += " WHERE " + " AND ".join(zona_cond)
        zona_query += " ORDER BY zona_operativa"

        cursor.execute(zona_query, zona_params)
        zonas = [r[0] for r in cursor.fetchall() if r[0]]

    return render(request, 'fertilizantes/vista_cedas_con_remanentes.html', {
        'datos': datos,
        'unidades': unidades,
        'estados': estados,
        'zonas': zonas,
        'unidad_seleccionada': filtro_unidad,
        'estado_seleccionado': filtro_estado,
        'zona_seleccionada': filtro_zona,
        'resumen': {
            'total_cedas': total_cedas,
            'total_dap': total_dap,
            'total_urea': total_urea,
            'total_ton': total_toneladas
        }
    })




@login_required
def vista_cedas_con_remanentes_negativos(request):
    filtro_unidad = request.GET.get('unidad_operativa', '')
    filtro_estado = request.GET.get('estado', '')
    filtro_zona = request.GET.get('zona_operativa', '')

    condiciones = []
    parametros = []

    if filtro_unidad:
        condiciones.append("coordinacion_estatal = %s")
        parametros.append(filtro_unidad)
    if filtro_estado:
        condiciones.append("estado = %s")
        parametros.append(filtro_estado)
    if filtro_zona:
        condiciones.append("zona_operativa = %s")
        parametros.append(filtro_zona)

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    with connection.cursor() as cursor:
        # Datos principales filtrados
        cursor.execute(f"""
            SELECT *
            FROM cedas_con_remanentes_negativos_2025
            {where_sql}
            ORDER BY coordinacion_estatal, estado, zona_operativa
        """, parametros)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        # Filtros disponibles
        cursor.execute("SELECT DISTINCT coordinacion_estatal FROM cedas_con_remanentes_negativos_2025 ORDER BY coordinacion_estatal")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]

        cursor.execute("""
            SELECT DISTINCT estado FROM cedas_con_remanentes_negativos_2025
            {}
            ORDER BY estado
        """.format("WHERE coordinacion_estatal = %s" if filtro_unidad else ""),
        [filtro_unidad] if filtro_unidad else [])
        estados = [r[0] for r in cursor.fetchall() if r[0]]

        zona_query = "SELECT DISTINCT zona_operativa FROM cedas_con_remanentes_negativos_2025"
        zona_cond = []
        zona_params = []

        if filtro_unidad:
            zona_cond.append("coordinacion_estatal = %s")
            zona_params.append(filtro_unidad)
        if filtro_estado:
            zona_cond.append("estado = %s")
            zona_params.append(filtro_estado)

        if zona_cond:
            zona_query += " WHERE " + " AND ".join(zona_cond)
        zona_query += " ORDER BY zona_operativa"

        cursor.execute(zona_query, zona_params)
        zonas = [r[0] for r in cursor.fetchall() if r[0]]

    return render(request, 'fertilizantes/vista_cedas_con_remanentes_negativos.html', {
        'datos': datos,
        'unidades': unidades,
        'estados': estados,
        'zonas': zonas,
        'unidad_seleccionada': filtro_unidad,
        'estado_seleccionado': filtro_estado,
        'zona_seleccionada': filtro_zona
    })



@login_required
def vista_fletes_ton_conteo_detalle(request):
    unidad = request.GET.get('unidad_operativa')
    estado = request.GET.get('estado')
    procedencia = request.GET.get('estado_procedencia')
    buscar_ceda = request.GET.get('buscar_ceda')

    filtros = []
    params = []

    if unidad:
        filtros.append("unidad_operativa = %s")
        params.append(unidad)
    if estado:
        filtros.append("estado = %s")
        params.append(estado)
    if procedencia:
        filtros.append("estado_procedencia = %s")
        params.append(procedencia)
    if buscar_ceda:
        filtros.append("id_ceda_agricultura ILIKE %s")
        params.append(f"%{buscar_ceda}%")

    where = "WHERE " + " AND ".join(filtros) if filtros else ""

    mostrar_datos = bool(filtros)
    datos = []

    if mostrar_datos:
        query = f"""
            SELECT 
                unidad_operativa,
                estado,
                zona_operativa,
                id_ceda_agricultura,
                nombre_cedas,
                estado_procedencia,
                toneladas_iniciales,
                cantidad_fletes_dap,
                cantidad_fletes_urea
            FROM fletes_ton_conteo_detalle_td
            {where}
            ORDER BY toneladas_iniciales DESC;
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columnas = [col[0] for col in cursor.description]
            datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

    # Se obtienen filtros únicos desde la vista completa, no solo desde los datos filtrados
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT unidad_operativa FROM fletes_ton_conteo_detalle_td ORDER BY unidad_operativa")
        unidades = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT estado FROM fletes_ton_conteo_detalle_td ORDER BY estado")
        estados = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT estado_procedencia FROM fletes_ton_conteo_detalle_td ORDER BY estado_procedencia")
        procedencias = [row[0] for row in cursor.fetchall()]

    return render(request, "fertilizantes/vista_fletes_ton_conteo_detalle.html", {
        "datos": datos,
        "unidades": unidades,
        "estados": estados,
        "procedencias": procedencias,
        "unidad_seleccionada": unidad,
        "estado_seleccionado": estado,
        "procedencia_seleccionada": procedencia,
        "buscar_ceda": buscar_ceda
    })


@login_required
def vista_fletes_toneladas_recibidas_atipicas(request):
    query = """
        SELECT *
        FROM fletes_toneladas_recibidas_atipicas_2025
        ORDER BY diferencia_ton DESC
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

    return render(request, "fertilizantes/vista_fletes_toneladas_recibidas_atipicas.html", {
        "datos": datos
    })


@login_required
def vista_fletes_fechas_incoherentes(request):
    query = """
        SELECT *
        FROM fletes_fechas_incoherentes_2025
        ORDER BY fecha_de_salida DESC
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

    return render(request, "fertilizantes/vista_fletes_fechas_incoherentes.html", {
        "datos": datos
    })



@login_required
def vista_pedidos_detalle_fecha(request):
    unidad = request.GET.get('unidad_operativa')
    estado = request.GET.get('estado')
    zona = request.GET.get('zona_operativa')
    buscar_ceda = request.GET.get('buscar_ceda') or ''  # evitar None
    fecha_inicio = request.GET.get('fecha_inicio')
    fecha_fin = request.GET.get('fecha_fin')

    filtros = []
    params = []

    if unidad:
        filtros.append("unidad_operativa = %s")
        params.append(unidad)
    if estado:
        filtros.append("estado = %s")
        params.append(estado)
    if zona:
        filtros.append("zona_operativa = %s")
        params.append(zona)
    if buscar_ceda:
        filtros.append("id_ceda_agricultura ILIKE %s")
        params.append(f"%{buscar_ceda}%")
    if fecha_inicio:
        filtros.append("fecha >= %s")
        params.append(fecha_inicio)
    if fecha_fin:
        filtros.append("fecha <= %s")
        params.append(fecha_fin)

    where = "WHERE " + " AND ".join(filtros) if filtros else ""

    datos = []
    resumen = {"total": 0, "dap": 0, "urea": 0}

    if filtros:
        query = f"""
            SELECT * FROM pedidos_detalle_por_fecha_2025
            {where}
            ORDER BY fecha DESC, estado, nombre_cedas
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columnas = [col[0] for col in cursor.description]
            datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]

        # Calcular resumen
        resumen["total"] = len(datos)
        resumen["dap"] = sum(float(d["dap"]) for d in datos)
        resumen["urea"] = sum(float(d["urea"]) for d in datos)

    # Obtener opciones de filtros
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT unidad_operativa FROM pedidos_detalle_por_fecha_2025 ORDER BY unidad_operativa")
        unidades = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT estado FROM pedidos_detalle_por_fecha_2025 ORDER BY estado")
        estados = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT DISTINCT zona_operativa FROM pedidos_detalle_por_fecha_2025 ORDER BY zona_operativa")
        zonas = [row[0] for row in cursor.fetchall()]

    return render(request, "fertilizantes/pedidos_detalle_por_fecha.html", {
        "datos": datos,
        "resumen": resumen,
        "unidades": unidades,
        "estados": estados,
        "zonas": zonas,
        "unidad_seleccionada": unidad,
        "estado_seleccionado": estado,
        "zona_seleccionada": zona,
        "buscar_ceda": buscar_ceda,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
    })


# ---------- Consulta Derechohabientes ----------
@login_required
def vista_derechohabientes(request):
    qs = DH.objects.all()  # ← DH viene del import paso 2

    # --- Filtros “grandes” ---------------------------------------
    uo   = request.GET.get("unidad_operativa")
    edo  = request.GET.get("estado")
    ceda = request.GET.get("id_ceda_agricultura")

    if uo:
        qs = qs.filter(unidad_operativa=uo)
    if edo:
        qs = qs.filter(estado=edo)
    if ceda:
        qs = qs.filter(id_ceda_agricultura=ceda)
    
    # --- Nuevo filtro por fecha entrega --------------------------
    
    fecha_inicio = request.GET.get("fecha_inicio")
    fecha_fin    = request.GET.get("fecha_fin")

    if fecha_inicio:
        qs = qs.filter(fecha_entrega__gte=fecha_inicio)
    if fecha_fin:
        qs = qs.filter(fecha_entrega__lte=fecha_fin)


    # --- Búsquedas 1×1 ------------------------------------------
    for campo in ("acuse_estatal", "curp_solicitud", "curp_renapo"):
        v = request.GET.get(campo)
        if v:
            qs = qs.filter(**{f"{campo}__iexact": v})

    # --- Lista multilineal de acuses ----------------------------
    lista = request.GET.get("lista_acuses")
    if lista:
        acuses = [l.strip() for l in lista.splitlines() if l.strip()]
        qs = qs.filter(acuse_estatal__in=acuses)

    # --- Columnas a mostrar -------------------------------------
    seleccion = request.GET.getlist("campos") or [f for f, _ in CAMPOS_DH]

    # 🔒 Validar columnas: evitar errores por nombres inválidos
    validos = {f for f, _ in CAMPOS_DH}
    seleccion = [f for f in seleccion if f in validos]

    qs = qs.values(*seleccion)

    # --- CSV directo (descarga) ---------------------------------
    export_csv = request.GET.get("csv") == "1"
    masivo     = bool(uo or edo or ceda)     # filtros grandes ⇒ CSV directo

    if export_csv or masivo:
        def filas():
            yield [lbl for f, lbl in CAMPOS_DH if f in seleccion]
            for fila in qs.iterator(chunk_size=10_000):
                yield [fila[c] for c in seleccion]

        pseudo = io.StringIO()
        writer = csv.writer(pseudo)

        def stream():
            first = True
            for row in filas():
                pseudo.seek(0)
                pseudo.truncate(0)
                writer.writerow(row)
                data = pseudo.getvalue()
                if first:
                    data = '\ufeff' + data  # BOM
                    first = False
                yield data

        nombre = f"derechohabientes_{datetime.date.today():%Y%m%d}.csv"
        resp = StreamingHttpResponse(stream(), content_type="text/csv; charset=utf-8")
        resp["Content-Disposition"] = f'attachment; filename="{nombre}"'
        return resp

    # --- HTML (solo si hay filtros) ---------------------
    mostrar_datos = bool(request.GET)
    datos = list(qs[:500]) if mostrar_datos else []

    # 🔄 Listas únicas para los dropdowns
    unidades = DH.objects.order_by('unidad_operativa').values_list('unidad_operativa', flat=True).distinct()
    estados  = DH.objects.order_by('estado').values_list('estado', flat=True).distinct()

    return render(request, "fertilizantes/vista_derechohabientes.html", {
        "datos":         datos,
        "campos":        CAMPOS_DH,
        "seleccionados": seleccion,
        "unidades":      unidades,
        "estados":       estados,
        "unidad_seleccionada": uo,
        "estado_seleccionada": edo,
    })


# ------------------------------------------------------------
#  Inventario diario CEDA  (Campo vs SIGAP)
# ------------------------------------------------------------
from datetime import date, timedelta

@login_required
def vista_inventario_ceda_diario(request):
    cols = ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura"]
    where, params, ctx = _aplicar_filtros_get(request, cols)

    # 🆕 0.  Lista de Unidades operativas — SIEMPRE (para el select inicial)
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT unidad_operativa
            FROM   inventario_ceda_diario_2025_campo_sigap
            WHERE  unidad_operativa IS NOT NULL
            ORDER  BY unidad_operativa
        """)
        ctx["unidades"] = [r[0] for r in cur.fetchall()]

    # 🆕 0.1  Alias para que el template coincida («seleccionada» vs «seleccionado»)
    ctx["unidad_operativa_seleccionada"] = ctx.get("unidad_operativa_seleccionado")

    # 🟢 1.  ¿Hay algún filtro aplicado?
    filtros_aplicados = bool(params)

    # 🔒 2.  Si NO hay filtros, solo renderiza el formulario (tabla vacía)
    if not filtros_aplicados:
        ctx.update({
            "datos": [],
            "mensaje": "Seleccione al menos un filtro para mostrar resultados."
        })
        return render(
            request,
            "fertilizantes/vista_inventario_ceda_diario_campo.html",
            ctx
        )

    # 🗓️ 3.  Limitar a “hasta ayer” si el usuario no fijó fecha_fin
    if not request.GET.get("fecha_fin", "").strip():
        ayer = date.today() - timedelta(days=1)
        where += (" AND " if where else " WHERE ") + "fecha <= %s"
        params.append(ayer)
        ctx["fecha_fin"] = ayer.strftime("%Y-%m-%d")

    # 🔍 4.  Ejecutar consulta con los filtros armados
    sql = f"""
        SELECT *
        FROM inventario_ceda_diario_2025_campo_sigap
        {where}
        ORDER BY fecha DESC, id_ceda_agricultura
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        headers = [c[0] for c in cur.description]
        ctx["datos"] = [dict(zip(headers, row)) for row in cur.fetchall()]

    return render(
        request,
        "fertilizantes/vista_inventario_ceda_diario_campo.html",
        ctx
    )
    

# ------------------------------------------------------------
#  Estadísticas de inventarios capturados en campo
# ------------------------------------------------------------
@login_required
def vista_estadisticas_inventarios_campo(request):
    where, params, ctx = _aplicar_filtros_get(
        request,
        ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura"]
    )

    # ✅ lista de Unidades (siempre, para el combo)
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT unidad_operativa
            FROM   estadisticas_inventarios_campo
            WHERE  unidad_operativa IS NOT NULL
            ORDER  BY unidad_operativa
        """)
        ctx["unidades"] = [r[0] for r in cur.fetchall()]

    # 🔒 1. si NO hay ningún filtro, no consultes la vista
    filtros_aplicados = bool(params)          # hay algo en WHERE / fechas
    if not filtros_aplicados:
        ctx["datos"] = []
        ctx["mensaje"] = "Seleccione al menos un filtro para mostrar resultados."
        return render(
            request,
            "fertilizantes/vista_estadisticas_inventarios_campo.html",
            ctx
        )

    # 🔍 2. solo llega aquí cuando sí hay filtros
    sql = f"""
        SELECT *
        FROM estadisticas_inventarios_campo
        {where}
        ORDER BY unidad_operativa, estado, zona_operativa, nombre_cedas
    """
    with connection.cursor() as cur:
        cur.execute(sql, params)
        headers = [c[0] for c in cur.description]
        ctx["datos"] = [dict(zip(headers, fila)) for fila in cur.fetchall()]

    return render(
        request,
        "fertilizantes/vista_estadisticas_inventarios_campo.html",
        ctx
    )



@login_required
def comentarios_por_ceda(request):
    from datetime import datetime  # Asegúrate de tenerlo arriba si no lo has importado

    # Si se envía un comentario por POST
    if request.method == "POST":
        id_ceda = request.POST.get("id_ceda_agricultura")
        texto = request.POST.get("comentario", "").strip()

        if id_ceda and texto:
            with psycopg_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO comentarios_ceda (id_ceda_agricultura, comentario, fecha)
                    VALUES (%s, %s, %s)
                """, (id_ceda, texto, datetime.now()))
                psycopg_conn.commit()

        return redirect("comentarios_por_ceda")

    # Para GET: aplicar filtros y mostrar vista
    where, params, ctx = _aplicar_filtros_get(
        request,
        ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura"]
    )

    # ⚠️ Filtrar valores vacíos Y asegurar que params no esté vacío
    params = [p for p in params if p not in ("", None)]

    # Cargar combos (unidades operativas)
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT unidad_operativa
            FROM vista_comentarios_ceda
            WHERE unidad_operativa IS NOT NULL
            ORDER BY unidad_operativa
        """)
        ctx["unidades"] = [r[0] for r in cur.fetchall()]

    # Si no se aplicó ningún filtro válido, no cargar datos
    if not params:
        ctx["filtro_aplicado"] = False
        return render(request, "fertilizantes/comentarios_por_ceda.html", ctx)

    # ✅ Asegurar que sea una lista de tuplas para evitar ArgumentError
    query = f"""
        SELECT *
        FROM vista_comentarios_ceda
        {where}
        ORDER BY unidad_operativa, estado, zona_operativa, nombre_cedas
    """
    
    df = pd.read_sql(query, con=engine, params=[tuple(params)])

    ctx["df"] = df
    ctx["filtro_aplicado"] = True

    return render(request, "fertilizantes/comentarios_por_ceda.html", ctx)
    
    
    
    

@require_GET
def ajax_zonas_por_filtros(request):
    unidad = request.GET.get("unidad", "").strip()
    estado = request.GET.get("estado", "").strip()

    # Si no se envía nada, devolver lista vacía
    if not unidad and not estado:
        return JsonResponse({"zonas": []})

    query = "SELECT DISTINCT zona_operativa FROM vista_comentarios_ceda WHERE 1=1"
    params = []

    if unidad:
        query += " AND unidad_operativa = %s"
        params.append(unidad)

    if estado:
        query += " AND estado = %s"
        params.append(estado)

    zonas = pd.read_sql(query, con=engine, params=params)
    lista_zonas = sorted(zonas["zona_operativa"].dropna().unique().tolist())

    return JsonResponse({"zonas": lista_zonas})






# 👇 Nombre de la vista materializada a consultar
MATVIEW = "mv_fletes_enriquecidos"

# ============ Helpers de BD ============

def _fetch_all_columns():
    """
    Regresa TODAS las columnas reales de la vista materializada, en orden físico.
    Si la MV no existe, regresa lista vacía.
    """
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname AS column_name
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'm'
                  AND n.nspname = 'public'
                  AND c.relname = %s
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attnum;
                """,
                [MATVIEW],
            )
            return [r[0] for r in cur.fetchall()]
    except Exception:
        return []


def _norm_date(value):
    """
    Normaliza fechas provenientes del front:
    - acepta 'dd/mm/aaaa' y 'aaaa-mm-dd'
    - regresa 'aaaa-mm-dd' o None si no es válida
    """
    if not value:
        return None
    v = str(value).strip()
    # dd/mm/aaaa
    try:
        d = datetime.datetime.strptime(v, "%d/%m/%Y").date()
        return d.isoformat()
    except Exception:
        pass
    # aaaa-mm-dd (o parse_date de Django)
    d = parse_date(v)
    return d.isoformat() if d else None


def _build_where_and_params(q):
    """
    Construye el WHERE y la lista de parámetros según los filtros recibidos.
    q es un dict con posibles llaves:
      unidad_operativa, estado, zona_operativa, id_ceda_agricultura,
      estatus, abreviacion_producto, cdf_destino_original, cdf_destino_final,
      fecha_salida_ini, fecha_salida_fin, fecha_entrega_ini, fecha_entrega_fin,
      folios_multiline
    IMPORTANTE: en la MV los campos de fecha son 'fecha_de_salida' y 'fecha_de_entrega'
    (algunos con timestamp en origen). Para consistencia filtramos por su ::date.
    """
    where = []
    params = []

    # Encadenados
    if q.get("unidad_operativa"):
        where.append("unidad_operativa = %s")
        params.append(q["unidad_operativa"])
    if q.get("estado"):
        where.append("estado = %s")
        params.append(q["estado"])
    if q.get("zona_operativa"):
        where.append("zona_operativa = %s")
        params.append(q["zona_operativa"])
    if q.get("id_ceda_agricultura"):
        where.append("id_ceda_agricultura = %s")
        params.append(q["id_ceda_agricultura"])

    # Otros filtros directos
    if q.get("estatus"):
        where.append("estatus = %s")
        params.append(q["estatus"])
    if q.get("abreviacion_producto"):
        where.append("abreviacion_producto = %s")
        params.append(q["abreviacion_producto"])
    if q.get("cdf_destino_original"):
        where.append("cdf_destino_original = %s")
        params.append(q["cdf_destino_original"])
    if q.get("cdf_destino_final"):
        where.append("cdf_destino_final = %s")
        params.append(q["cdf_destino_final"])

    # Fechas (salida) -> usar columna real 'fecha_de_salida'
    if q.get("fecha_salida_ini"):
        where.append("fecha_de_salida::date >= %s")
        params.append(q["fecha_salida_ini"])
    if q.get("fecha_salida_fin"):
        where.append("fecha_de_salida::date <= %s")
        params.append(q["fecha_salida_fin"])

    # Fechas (entrega) -> usar columna real 'fecha_de_entrega'
    # NOTA: aquí también hacemos filtro por fechas “cerradas” (::date)
    if q.get("fecha_entrega_ini"):
        where.append("fecha_de_entrega::date >= %s")
        params.append(q["fecha_entrega_ini"])
    if q.get("fecha_entrega_fin"):
        where.append("fecha_de_entrega::date <= %s")
        params.append(q["fecha_entrega_fin"])

    # Lista de folios (uno por línea o separados por coma)
    folios = []
    raw_folios = (q.get("folios_multiline") or "").strip()
    if raw_folios:
        for line in raw_folios.replace(",", "\n").splitlines():
            val = line.strip()
            if val:
                folios.append(val)
    if folios:
        where.append("folio_del_flete = ANY(%s)")
        params.append(folios)  # psycopg convierte a ARRAY

    sql_where = ("WHERE " + " AND ".join(where)) if where else ""
    return sql_where, params


# ============ Vistas ============

@require_GET
def vista_fletes(request):
    """
    Renderiza el template principal de consulta de fletes.
    Envía la lista de columnas (de la MV) para los checkboxes.
    """
    columnas = _fetch_all_columns()

    # Si no hay columnas, avisa en el template (no rompemos el render)
    prefer = [
        "unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura",
        "estatus", "abreviacion_producto", "folio_del_flete",
        "cdf_destino_original", "cdf_destino_final",
        # OJO: en la MV se llaman 'fecha_de_salida' y 'fecha_de_entrega'
        "fecha_de_salida", "fecha_de_entrega"
    ]
    columnas = sorted(
        columnas,
        key=lambda c: (
            0 if c in prefer else 1,
            prefer.index(c) if c in prefer else 999,
            c,
        ),
    )

    return render(request, "fertilizantes/vistas_fletes.html", {
        "columnas": columnas,
        "matview": MATVIEW,
        "mv_tiene_columnas": bool(columnas),
    })


@require_GET
def api_fletes_opciones(request):
    """
    Devuelve opciones distinct para los selects encadenados.
    Se filtra con lo que ya tenga elegido el usuario (para encadenamiento).
    """
    q = {
        "unidad_operativa": request.GET.get("unidad_operativa") or None,
        "estado": request.GET.get("estado") or None,
        "zona_operativa": request.GET.get("zona_operativa") or None,
    }
    where, params = _build_where_and_params(q)

    def distinct_of(field):
        try:
            with connection.cursor() as cur:
                cur.execute(
                    f"SELECT DISTINCT {field} FROM {MATVIEW} {where} ORDER BY {field} NULLS LAST;",
                    params,
                )
                return [r[0] for r in cur.fetchall() if r[0] not in (None, "")]
        except Exception:
            return []

    data = {
        "unidad_operativa": distinct_of("unidad_operativa"),
        "estado": distinct_of("estado"),
        "zona_operativa": distinct_of("zona_operativa"),
        "id_ceda_agricultura": distinct_of("id_ceda_agricultura"),
        "estatus": distinct_of("estatus"),
        "abreviacion_producto": distinct_of("abreviacion_producto"),
        "cdf_destino_original": distinct_of("cdf_destino_original"),
        "cdf_destino_final": distinct_of("cdf_destino_final"),
    }
    return JsonResponse(data)


@require_POST
def api_fletes_consultar(request):
    """
    Devuelve JSON con las filas filtradas y sólo las columnas seleccionadas.
    """
    try:
        body = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Body JSON inválido")

    # Normalizar fechas a 'YYYY-MM-DD' (acepta dd/mm/aaaa)
    body["fecha_salida_ini"]  = _norm_date(body.get("fecha_salida_ini"))
    body["fecha_salida_fin"]  = _norm_date(body.get("fecha_salida_fin"))
    body["fecha_entrega_ini"] = _norm_date(body.get("fecha_entrega_ini"))
    body["fecha_entrega_fin"] = _norm_date(body.get("fecha_entrega_fin"))

    q = {k: (body.get(k) or None) for k in [
        "unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura",
        "estatus", "abreviacion_producto", "cdf_destino_original", "cdf_destino_final",
        "fecha_salida_ini", "fecha_salida_fin", "fecha_entrega_ini", "fecha_entrega_fin",
        "folios_multiline"
    ]}
    user_cols = body.get("columnas") or []

    all_cols = _fetch_all_columns()
    if not all_cols:
        return JsonResponse({"columns": [], "rows": [], "count": 0})

    # Sanitizar columnas
    cols = [c for c in user_cols if c in all_cols]
    if not cols:
        defaults = [
            "folio_del_flete", "unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura",
            "estatus", "abreviacion_producto", "cdf_destino_original", "cdf_destino_final",
            # usar nombres reales
            "fecha_de_salida", "fecha_de_entrega"
        ]
        cols = [c for c in defaults if c in all_cols] or all_cols[:10]

    where, params = _build_where_and_params(q)
    sql = f"SELECT {', '.join(cols)} FROM {MATVIEW} {where} LIMIT 10000;"

    try:
        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        return HttpResponseBadRequest(f"Error en la consulta: {e}")

    return JsonResponse({
        "columns": cols,
        "rows": rows,
        "count": len(rows),
    })
    
    try:
        with connection.cursor() as cur:
            # Consulta principal (no se toca)
            cur.execute(sql, params)
            rows = cur.fetchall()

            # 🔽🔽🔽 NUEVO: agregados para el resumen
            agg_sql = f"""
                SELECT
                  COUNT(*)::int                                                 AS total,
                  COALESCE(SUM(toneladas_iniciales), 0)::float                 AS sum_toneladas_iniciales,
                  COALESCE(SUM(toneladas_en_el_destino), 0)::float             AS sum_toneladas_en_el_destino,

                  COUNT(*) FILTER (WHERE abreviacion_producto = 'DAP')::int    AS dap_count,
                  COUNT(*) FILTER (WHERE abreviacion_producto = 'UREA')::int   AS urea_count,

                  COALESCE(SUM(CASE WHEN abreviacion_producto='DAP'
                                    THEN toneladas_iniciales END), 0)::float   AS dap_sum_ini,
                  COALESCE(SUM(CASE WHEN abreviacion_producto='DAP'
                                    THEN toneladas_en_el_destino END), 0)::float AS dap_sum_dest,

                  COALESCE(SUM(CASE WHEN abreviacion_producto='UREA'
                                    THEN toneladas_iniciales END), 0)::float   AS urea_sum_ini,
                  COALESCE(SUM(CASE WHEN abreviacion_producto='UREA'
                                    THEN toneladas_en_el_destino END), 0)::float AS urea_sum_dest
                FROM {MATVIEW} {where};
            """
            cur.execute(agg_sql, params)
            a = cur.fetchone()
            summary = {
                "total": a[0],
                "sum_toneladas_iniciales": a[1],
                "sum_toneladas_en_el_destino": a[2],
                "DAP": {
                    "count": a[3],
                    "sum_toneladas_iniciales": a[5],
                    "sum_toneladas_en_el_destino": a[6],
                },
                "UREA": {
                    "count": a[4],
                    "sum_toneladas_iniciales": a[7],
                    "sum_toneladas_en_el_destino": a[8],
                },
            }
            # 🔼🔼🔼 FIN NUEVO

    except Exception as e:
        return HttpResponseBadRequest(f"Error en la consulta: {e}")

    return JsonResponse({
        "columns": cols,
        "rows": rows,
        "count": len(rows),
        "summary": summary,  # ← 🔥 NUEVO: el resumen llega al front
    })
    


@require_POST
def api_fletes_exportar_csv(request):
    """
    Mismos filtros/columnas que consultar, pero responde un archivo CSV.
    """
    try:
        body = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Body JSON inválido")

    # Normalizar fechas a 'YYYY-MM-DD' (acepta dd/mm/aaaa)
    body["fecha_salida_ini"]  = _norm_date(body.get("fecha_salida_ini"))
    body["fecha_salida_fin"]  = _norm_date(body.get("fecha_salida_fin"))
    body["fecha_entrega_ini"] = _norm_date(body.get("fecha_entrega_ini"))
    body["fecha_entrega_fin"] = _norm_date(body.get("fecha_entrega_fin"))

    q = {k: (body.get(k) or None) for k in [
        "unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura",
        "estatus", "abreviacion_producto", "cdf_destino_original", "cdf_destino_final",
        "fecha_salida_ini", "fecha_salida_fin", "fecha_entrega_ini", "fecha_entrega_fin",
        "folios_multiline"
    ]}
    user_cols = body.get("columnas") or []

    all_cols = _fetch_all_columns()
    if not all_cols:
        # CSV vacío pero válido
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="fletes_consulta.csv"'
        writer = csv.writer(response)
        writer.writerow([])
        return response

    cols = [c for c in user_cols if c in all_cols] or all_cols  # si no eligió, todas

    where, params = _build_where_and_params(q)
    sql = f"SELECT {', '.join(cols)} FROM {MATVIEW} {where};"

    try:
        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e:
        return HttpResponseBadRequest(f"Error en la consulta: {e}")

    # Respuesta CSV
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="fletes_consulta.csv"'
    writer = csv.writer(response)
    writer.writerow(cols)
    for r in rows:
        writer.writerow([smart_str(v) for v in r])
    return response
    
# --------------------------------------------------------------------
#  OBTENER TEXTO – Para obtener texto de imágenes o PDF
# --------------------------------------------------------------------

ALLOWED_EXT = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff", ".pdf"}
DEFAULT_LANG = "spa+eng"  # puedes cambiar a "spa" si lo deseas

def _image_bytes_to_pil(img_bytes: bytes) -> Image.Image:
    return Image.open(io.BytesIO(img_bytes)).convert("RGB")

def _simple_preprocess(img: Image.Image) -> Image.Image:
    # Ligero: escala de grises + autocontraste
    from PIL import ImageOps, ImageFilter
    img = ImageOps.grayscale(img)
    img = ImageOps.autocontrast(img, cutoff=1)
    img = img.filter(ImageFilter.MedianFilter(3))
    return img

def _ocr_pil(img: Image.Image, lang: str = DEFAULT_LANG) -> str:
    cfg = "--oem 3 --psm 6"  # texto “normal”; cambia a 7 si son folios de una línea
    img = _simple_preprocess(img)
    return pytesseract.image_to_string(img, lang=lang, config=cfg)

def _ocr_pdf_bytes(pdf_bytes: bytes, lang: str = DEFAULT_LANG, dpi: int = 300) -> str:
    if not HAS_PDF:
        return "Necesitas instalar pdf2image y poppler para procesar PDFs."
    pages = convert_from_bytes(pdf_bytes, dpi=dpi)
    textos = []
    for p in pages:
        textos.append(_ocr_pil(p, lang=lang))
    return "\n\n".join(textos).strip()

@login_required
def ocr_page(request):
    return render(request, "fertilizantes/ocr_page.html")

@require_POST
def ocr_extract(request):
    """
    Acepta:
      - file: archivo subido (imagen o PDF)
      - o bien clipboard_data: dataURL base64 (pegado desde portapapeles)
      - params opcionales: lang, psm
    Responde JSON: { text: "..." }
    """
    lang = request.POST.get("lang", DEFAULT_LANG).strip() or DEFAULT_LANG
    psm = request.POST.get("psm")  # si quieres exponerlo, no es obligatorio
    whitelist = request.POST.get("whitelist")  # opcional para folios/códigos

    cfg = "--oem 3 --psm 6"
    if psm and psm.isdigit():
        cfg = f"--oem 3 --psm {psm}"
    if whitelist:
        cfg += f' -c tessedit_char_whitelist="{whitelist}"'

    def ocr_image_bytes(b: bytes) -> str:
        img = _image_bytes_to_pil(b)
        img = _simple_preprocess(img)
        return pytesseract.image_to_string(img, lang=lang, config=cfg).strip()

    # 1) Archivo subido
    if "file" in request.FILES:
        f = request.FILES["file"]
        name = f.name.lower()
        ext = Path(name).suffix
        if ext not in ALLOWED_EXT:
            return HttpResponseBadRequest("Tipo de archivo no soportado.")
        data = f.read()

        if ext == ".pdf":
            text = _ocr_pdf_bytes(data, lang=lang)
        else:
            text = ocr_image_bytes(data)
        return JsonResponse({"text": text})

    # 2) Pegado desde portapapeles (dataURL base64)
    data_url = request.POST.get("clipboard_data")
    if data_url and data_url.startswith("data:"):
        header, b64 = data_url.split(",", 1)
        raw = base64.b64decode(b64)
        if ";base64" in header and "pdf" in header:
            text = _ocr_pdf_bytes(raw, lang=lang)
        else:
            text = ocr_image_bytes(raw)
        return JsonResponse({"text": text})

    return HttpResponseBadRequest("No se recibió archivo ni datos del portapapeles.")



    
# --------------------------------------------------------------------
#  AJAX – Filtros dependientes: Unidad ➜ Estado ➜ Zona operativa
# --------------------------------------------------------------------
@require_GET
@login_required
def ajax_filtros_generales(request):
    tabla  = request.GET.get("tabla")
    unidad = request.GET.get("unidad_operativa")
    estado = request.GET.get("estado")

    tablas_validas = {
        # col. que representa la unidad / coordinación en cada vista
        "fletes_en_transito_detalle":               "unidad_operativa",
        "vista_fletes_autorizados_en_transito":     "unidad_operativa",
        "vista_fletes_transito_por_CEDA":           "unidad_operativa",
        "inventarios_negativos_2025":               "unidad_operativa",
        "inventario_acumulado_x_ceda_diario_2025":  "unidad_operativa",
        "inventario_ceda_diario_2025_campo_sigap":  "unidad_operativa",   # nueva
        "estadisticas_inventarios_campo":           "unidad_operativa",   # nueva
        "cedas_con_remanentes_2025":                "coordinacion_estatal",
        "cedas_con_remanentes_negativos_2025":      "coordinacion_estatal",
        "vista_comentarios_ceda": "unidad_operativa",
    }
    if tabla not in tablas_validas:
        return JsonResponse({"error": "Tabla no autorizada"}, status=400)

    col_uo = tablas_validas[tabla]
    resp = {"estados": [], "zonas": []}

    with connection.cursor() as cur:
        # -------- ESTADOS --------
        sql_est = f"SELECT DISTINCT estado FROM {tabla}"
        params_est = []
        if unidad:
            sql_est += f" WHERE {col_uo} = %s"
            params_est.append(unidad)
        sql_est += " ORDER BY estado"

        cur.execute(sql_est, params_est)
        resp["estados"] = [r[0] for r in cur.fetchall()]

        # -------- ZONAS (solo si ya hay estado) --------
        if estado:
            sql_zona = f"""
                SELECT DISTINCT zona_operativa
                FROM {tabla}
                WHERE estado = %s
            """
            params_zona = [estado]
            if unidad:
                sql_zona += f" AND {col_uo} = %s"
                params_zona.append(unidad)
            sql_zona += " ORDER BY zona_operativa"

            cur.execute(sql_zona, params_zona)
            resp["zonas"] = [r[0] for r in cur.fetchall()]

    return JsonResponse(resp)

