import csv
import io
import json
import math
import datetime
import base64
import subprocess
import pandas as pd
import tempfile
from pathlib import Path
from decimal import Decimal
from datetime import date, timedelta

# Django Imports
from django.shortcuts import render, redirect
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest, StreamingHttpResponse
from django.db import connection
from django.db.models import Sum
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_GET, require_POST
from django.utils.timezone import now
from django.utils.encoding import smart_str
from django.utils.dateparse import parse_date
from django.core.paginator import Paginator  # ✨ Paginación


# App Imports
from .forms import ComentarioCEDAForm
from .models import ComentarioCEDA, VwDerechohabientesConContexto as DH
from sqlalchemy import text
from .conexion import (
    engine,
    psycopg_conn,
    get_engine_for_year,
    get_psycopg_conn_for_year,
)

# OCR Imports
from PIL import Image, ImageOps, ImageFilter
import pytesseract

try:
    from pdf2image import convert_from_bytes
    HAS_PDF = True
except Exception:
    HAS_PDF = False


# ==========================================
# 🛠️ HELPERS Y UTILIDADES GLOBALES
# ==========================================

def get_anio_context(request):
    """
    Recupera el año activo de la sesión. Si no existe, usa 2025 por defecto.
    """
    return request.session.get("anio_activo", "2025")

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
    filtros, params, ctx = [], [], {}
    for col in columnas:
        v = request.GET.get(col)
        if v:
            filtros.append(f"{col} = %s")
            params.append(v)
        ctx[f"{col}_seleccionado"] = v

    fi = request.GET.get("fecha_ini")
    ff = request.GET.get("fecha_fin")
    if fi:
        filtros.append("fecha >= %s"); params.append(fi)
    if ff:
        filtros.append("fecha <= %s"); params.append(ff)
    
    ctx.update({"fecha_ini": fi, "fecha_fin": ff})
    where_sql = " WHERE " + " AND ".join(filtros) if filtros else ""
    return where_sql, params, ctx

# ==========================================
# 🏠 VISTAS DE INICIO Y CONFIGURACIÓN
# ==========================================

@login_required
def inicio(request):
    anio_activo = get_anio_context(request)
    return render(request, "fertilizantes/inicio.html", {"anio_activo": anio_activo})

@login_required
def seleccionar_anio(request):
    ANIOS_VALIDOS = ["2025", "2026"]
    if request.method == "POST":
        anio = request.POST.get("anio")
        if anio in ANIOS_VALIDOS:
            request.session["anio_activo"] = anio
        else:
            request.session["anio_activo"] = "2025"
        return redirect("inicio")
    
    anio_actual = get_anio_context(request)
    return render(request, "fertilizantes/seleccionar_anio.html", {"anio_actual": anio_actual})

@login_required
def actualizar_bases(request):
    mensaje = ''
    if request.method == 'POST':
        # Rutas hardcoded (Validar si cambian en 2026)
        path_python = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/ENTORNO/env/bin/python"
        path_scripts = "/Users/Arturo/AGRICULTURA/FERTILIZANTES/SCRIPTS/"
        
        script = "actualizar_todo.py" if 'actualizar_todo' in request.POST else "Integrar_informe_2025.py"
        
        resultado = subprocess.run(
            [path_python, path_scripts + script],
            capture_output=True, text=True
        )
        mensaje = resultado.stdout or resultado.stderr
    return render(request, 'fertilizantes/actualizacion.html', {'mensaje': mensaje})

# ==========================================
# 📊 REPORTES Y MENÚS
# ==========================================

@login_required
def reportes(request):
    return render(request, 'fertilizantes/reportes.html')

@login_required
def vistas(request):
    return render(request, 'fertilizantes/vistas.html')

@login_required
def visualizacion(request):
    return render(request, 'fertilizantes/visualizacion.html')

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
    anio = get_anio_context(request)
    tabla = f"entregas_diarias_{anio}"
    datos = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {tabla} LIMIT 100")
            columnas = [col[0] for col in cursor.description]
            datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
    except Exception: pass
    return render(request, 'fertilizantes/reporte_entregas_nacionales.html', {'datos': datos})

# ==========================================
# 📈 DASHBOARD AVANCE NACIONAL
# ==========================================

@login_required
def dashboard_avance_nacional(request):
    return render(request, 'fertilizantes/visualizacion/dashboard_nacional.html', {"timestamp": int(now().timestamp())})

@login_required
def dashboard_avance_nacional_2026(request):
    return render(request, 'fertilizantes/visualizacion/dashboard_nacional_2026.html', {"timestamp": int(now().timestamp())})

@login_required
def api_kpi_avance_nacional(request):
    anio = get_anio_context(request)
    tbl_avance = f"avance_operativo_ceda_{anio}"
    tbl_metas  = f"metas_{anio}"
    tbl_red    = f"red_distribucion_{anio}" if anio == "2026" else "red_distribucion"
    
    filtros = {
        "unidad": request.GET.get("unidad_operativa"),
        "estado": request.GET.get("estado"),
        "ceda": request.GET.get("id_ceda"),
    }
    tipo_meta = request.GET.get("tipo_meta", "operativa")

    cond, params = [], []
    if filtros["unidad"]:
        cond.append("rd.coordinacion_estatal = %s"); params.append(filtros["unidad"])
    if filtros["estado"]:
        cond.append("rd.estado = %s"); params.append(filtros["estado"])
    if filtros["ceda"]:
        cond.append("rd.id_ceda_agricultura = %s"); params.append(filtros["ceda"])

    where = "WHERE " + " AND ".join(cond) if cond else ""
    
    try:
        if tipo_meta == "oficial" and not filtros["ceda"]:
            sql = f"""
            WITH avance_estado AS (
                SELECT rd.estado,
                       SUM(a.dap_flete + a.urea_flete + a.dap_transfer + a.urea_transfer
                           + a.dap_remanente + a.urea_remanente
                           - a.dap_transf_out - a.urea_transf_out - a.dap_rem_out - a.urea_rem_out) AS abasto,
                       SUM(a.dap_dh + a.urea_dh) AS entregado,
                       SUM(a.dh_apoyados) AS dh_apoyados, SUM(a.ha_apoyadas) AS ha_apoyadas
                FROM   {tbl_avance} a JOIN {tbl_red} rd ON rd.id_ceda_agricultura = a.id_ceda_agricultura
                {where} GROUP BY rd.estado
            )
            SELECT SUM(m.total_ton), SUM(av.abasto), SUM(av.entregado), SUM(av.dh_apoyados), SUM(av.ha_apoyadas), SUM(m.derechohabientes), SUM(m.superficie_ha)
            FROM   avance_estado av JOIN {tbl_metas} m ON m.estado = av.estado;
            """    
        else:
            sql = f"""
                SELECT SUM(a.meta_total_ton), 
                       SUM(a.dap_flete + a.urea_flete + a.dap_transfer + a.urea_transfer + a.dap_remanente + a.urea_remanente - a.dap_transf_out - a.urea_transf_out - a.dap_rem_out - a.urea_rem_out),
                       SUM(a.dap_dh + a.urea_dh), SUM(a.dh_apoyados), SUM(a.ha_apoyadas), SUM(a.meta_derechohabientes), SUM(a.meta_superficie_ha)
                FROM {tbl_avance} a JOIN {tbl_red} rd ON a.id_ceda_agricultura = rd.id_ceda_agricultura {where}
            """

        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            meta, abasto, entregado, dh_apoyados, ha_apoyadas, meta_dh, meta_ha = cursor.fetchone()
    
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

    def pct(a, m): return min(math.floor((a or 0) * 100 / m), 100) if m else 0
    
    data = {
        "meta_total_ton": float(meta or 0),
        "abasto_recibido": float(abasto or 0),
        "entregado": float(entregado or 0),
        "derechohabientes_apoyados": int(dh_apoyados or 0),
        "superficie_beneficiada": float(ha_apoyadas or 0),
        "meta_dh": int(meta_dh or 0),
        "meta_ha": float(meta_ha or 0),
        "porc_abasto": pct(abasto, meta),
        "porc_entregado": pct(entregado, meta),
        "porc_dh": pct(dh_apoyados, meta_dh),
        "porc_ha": pct(ha_apoyadas, meta_ha),
        "pendiente_abasto": float(max((meta or 0) - (abasto or 0), 0)),
        "pendiente_entregado": float(max((meta or 0) - (entregado or 0), 0)),
        "pendiente_dh": int(max((meta_dh or 0) - (dh_apoyados or 0), 0)),
        "pendiente_ha": float(max((meta_ha or 0) - (ha_apoyadas or 0), 0)),
    }
    return JsonResponse(data)

@login_required
def api_kpi_avance_nacional_2026(request):
    return api_kpi_avance_nacional(request)

@login_required
def api_filtros_kpi(request):
    anio = get_anio_context(request)
    tbl_red = f"red_distribucion_{anio}" if anio == "2026" else "red_distribucion"
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT DISTINCT coordinacion_estatal FROM {tbl_red} ORDER BY 1")
            unidades = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT estado FROM {tbl_red} ORDER BY 1")
            estados = [r[0] for r in cursor.fetchall() if r[0]]
    except Exception: units, estados = [], []
    return JsonResponse({"unidades": unidades, "estados": estados})

@login_required
def resumen_estatal(request):
    return render(request, "fertilizantes/visualizacion/resumen_estatal.html")

@login_required
def api_tabla_resumen_por_estado(request):
    return JsonResponse({"resultados": []})

# ==========================================
# 🗂️ VISTAS DETALLADAS
# ==========================================

@login_required
def vista_fletes_transito(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM fletes_en_transito_resumen_estado ORDER BY max_dias_en_transito DESC")
        columnas = [col[0] for col in cursor.description]
        datos = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
    
    totales = {
        'fletes_transito_dap': sum(d['fletes_transito_dap'] for d in datos),
        'fletes_transito_urea': sum(d['fletes_transito_urea'] for d in datos),
        'total_fletes_transito': sum(d['total_fletes_transito'] for d in datos),
        'ton_transito_dap': sum(d['ton_transito_dap'] for d in datos),
        'ton_transito_urea': sum(d['ton_transito_urea'] for d in datos),
        'total_ton_transito': sum(d['total_ton_transito'] for d in datos),
        'max_dias_en_transito': max((d['max_dias_en_transito'] for d in datos), default=0),
    }
    return render(request, 'fertilizantes/vista_fletes_transito.html', {'datos': datos, 'totales': totales})

@login_required
def vista_fletes_transito_por_CEDA(request):
    u, e, z = request.GET.get('unidad_operativa'), request.GET.get('estado'), request.GET.get('zona_operativa')
    cond, params = [], []
    if u: cond.append("unidad_operativa = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    if z: cond.append("zona_operativa = %s"); params.append(z)
    where = f"WHERE {' AND '.join(cond)}" if cond else ""

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM fletes_en_transito_resumen {where} ORDER BY max_dias_en_transito DESC", params)
        cols = [col[0] for col in cursor.description]
        datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT unidad_operativa FROM fletes_en_transito_resumen ORDER BY 1")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]
        cursor.execute("SELECT DISTINCT estado FROM fletes_en_transito_resumen ORDER BY 1")
        estados = [r[0] for r in cursor.fetchall() if r[0]]
        cursor.execute("SELECT DISTINCT zona_operativa FROM fletes_en_transito_resumen ORDER BY 1")
        zonas = [r[0] for r in cursor.fetchall() if r[0]]

    t = {'fletes_dap':0, 'fletes_urea':0, 'total_fletes':0, 'ton_dap':0, 'ton_urea':0, 'ton_total':0, 'max_dias':0}
    if datos:
        t['fletes_dap'] = sum(d['fletes_transito_dap'] for d in datos)
        t['total_fletes'] = sum(d['total_fletes_transito'] for d in datos)
        t['ton_total'] = sum(d['ton_transito_dap'] + d['ton_transito_urea'] for d in datos)
        t['max_dias'] = max(d['max_dias_en_transito'] for d in datos)

    return render(request, 'fertilizantes/vista_fletes_transito_por_CEDA.html', {
        'datos': datos, 'unidades': unidades, 'estados': estados, 'zonas': zonas,
        'unidad_seleccionada': u, 'estado_seleccionado': e, 'zona_seleccionada': z, 'totales': t
    })

@login_required
def vista_fletes_autorizados_en_transito(request):
    u, e, z = request.GET.get('unidad_operativa'), request.GET.get('estado'), request.GET.get('zona_operativa')
    cond, params = [], []
    if u: cond.append("unidad_operativa = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    if z: cond.append("zona_operativa = %s"); params.append(z)
    where = f"WHERE {' AND '.join(cond)}" if cond else ""

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM fletes_en_transito_detalle {where} ORDER BY dias_en_transito DESC", params)
        cols = [col[0] for col in cursor.description]
        datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
        
        cursor.execute("SELECT DISTINCT unidad_operativa FROM fletes_en_transito_detalle ORDER BY 1")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]
        cursor.execute("SELECT DISTINCT estado FROM fletes_en_transito_detalle ORDER BY 1")
        estados = [r[0] for r in cursor.fetchall() if r[0]]
        cursor.execute("SELECT DISTINCT zona_operativa FROM fletes_en_transito_detalle ORDER BY 1")
        zonas = [r[0] for r in cursor.fetchall() if r[0]]

    total = sum(f['toneladas_iniciales'] for f in datos)
    return render(request, 'fertilizantes/vista_fletes_autorizados_en_transito.html', {
        'datos': datos, 'total_fletes': len(datos), 'total_toneladas': total,
        'max_dias': max((f['dias_en_transito'] for f in datos), default=0),
        'unidades': unidades, 'estados': estados, 'zonas': zonas,
        'unidad_seleccionada': u, 'estado_seleccionado': e, 'zona_seleccionada': z
    })

@login_required
def vista_inventario_diario_ceda(request):
    anio = get_anio_context(request)
    ceda = request.GET.get('ceda')
    datos, resumen, ceda_info = [], None, None
    tbl = f"inventario_acumulado_x_ceda_diario_{anio}"

    if ceda:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT fecha, id_ceda_agricultura, nombre_cedas, coordinacion_estatal as unidad_operativa,
                           estado, zona_operativa, dap_ton_total_entrada, urea_ton_total_entrada,
                           dap_ton_total_salida, urea_ton_total_salida, dap_ton_inventario_acumulado,
                           urea_ton_inventario_acumulado
                    FROM {tbl}
                    WHERE id_ceda_agricultura = %s
                      AND fecha >= (SELECT MIN(fecha) FROM {tbl} WHERE id_ceda_agricultura = %s AND (dap_ton_total_entrada > 0 OR urea_ton_total_entrada > 0))
                      AND fecha <= CURRENT_DATE
                    ORDER BY fecha DESC
                """, [ceda, ceda])
                cols = [col[0] for col in cursor.description]
                datos = [dict(zip(cols, f)) for f in cursor.fetchall()]

                if datos:
                    ceda_info = {'id': datos[0]['id_ceda_agricultura'], 'nombre': datos[0]['nombre_cedas'],
                                 'unidad': datos[0]['unidad_operativa'], 'estado': datos[0]['estado'], 'zona': datos[0]['zona_operativa']}
                
                cursor.execute(f"SELECT dap_ton_inventario_acumulado, urea_ton_inventario_acumulado FROM {tbl} WHERE id_ceda_agricultura = %s AND fecha = '{anio}-12-31'", [ceda])
                f = cursor.fetchone()
                if f: resumen = {'dap': f[0], 'urea': f[1], 'total': f[0]+f[1]}
        except Exception: pass

    return render(request, 'fertilizantes/vista_inventario_diario_ceda.html', {
        'datos': datos, 'resumen': resumen, 'ceda': ceda, 'ceda_info': ceda_info
    })

@login_required
def vista_inventarios_negativos_x_dia(request):
    anio = get_anio_context(request)
    tbl = f"inventarios_negativos_x_ceda_diario_{anio}"
    u, e = request.GET.get('unidad_operativa'), request.GET.get('estado')
    cond, params = [], []
    if u: cond.append("coordinacion_estatal = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    where = (" AND " + " AND ".join(cond)) if cond else ""

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT fecha, coordinacion_estatal AS unidad_operativa, estado, zona_operativa, 
                       id_ceda_agricultura, nombre_cedas, dap_ton_total_entrada, urea_ton_total_entrada,
                       dap_ton_total_salida, urea_ton_total_salida, 
                       dap_ton_inventario_acumulado AS dap_inventario, 
                       urea_ton_inventario_acumulado AS urea_inventario
                FROM {tbl} WHERE fecha <= CURRENT_DATE {where}
                ORDER BY unidad_operativa, estado, nombre_cedas, fecha DESC
            """, params)
            cols = [col[0] for col in cursor.description]
            datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
    except Exception: datos = []

    unidades = sorted(list(set(d['unidad_operativa'] for d in datos)))
    estados = sorted(list(set(d['estado'] for d in datos)))
    return render(request, 'fertilizantes/vista_inventarios_negativos_x_dia.html', {
        'datos': datos, 'unidades': unidades, 'estados': estados, 'unidad_seleccionada': u, 'estado_seleccionada': e
    })

@login_required
def vista_inventarios_negativos_actuales(request):
    anio = get_anio_context(request)
    tbl = f"inventarios_negativos_{anio}"
    u, e, z = request.GET.get('unidad_operativa'), request.GET.get('estado'), request.GET.get('zona_operativa')
    cond, params = [], []
    if u: cond.append("unidad_operativa = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    if z: cond.append("zona_operativa = %s"); params.append(z)
    where = f"WHERE {' AND '.join(cond)}" if cond else ""

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {tbl} {where} ORDER BY unidad_operativa, estado", params)
            cols = [col[0] for col in cursor.description]
            datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
            
            cursor.execute(f"SELECT DISTINCT unidad_operativa FROM {tbl} ORDER BY 1")
            unidades = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT estado FROM {tbl} ORDER BY 1")
            estados = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT zona_operativa FROM {tbl} ORDER BY 1")
            zonas = [r[0] for r in cursor.fetchall() if r[0]]
    except Exception:
        datos, unidades, estados, zonas = [], [], [], []

    return render(request, 'fertilizantes/vista_inventarios_negativos_actuales.html', {
        'datos': datos, 'unidades': unidades, 'estados': estados, 'zonas': zonas,
        'unidad_seleccionada': u, 'estado_seleccionado': e, 'zona_seleccionada': z
    })

@login_required
def vista_resumen_remanente_estado(request):
    anio = get_anio_context(request)
    tbl = f"resumen_remanente_estado_{anio}"
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {tbl}")
            cols = [col[0] for col in cursor.description]
            datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
    except Exception: datos = []
    return render(request, 'fertilizantes/vista_resumen_remanente_estado.html', {'datos': datos})

@login_required
def vista_cedas_con_remanentes(request):
    anio = get_anio_context(request)
    tbl = f"cedas_con_remanentes_{anio}"
    u, e = request.GET.get('unidad_operativa'), request.GET.get('estado')
    cond, params = [], []
    if u: cond.append("coordinacion_estatal = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    where = f"WHERE {' AND '.join(cond)}" if cond else ""

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {tbl} {where} ORDER BY coordinacion_estatal, estado", params)
            cols = [col[0] for col in cursor.description]
            datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
            
            cursor.execute(f"SELECT DISTINCT coordinacion_estatal FROM {tbl} ORDER BY 1")
            unidades = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT estado FROM {tbl} ORDER BY 1")
            estados = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT zona_operativa FROM {tbl} ORDER BY 1")
            zonas = [r[0] for r in cursor.fetchall() if r[0]]
    except Exception:
        datos, unidades, estados, zonas = [], [], [], []

    total_ton = sum(d['dap_ton_remanente_inventario'] + d['urea_ton_remanente_inventario'] for d in datos)
    return render(request, 'fertilizantes/vista_cedas_con_remanentes.html', {
        'datos': datos, 'unidades': unidades, 'estados': estados, 'zonas': zonas,
        'resumen': {'total_cedas': len(datos), 'total_ton': total_ton},
        'unidad_seleccionada': u, 'estado_seleccionado': e
    })

@login_required
def vista_cedas_con_remanentes_negativos(request):
    anio = get_anio_context(request)
    tbl = f"cedas_con_remanentes_negativos_{anio}"
    u, e = request.GET.get('unidad_operativa'), request.GET.get('estado')
    cond, params = [], []
    if u: cond.append("coordinacion_estatal = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    where = f"WHERE {' AND '.join(cond)}" if cond else ""

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {tbl} {where} ORDER BY coordinacion_estatal", params)
            cols = [col[0] for col in cursor.description]
            datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
            cursor.execute(f"SELECT DISTINCT coordinacion_estatal FROM {tbl} ORDER BY 1")
            unidades = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT estado FROM {tbl} ORDER BY 1")
            estados = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT zona_operativa FROM {tbl} ORDER BY 1")
            zonas = [r[0] for r in cursor.fetchall() if r[0]]
    except Exception: datos, unidades, estados, zonas = [], [], [], []

    return render(request, 'fertilizantes/vista_cedas_con_remanentes_negativos.html', {
        'datos': datos, 'unidades': unidades, 'estados': estados, 'zonas': zonas, 'unidad_seleccionada': u, 'estado_seleccionado': e
    })

# 🔥 VISTAS RESTAURADAS QUE FALTABAN 🔥

@login_required
def vista_fletes_ton_conteo_detalle(request):
    # Tabla fletes_ton_conteo_detalle_td
    u = request.GET.get('unidad_operativa')
    e = request.GET.get('estado')
    cond, params = [], []
    if u: cond.append("unidad_operativa = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    where = f"WHERE {' AND '.join(cond)}" if cond else ""
    
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM fletes_ton_conteo_detalle_td {where} ORDER BY toneladas_iniciales DESC", params)
        cols = [col[0] for col in cursor.description]
        datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
        cursor.execute("SELECT DISTINCT unidad_operativa FROM fletes_ton_conteo_detalle_td ORDER BY 1")
        unidades = [r[0] for r in cursor.fetchall() if r[0]]
        cursor.execute("SELECT DISTINCT estado FROM fletes_ton_conteo_detalle_td ORDER BY 1")
        estados = [r[0] for r in cursor.fetchall() if r[0]]
        cursor.execute("SELECT DISTINCT estado_procedencia FROM fletes_ton_conteo_detalle_td ORDER BY 1")
        proc = [r[0] for r in cursor.fetchall() if r[0]]

    return render(request, "fertilizantes/vista_fletes_ton_conteo_detalle.html", {
        "datos": datos, "unidades": unidades, "estados": estados, "procedencias": proc,
        "unidad_seleccionada": u, "estado_seleccionado": e
    })

@login_required
def vista_fletes_toneladas_recibidas_atipicas(request):
    anio = get_anio_context(request)
    tbl = f"fletes_toneladas_recibidas_atipicas_{anio}"
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {tbl} ORDER BY diferencia_ton DESC")
            cols = [col[0] for col in cursor.description]
            datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
    except Exception: datos = []
    return render(request, "fertilizantes/vista_fletes_toneladas_recibidas_atipicas.html", {"datos": datos})

@login_required
def vista_fletes_fechas_incoherentes(request):
    anio = get_anio_context(request)
    tbl = f"fletes_fechas_incoherentes_{anio}"
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {tbl} ORDER BY fecha_de_salida DESC")
            cols = [col[0] for col in cursor.description]
            datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
    except Exception: datos = []
    return render(request, "fertilizantes/vista_fletes_fechas_incoherentes.html", {"datos": datos})

@login_required
def vista_pedidos_detalle_fecha(request):
    anio = get_anio_context(request)
    tbl = f"pedidos_detalle_por_fecha_{anio}"
    u, e = request.GET.get('unidad_operativa'), request.GET.get('estado')
    fi, ff = request.GET.get('fecha_inicio'), request.GET.get('fecha_fin')
    
    cond, params = [], []
    if u: cond.append("unidad_operativa = %s"); params.append(u)
    if e: cond.append("estado = %s"); params.append(e)
    if fi: cond.append("fecha >= %s"); params.append(fi)
    if ff: cond.append("fecha <= %s"); params.append(ff)
    
    where = f"WHERE {' AND '.join(cond)}" if cond else ""

    # 🔥 PROTECCIÓN RESTAURADA: Si no hay filtros (cond), datos se queda vacío.
    datos = []
    resumen = {"total": 0, "dap": 0, "urea": 0}
    
    # Solo ejecutamos la consulta si hay condiciones
    if cond:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {tbl} {where} ORDER BY fecha DESC", params)
                cols = [col[0] for col in cursor.description]
                datos = [dict(zip(cols, f)) for f in cursor.fetchall()]
        except Exception: 
            datos = []

        # Calcular resumen solo si hubo datos
        if datos:
            resumen = {
                "total": len(datos), 
                "dap": sum(d['dap'] for d in datos), 
                "urea": sum(d['urea'] for d in datos)
            }

    # Los filtros (combos) se cargan siempre para que el usuario pueda elegir
    unidades, estados, zonas = [], [], []
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT DISTINCT unidad_operativa FROM {tbl} ORDER BY 1")
            unidades = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT estado FROM {tbl} ORDER BY 1")
            estados = [r[0] for r in cursor.fetchall() if r[0]]
            cursor.execute(f"SELECT DISTINCT zona_operativa FROM {tbl} ORDER BY 1")
            zonas = [r[0] for r in cursor.fetchall() if r[0]]
    except Exception: pass

    return render(request, "fertilizantes/pedidos_detalle_por_fecha.html", {
        "datos": datos, "resumen": resumen, "unidades": unidades, "estados": estados, "zonas": zonas,
        "unidad_seleccionada": u, "estado_seleccionado": e, "fecha_inicio": fi, "fecha_fin": ff
    })

# 🔥 AQUÍ ESTÁ LA VISTA QUE CAUSABA EL ERROR (RESTAURADA) 🔥
@login_required
def vista_inventario_ceda_diario(request):
    """
    Vista comparativa Campo vs SIGAP (antes 'vista_inventario_ceda_diario_campo').
    """
    anio = get_anio_context(request)
    tbl = f"inventario_ceda_diario_{anio}_campo_sigap"
    cols = ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura"]
    where, params, ctx = _aplicar_filtros_get(request, cols)

    with connection.cursor() as cur:
        cur.execute(f"SELECT DISTINCT unidad_operativa FROM {tbl} WHERE unidad_operativa IS NOT NULL ORDER BY 1")
        ctx["unidades"] = [r[0] for r in cur.fetchall()]

    ctx["unidad_operativa_seleccionada"] = ctx.get("unidad_operativa_seleccionado")

    if not params:
        ctx.update({"datos": [], "mensaje": "Seleccione al menos un filtro."})
        return render(request, "fertilizantes/vista_inventario_ceda_diario_campo.html", ctx)

    if not request.GET.get("fecha_fin", "").strip():
        ayer = date.today() - timedelta(days=1)
        where += (" AND " if where else " WHERE ") + "fecha <= %s"
        params.append(ayer)
        ctx["fecha_fin"] = ayer.strftime("%Y-%m-%d")

    sql = f"SELECT * FROM {tbl} {where} ORDER BY fecha DESC, id_ceda_agricultura"
    with connection.cursor() as cur:
        cur.execute(sql, params)
        headers = [c[0] for c in cur.description]
        ctx["datos"] = [dict(zip(headers, row)) for row in cur.fetchall()]

    return render(request, "fertilizantes/vista_inventario_ceda_diario_campo.html", ctx)

# ==========================================
# 🔍 CONSULTA DERECHOHABIENTES (Paginada)
# ==========================================

# Asegúrate de tener este import al inicio de tu archivo views.py

@login_required
def vista_derechohabientes(request):
    """
    Consulta paginada en servidor (Server-side Pagination).
    Permite navegar millones de registros en bloques de 200.
    """
    qs = DH.objects.all()

    # --- 1. Filtros Generales ---
    uo   = request.GET.get("unidad_operativa")
    edo  = request.GET.get("estado")
    ceda = request.GET.get("id_ceda_agricultura")
    
    if uo: qs = qs.filter(unidad_operativa=uo)
    if edo: qs = qs.filter(estado=edo)
    if ceda: qs = qs.filter(id_ceda_agricultura=ceda)

    # --- 2. Filtros de Fecha ---
    fecha_inicio = request.GET.get("fecha_inicio")
    fecha_fin    = request.GET.get("fecha_fin")

    if fecha_inicio: qs = qs.filter(fecha_entrega__gte=fecha_inicio)
    if fecha_fin: qs = qs.filter(fecha_entrega__lte=fecha_fin)

    # --- 3. Búsquedas Específicas ---
    for campo in ("acuse_estatal", "curp_solicitud", "curp_renapo"):
        v = request.GET.get(campo)
        if v: qs = qs.filter(**{f"{campo}__iexact": v})

    # --- 4. Búsqueda Masiva (Lista) ---
    lista = request.GET.get("lista_acuses")
    if lista:
        acuses = [l.strip() for l in lista.splitlines() if l.strip()]
        qs = qs.filter(acuse_estatal__in=acuses)

    # --- 5. Selección de Columnas ---
    seleccion = request.GET.getlist("campos") or [f for f, _ in CAMPOS_DH]
    validos = {f for f, _ in CAMPOS_DH}
    seleccion = [f for f in seleccion if f in validos]

    # --- MODO 1: DESCARGA CSV (Todo el resultado) ---
    # Si el usuario hace clic en "Descargar", se exporta todo sin paginar.
    if request.GET.get("csv") == "1":
        qs = qs.values(*seleccion) # Optimización: traer solo columnas necesarias
        
        def filas():
            # Encabezados
            yield [lbl for f, lbl in CAMPOS_DH if f in seleccion]
            # Datos (usando iterator para no saturar RAM)
            for fila in qs.iterator(chunk_size=10000):
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
                    data = '\ufeff' + data  # BOM para Excel
                    first = False
                yield data

        nombre = f"derechohabientes_{datetime.date.today():%Y%m%d}.csv"
        resp = StreamingHttpResponse(stream(), content_type="text/csv; charset=utf-8")
        resp["Content-Disposition"] = f'attachment; filename="{nombre}"'
        return resp

    # --- MODO 2: PANTALLA (Paginación) ---
    
    # Solo buscamos si hay algún filtro aplicado para evitar cargar 2M de registros al inicio
    filtros_activos = bool(request.GET)
    
    if filtros_activos:
        # Optimizamos query
        qs = qs.values(*seleccion)
        
        # Paginador: 200 registros por página
        paginator = Paginator(qs, 200)
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
    else:
        # Lista vacía si no hay filtros
        page_obj = []

    # Listas para los dropdowns
    unidades = DH.objects.order_by('unidad_operativa').values_list('unidad_operativa', flat=True).distinct()
    estados  = DH.objects.order_by('estado').values_list('estado', flat=True).distinct()

    return render(request, "fertilizantes/vista_derechohabientes.html", {
        "datos":         page_obj,  # Objeto paginado
        "campos":        CAMPOS_DH,
        "seleccionados": seleccion,
        "unidades":      unidades,
        "estados":       estados,
        # Preservar filtros seleccionados en el template
        "unidad_seleccionada": uo,
        "estado_seleccionada": edo,
    })

# ==========================================
# 💬 COMENTARIOS Y AJAX
# ==========================================

@login_required
def comentarios_por_ceda(request):
    from datetime import datetime
    if request.method == "POST":
        id_ceda = request.POST.get("id_ceda_agricultura")
        texto = request.POST.get("comentario", "").strip()
        if id_ceda and texto:
            with psycopg_conn.cursor() as cur:
                cur.execute("INSERT INTO comentarios_ceda (id_ceda_agricultura, comentario, fecha) VALUES (%s, %s, %s)", 
                            (id_ceda, texto, datetime.now()))
                psycopg_conn.commit()
        return redirect("comentarios_por_ceda")

    where, params, ctx = _aplicar_filtros_get(request, ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura"])
    params = [p for p in params if p not in ("", None)]
    
    with connection.cursor() as cur:
        cur.execute("SELECT DISTINCT unidad_operativa FROM vista_comentarios_ceda WHERE unidad_operativa IS NOT NULL ORDER BY 1")
        ctx["unidades"] = [r[0] for r in cur.fetchall()]

    if not params:
        ctx["filtro_aplicado"] = False
        return render(request, "fertilizantes/comentarios_por_ceda.html", ctx)

    query = f"SELECT * FROM vista_comentarios_ceda {where} ORDER BY unidad_operativa, estado, zona_operativa, nombre_cedas"
    ctx["df"] = pd.read_sql(query, con=engine, params=[tuple(params)])
    ctx["filtro_aplicado"] = True
    return render(request, "fertilizantes/comentarios_por_ceda.html", ctx)

@require_GET
def ajax_zonas_por_filtros(request):
    unidad, estado = request.GET.get("unidad", "").strip(), request.GET.get("estado", "").strip()
    if not unidad and not estado: return JsonResponse({"zonas": []})
    
    query, params = "SELECT DISTINCT zona_operativa FROM vista_comentarios_ceda WHERE 1=1", []
    if unidad: query += " AND unidad_operativa = %s"; params.append(unidad)
    if estado: query += " AND estado = %s"; params.append(estado)
    
    try:
        zonas = pd.read_sql(query, con=engine, params=params)
        lista = sorted(zonas["zona_operativa"].dropna().unique().tolist())
    except: lista = []
    return JsonResponse({"zonas": lista})

@require_GET
@login_required
def ajax_filtros_generales(request):
    tabla, unidad, estado = request.GET.get("tabla"), request.GET.get("unidad_operativa"), request.GET.get("estado")
    anio = get_anio_context(request)
    
    tablas_validas = {
        "fletes_en_transito_detalle": "unidad_operativa",
        "vista_fletes_autorizados_en_transito": "unidad_operativa",
        "vista_fletes_transito_por_CEDA": "unidad_operativa",
        "vista_comentarios_ceda": "unidad_operativa",
        f"inventarios_negativos_{anio}": "unidad_operativa",
        f"cedas_con_remanentes_negativos_{anio}": "coordinacion_estatal",
        f"inventario_ceda_diario_{anio}_campo_sigap": "unidad_operativa",
        "estadisticas_inventarios_campo": "unidad_operativa",
        f"cedas_con_remanentes_{anio}": "coordinacion_estatal",
    }
    
    # Fallback para nombres sin año (2025 default si falla)
    if tabla not in tablas_validas:
        if tabla == "inventarios_negativos": tabla = f"inventarios_negativos_{anio}"
        if tabla == "cedas_con_remanentes_negativos": tabla = f"cedas_con_remanentes_negativos_{anio}"
        if tabla not in tablas_validas: return JsonResponse({"error": "Tabla no autorizada"}, status=400)

    col_uo = tablas_validas.get(tabla, "unidad_operativa")
    resp = {"estados": [], "zonas": []}

    try:
        with connection.cursor() as cur:
            sql = f"SELECT DISTINCT estado FROM {tabla} " + (f"WHERE {col_uo} = %s" if unidad else "") + " ORDER BY estado"
            cur.execute(sql, [unidad] if unidad else [])
            resp["estados"] = [r[0] for r in cur.fetchall()]
            
            if estado:
                sql = f"SELECT DISTINCT zona_operativa FROM {tabla} WHERE estado = %s" + (f" AND {col_uo} = %s" if unidad else "") + " ORDER BY 1"
                cur.execute(sql, [estado, unidad] if unidad else [estado])
                resp["zonas"] = [r[0] for r in cur.fetchall()]
    except Exception: pass
    return JsonResponse(resp)

@require_GET
def vista_estadisticas_inventarios_campo(request):
    where, params, ctx = _aplicar_filtros_get(request, ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura"])
    with connection.cursor() as cur:
        cur.execute("SELECT DISTINCT unidad_operativa FROM estadisticas_inventarios_campo WHERE unidad_operativa IS NOT NULL ORDER BY 1")
        ctx["unidades"] = [r[0] for r in cur.fetchall()]
    
    if not params:
        ctx["datos"], ctx["mensaje"] = [], "Seleccione al menos un filtro."
    else:
        with connection.cursor() as cur:
            cur.execute(f"SELECT * FROM estadisticas_inventarios_campo {where} ORDER BY unidad_operativa", params)
            cols = [c[0] for c in cur.description]
            ctx["datos"] = [dict(zip(cols, r)) for r in cur.fetchall()]
    return render(request, "fertilizantes/vista_estadisticas_inventarios_campo.html", ctx)

# ==========================================
# 🚚 CONSULTA FLETES (Materialized View)
# ==========================================

MATVIEW = "mv_fletes_enriquecidos"

def _fetch_all_columns():
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT a.attname FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'm' AND n.nspname = 'public' AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum;
            """, [MATVIEW])
            return [r[0] for r in cur.fetchall()]
    except Exception: return []

def _norm_date(value):
    if not value: return None
    try: return datetime.datetime.strptime(str(value).strip(), "%d/%m/%Y").date().isoformat()
    except: pass
    d = parse_date(str(value).strip())
    return d.isoformat() if d else None

def _build_where_and_params(q):
    where, params = [], []
    for f in ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura", "estatus", "abreviacion_producto", "cdf_destino_original", "cdf_destino_final"]:
        if q.get(f): where.append(f"{f} = %s"); params.append(q[f])
    
    if q.get("fecha_salida_ini"): where.append("fecha_de_salida::date >= %s"); params.append(q["fecha_salida_ini"])
    if q.get("fecha_salida_fin"): where.append("fecha_de_salida::date <= %s"); params.append(q["fecha_salida_fin"])
    if q.get("fecha_entrega_ini"): where.append("fecha_de_entrega::date >= %s"); params.append(q["fecha_entrega_ini"])
    if q.get("fecha_entrega_fin"): where.append("fecha_de_entrega::date <= %s"); params.append(q["fecha_entrega_fin"])

    raw_folios = (q.get("folios_multiline") or "").strip()
    folios = [l.strip() for l in raw_folios.replace(",", "\n").splitlines() if l.strip()]
    if folios: where.append("folio_del_flete = ANY(%s)"); params.append(folios)

    return ("WHERE " + " AND ".join(where)) if where else "", params

# ==========================================
# 🚚 VISTA FLETES (Mejorada: Paginación Server-Side)
# ==========================================

@login_required
def vista_fletes(request):
    """
    Consulta de fletes con Paginación en Servidor y diseño robusto.
    Reemplaza la lógica de JS cliente por procesamiento en backend.
    """
    # 1. Obtener todas las columnas posibles de la Vista Materializada
    all_cols = _fetch_all_columns()
    
    # 2. Definir columnas por defecto si no se seleccionan
    defaults = [
        "folio_del_flete", "unidad_operativa", "estado", "zona_operativa", 
        "id_ceda_agricultura", "estatus", "abreviacion_producto", 
        "fecha_de_salida", "fecha_de_entrega"
    ]
    
    # 3. Procesar filtros del GET
    # Normalizar fechas
    fecha_salida_ini = _norm_date(request.GET.get("fecha_salida_ini"))
    fecha_salida_fin = _norm_date(request.GET.get("fecha_salida_fin"))
    fecha_entrega_ini = _norm_date(request.GET.get("fecha_entrega_ini"))
    fecha_entrega_fin = _norm_date(request.GET.get("fecha_entrega_fin"))

    # Construir diccionario de filtros
    q = {
        "unidad_operativa": request.GET.get("unidad_operativa"),
        "estado": request.GET.get("estado"),
        "zona_operativa": request.GET.get("zona_operativa"),
        "id_ceda_agricultura": request.GET.get("id_ceda_agricultura"),
        "estatus": request.GET.get("estatus"),
        "abreviacion_producto": request.GET.get("producto"), # Ojo: en template el name debe coincidir
        "cdf_destino_original": request.GET.get("cdf_destino_original"),
        "cdf_destino_final": request.GET.get("cdf_destino_final"),
        "fecha_salida_ini": fecha_salida_ini,
        "fecha_salida_fin": fecha_salida_fin,
        "fecha_entrega_ini": fecha_entrega_ini,
        "fecha_entrega_fin": fecha_entrega_fin,
        "folios_multiline": request.GET.get("folios_multiline"),
    }

    # 4. Determinar columnas seleccionadas
    user_cols = request.GET.getlist("columnas")
    # Si es la primera carga (sin GET) o no hay selección, usar defaults que existan en la tabla
    if not request.GET and not user_cols:
        cols = [c for c in defaults if c in all_cols]
    else:
        cols = [c for c in user_cols if c in all_cols] or [c for c in defaults if c in all_cols]

    # 5. Construir SQL WHERE
    where, params = _build_where_and_params(q)

    # --- MODO 1: DESCARGA CSV ---
    if request.GET.get("csv") == "1":
        sql = f"SELECT {', '.join(cols)} FROM {MATVIEW} {where} ORDER BY fecha_de_salida DESC NULLS LAST"
        
        # Usamos generator para StreamingHttpResponse (igual que en Derechohabientes)
        def filas():
            yield cols # Header
            with connection.cursor() as cur:
                cur.execute(sql, params)
                while True:
                    rows = cur.fetchmany(5000)
                    if not rows: break
                    for row in rows:
                        # Limpiamos datos para evitar errores de codificación
                        yield [smart_str(v) if v is not None else "" for v in row]

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
                    data = '\ufeff' + data  # BOM para Excel
                    first = False
                yield data

        response = StreamingHttpResponse(stream(), content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="consulta_fletes.csv"'
        return response

    # --- MODO 2: PANTALLA (Paginada) ---
    datos_paginados = None
    
    # Solo consultamos si hay filtros aplicados para evitar carga inicial pesada
    if request.GET:
        # A) Conteo total
        count_sql = f"SELECT COUNT(*) FROM {MATVIEW} {where}"
        with connection.cursor() as cur:
            cur.execute(count_sql, params)
            total_registros = cur.fetchone()[0]

        # B) Paginación manual
        try:
            page_number = int(request.GET.get("page", 1))
        except ValueError:
            page_number = 1
            
        page_size = 100
        offset = (page_number - 1) * page_size
        
        data_sql = f"""
            SELECT {', '.join(cols)} 
            FROM {MATVIEW} 
            {where} 
            ORDER BY fecha_de_salida DESC NULLS LAST 
            LIMIT {page_size} OFFSET {offset}
        """
        
        with connection.cursor() as cur:
            cur.execute(data_sql, params)
            rows = cur.fetchall()
            page_obj = [dict(zip(cols, row)) for row in rows]

        # C) Crear objeto Paginator "falso" para el template
        class FakePaginator:
            def __init__(self, total, size, current):
                self.count = total
                self.num_pages = math.ceil(total / size)
                self.current = current
                self.has_next = current < self.num_pages
                self.has_previous = current > 1
                self.next_page_number = current + 1
                self.previous_page_number = current - 1
                self.start_index = offset + 1
                self.end_index = min(offset + size, total)

        datos_paginados = FakePaginator(total_registros, page_size, page_number)
        datos_paginados.object_list = page_obj 

    return render(request, "fertilizantes/vistas_fletes.html", {
        "columnas_disponibles": all_cols,
        "seleccionadas": cols,
        "datos": datos_paginados,
        "mv_tiene_columnas": bool(all_cols),
        "matview": MATVIEW,
        "filtros": q 
    })

@require_GET
def api_fletes_opciones(request):
    q = {k: request.GET.get(k) for k in ["unidad_operativa", "estado", "zona_operativa"]}
    where, params = _build_where_and_params(q)
    def distinct_of(field):
        try:
            with connection.cursor() as cur:
                cur.execute(f"SELECT DISTINCT {field} FROM {MATVIEW} {where} ORDER BY {field} NULLS LAST;", params)
                return [r[0] for r in cur.fetchall() if r[0] not in (None, "")]
        except: return []
    
    return JsonResponse({
        "unidad_operativa": distinct_of("unidad_operativa"), "estado": distinct_of("estado"),
        "zona_operativa": distinct_of("zona_operativa"), "id_ceda_agricultura": distinct_of("id_ceda_agricultura"),
        "estatus": distinct_of("estatus"), "abreviacion_producto": distinct_of("abreviacion_producto"),
        "cdf_destino_original": distinct_of("cdf_destino_original"), "cdf_destino_final": distinct_of("cdf_destino_final"),
    })

@require_POST
def api_fletes_consultar(request):
    try: body = json.loads(request.body.decode("utf-8"))
    except: return HttpResponseBadRequest("JSON inválido")
    
    for f in ["fecha_salida_ini", "fecha_salida_fin", "fecha_entrega_ini", "fecha_entrega_fin"]:
        body[f] = _norm_date(body.get(f))
    
    q = {k: (body.get(k) or None) for k in ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura", "estatus", "abreviacion_producto", "cdf_destino_original", "cdf_destino_final", "fecha_salida_ini", "fecha_salida_fin", "fecha_entrega_ini", "fecha_entrega_fin", "folios_multiline"]}
    
    all_cols = _fetch_all_columns()
    user_cols = body.get("columnas") or []
    cols = [c for c in user_cols if c in all_cols]
    if not cols: cols = [c for c in ["folio_del_flete", "unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura", "estatus", "abreviacion_producto", "fecha_de_salida", "fecha_de_entrega"] if c in all_cols]

    where, params = _build_where_and_params(q)
    sql = f"SELECT {', '.join(cols)} FROM {MATVIEW} {where} LIMIT 10000;"
    
    try:
        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            
            # Resumen Agregado
            agg_sql = f"""
                SELECT COUNT(*)::int, COALESCE(SUM(toneladas_iniciales), 0)::float, COALESCE(SUM(toneladas_en_el_destino), 0)::float,
                       COUNT(*) FILTER (WHERE abreviacion_producto = 'DAP')::int, COUNT(*) FILTER (WHERE abreviacion_producto = 'UREA')::int,
                       COALESCE(SUM(CASE WHEN abreviacion_producto='DAP' THEN toneladas_iniciales END), 0)::float,
                       COALESCE(SUM(CASE WHEN abreviacion_producto='DAP' THEN toneladas_en_el_destino END), 0)::float,
                       COALESCE(SUM(CASE WHEN abreviacion_producto='UREA' THEN toneladas_iniciales END), 0)::float,
                       COALESCE(SUM(CASE WHEN abreviacion_producto='UREA' THEN toneladas_en_el_destino END), 0)::float
                FROM {MATVIEW} {where};
            """
            cur.execute(agg_sql, params)
            a = cur.fetchone()
            summary = {
                "total": a[0], "sum_toneladas_iniciales": a[1], "sum_toneladas_en_el_destino": a[2],
                "DAP": {"count": a[3], "sum_toneladas_iniciales": a[5], "sum_toneladas_en_el_destino": a[6]},
                "UREA": {"count": a[4], "sum_toneladas_iniciales": a[7], "sum_toneladas_en_el_destino": a[8]},
            }
    except Exception as e: return HttpResponseBadRequest(f"Error: {e}")

    return JsonResponse({"columns": cols, "rows": rows, "count": len(rows), "summary": summary})

@require_POST
def api_fletes_exportar_csv(request):
    try: body = json.loads(request.body.decode("utf-8"))
    except: return HttpResponseBadRequest("JSON inválido")
    
    for f in ["fecha_salida_ini", "fecha_salida_fin", "fecha_entrega_ini", "fecha_entrega_fin"]:
        body[f] = _norm_date(body.get(f))
    
    q = {k: (body.get(k) or None) for k in ["unidad_operativa", "estado", "zona_operativa", "id_ceda_agricultura", "estatus", "abreviacion_producto", "cdf_destino_original", "cdf_destino_final", "fecha_salida_ini", "fecha_salida_fin", "fecha_entrega_ini", "fecha_entrega_fin", "folios_multiline"]}
    
    all_cols = _fetch_all_columns()
    user_cols = body.get("columnas") or []
    cols = [c for c in user_cols if c in all_cols] or all_cols
    
    where, params = _build_where_and_params(q)
    sql = f"SELECT {', '.join(cols)} FROM {MATVIEW} {where};"
    
    try:
        with connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    except Exception as e: return HttpResponseBadRequest(f"Error: {e}")

    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="fletes_consulta.csv"'
    writer = csv.writer(response)
    writer.writerow(cols)
    for r in rows: writer.writerow([smart_str(v) for v in r])
    return response


# ==============================================
#   OCR RÁPIDO (MEJORADO)
# ==============================================

def _decode_data_url(data_url: str) -> bytes:
    if not data_url.startswith("data:"):
        raise ValueError("No es un data URL válido")
    header, b64data = data_url.split(",", 1)
    return base64.b64decode(b64data)

def _procesar_imagen_pil(img: Image.Image, lang: str = "spa+eng", config: str = "--psm 6") -> str:
    """
    Preprocesamiento optimizado para capturas de pantalla y documentos.
    """
    # 1. Convertir a Escala de Grises
    img = ImageOps.grayscale(img)
    
    # 2. ✨ MEJORA CLAVE: Re-escalado (Upscaling) 
    # Aumentar el tamaño 2x ayuda a Tesseract a ver los espacios entre palabras
    width, height = img.size
    if width < 2000: # Solo escalamos si no es gigante
        new_size = (width * 2, height * 2)
        img = img.resize(new_size, Image.Resampling.LANCZOS)
    
    # 3. Mejora de Contraste (Sin binarización agresiva ni desenfoque)
    img = ImageOps.autocontrast(img)
    
    # (Opcional) Enfocar ligeramente los bordes
    # img = img.filter(ImageFilter.SHARPEN) 

    # 4. Ejecutar OCR
    texto = pytesseract.image_to_string(img, lang=lang, config=config)
    return texto.strip()

@login_required
def ocr_page(request):
    return render(request, "fertilizantes/ocr_page.html")

@login_required
@require_POST
def ocr_extract(request):
    try:
        # Valores por defecto
        lang = "spa+eng"
        psm = "6"
        whitelist = ""
        
        # Detectar origen de datos (FormData vs JSON)
        # El fetch de JS usa FormData normalmente, pero tu versión anterior usaba JSON para clipboard.
        # Aquí unificamos la lógica de extracción de parámetros.
        
        if request.content_type.startswith("multipart/form-data"):
            # Caso: Archivo subido o FormData standard
            lang = request.POST.get("lang", lang)
            psm = request.POST.get("psm", psm)
            whitelist = request.POST.get("whitelist", "")
            
            # Construir config string
            cfg = f"--oem 3 --psm {psm}"
            if whitelist:
                cfg += f' -c tessedit_char_whitelist="{whitelist}"'

            if "file" in request.FILES:
                f = request.FILES["file"]
                if f.name.lower().endswith(".pdf"):
                    if not HAS_PDF: return HttpResponseBadRequest("Servidor sin soporte PDF.")
                    images = convert_from_bytes(f.read(), dpi=300)
                    textos = [_procesar_imagen_pil(img, lang, cfg) for img in images]
                    return JsonResponse({"texto": "\n\n".join(textos)})
                else:
                    img = Image.open(f)
                    texto = _procesar_imagen_pil(img, lang, cfg)
                    return JsonResponse({"texto": texto})
            
            elif "clipboard_data" in request.POST:
                # Caso FormData con base64
                data_url = request.POST["clipboard_data"]
                bytes_data = _decode_data_url(data_url)
                
                if "application/pdf" in data_url:
                    if not HAS_PDF: return HttpResponseBadRequest("Servidor sin soporte PDF.")
                    images = convert_from_bytes(bytes_data, dpi=300)
                    textos = [_procesar_imagen_pil(img, lang, cfg) for img in images]
                    return JsonResponse({"texto": "\n\n".join(textos)})
                else:
                    img = Image.open(io.BytesIO(bytes_data))
                    texto = _procesar_imagen_pil(img, lang, cfg)
                    return JsonResponse({"texto": texto})

        # Fallback si el JS envía JSON puro (tu versión anterior)
        elif request.content_type.startswith("application/json"):
            body = json.loads(request.body.decode("utf-8"))
            # Extraer params del body si existen, o usar defaults
            # (Tu JS anterior no los mandaba en el JSON, pero por si acaso)
            
            data_url = body.get("data_url")
            if not data_url: return HttpResponseBadRequest("Falta data_url")
            
            # Config default para JSON ya que no vienen en el body actual del JS
            cfg = f"--oem 3 --psm 6" 
            
            img_bytes = _decode_data_url(data_url)
            
            if "application/pdf" in data_url:
                 images = convert_from_bytes(img_bytes, dpi=300)
                 textos = [_procesar_imagen_pil(img, "spa+eng", cfg) for img in images]
                 return JsonResponse({"texto": "\n\n".join(textos)})
            else:
                img = Image.open(io.BytesIO(img_bytes))
                texto = _procesar_imagen_pil(img, "spa+eng", cfg)
                return JsonResponse({"texto": texto})

        return HttpResponseBadRequest("Solicitud no válida.")

    except Exception as e:
        return HttpResponseBadRequest(f"Error procesando OCR: {e}")