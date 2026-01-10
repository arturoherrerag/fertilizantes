from django.urls import path
from . import views
from .views import comentarios_por_ceda

urlpatterns = [

    # URLS sistema 2025
    path('inicio/', views.inicio, name='inicio'),
    path('seleccionar-anio/', views.seleccionar_anio, name='seleccionar_anio'),
    path('actualizacion/', views.actualizar_bases, name='actualizacion'),
    path('reportes/', views.reportes, name='reportes'),
    path('reportes/fletes_transito/', views.reporte_fletes_transito, name='fletes_transito'),
    path('reportes/inventarios_negativos/', views.reporte_inventarios_negativos, name='inventarios_negativos'),
    path('reportes/inventarios_negativos_remanente/', views.reporte_inventarios_negativos_remanente, name='inventarios_negativos_remanente'),
    path('reportes/entregas_nacionales/', views.reporte_entregas_nacionales, name='entregas_nacionales'),
    path('visualizacion/', views.visualizacion, name='visualizacion'),
    path('vistas/', views.vistas, name='vistas'),
    path('vistas/fletes_transito/', views.vista_fletes_transito, name='vista_fletes_transito'),
    path('vistas/inventarios_negativos_actuales/', views.vista_inventarios_negativos_actuales, name='vista_inventarios_negativos_actuales'),
    path('vistas/resumen_remanente_estado/', views.vista_resumen_remanente_estado, name='vista_resumen_remanente_estado'),
    path('vistas/cedas_con_remanentes/', views.vista_cedas_con_remanentes, name='vista_cedas_con_remanentes'),
    path('vistas/cedas_con_remanentes_negativos/', views.vista_cedas_con_remanentes_negativos, name='vista_cedas_con_remanentes_negativos'),
    path('vistas/fletes_transito_por_ceda/', views.vista_fletes_transito_por_CEDA, name='vista_fletes_transito_por_CEDA'),
    path('vistas/fletes_autorizados_en_transito/', views.vista_fletes_autorizados_en_transito, name='vista_fletes_autorizados_en_transito'),
    path('api/filtros_generales/', views.ajax_filtros_generales, name='ajax_filtros_generales'),
    path('vistas/inventario_diario_ceda/', views.vista_inventario_diario_ceda, name='vista_inventario_diario_ceda'),
    path('inventarios-negativos-x-dia/', views.vista_inventarios_negativos_x_dia, name='vista_inventarios_negativos_x_dia'),
    path('fletes-conteo-toneladas/', views.vista_fletes_ton_conteo_detalle, name='vista_fletes_ton_conteo_detalle'),
    path("vistas/fletes_atipicos/", views.vista_fletes_toneladas_recibidas_atipicas, name="vista_fletes_atipicos"),
    path("vistas/fletes_fechas_incoherentes/", views.vista_fletes_fechas_incoherentes, name="vista_fletes_fechas_incoherentes"),
    path("pedidos/detalle-por-fecha/", views.vista_pedidos_detalle_fecha, name="pedidos_detalle_por_fecha"),
    path('visualizacion/avance/', views.dashboard_avance_nacional, name='dashboard_avance'),
    path(
            'vistas/inventario_ceda_diario_campo/',
            views.vista_inventario_ceda_diario,          # función que implementamos antes
            name='vista_inventario_ceda_diario_campo'
        ),
        path(
            'vistas/estadisticas_inventarios_campo/',
            views.vista_estadisticas_inventarios_campo,  # función que implementamos antes
            name='vista_estadisticas_inventarios_campo'
        ),
    path('api/kpi/', views.api_kpi_avance_nacional, name='api_kpi_avance'),
    path(
        'derechohabientes/',                 # ⇦ URL que verá el usuario
        views.vista_derechohabientes,        # ⇦ función que pegamos en Paso 3
        name='vista_derechohabientes'
    ),
    path("api/filtros_kpi/", views.api_filtros_kpi, name="api_filtros_kpi"),
    path('visualizacion/resumen-estatal/', views.resumen_estatal, name='resumen_estatal'),
    path('api/kpi/resumen-por-estado/', views.api_tabla_resumen_por_estado, name='api_resumen_por_estado'),
    path("comentarios_ceda/", comentarios_por_ceda, name="comentarios_por_ceda"),
    path("ajax/zonas_por_filtros/", views.ajax_zonas_por_filtros, name="ajax_zonas_por_filtros"),
    
    path("vistas/fletes/", views.vista_fletes, name="vista_fletes"),

    # APIs AJAX para Fletes
    path("api/fletes/opciones/", views.api_fletes_opciones, name="api_fletes_opciones"),
    path("api/fletes/consultar/", views.api_fletes_consultar, name="api_fletes_consultar"),
    path("api/fletes/exportar_csv/", views.api_fletes_exportar_csv, name="api_fletes_exportar_csv"),
    path("ocr/", views.ocr_page, name="ocr_page"),
    path("ocr/extract/", views.ocr_extract, name="ocr_extract"),

    # URLS sistema 2026
    path('visualizacion/avance-2026/', views.dashboard_avance_nacional_2026, name='dashboard_avance_2026'),
]