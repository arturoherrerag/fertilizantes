# ───────── Vista: derechohabientes + contexto operativo ─────────
from django.db import models

class VwDerechohabientesConContexto(models.Model):
    acuse_estatal                    = models.CharField(max_length=50, primary_key=True)
    clave_estado_predio_capturada   = models.CharField(max_length=10, blank=True, null=True)
    estado_predio_capturada         = models.CharField(max_length=100, blank=True, null=True)
    clave_municipio_predio_capturada = models.CharField(max_length=10, blank=True, null=True)
    municipio_predio_capturada      = models.CharField(max_length=100, blank=True, null=True)
    clave_localidad_predio_capturada = models.CharField(max_length=10, blank=True, null=True)
    localidad_predio_capturada      = models.CharField(max_length=100, blank=True, null=True)
    id_nu_solicitud                 = models.CharField(max_length=50, blank=True, null=True)
    cdf_entrega                     = models.CharField(max_length=20, blank=True, null=True)
    id_cdf_entrega                  = models.CharField(max_length=20, blank=True, null=True)
    curp_solicitud                  = models.CharField(max_length=18, blank=True, null=True)
    curp_renapo                     = models.CharField(max_length=18, blank=True, null=True)
    curp_historica                  = models.CharField(max_length=18, blank=True, null=True)
    sn_primer_apellido             = models.CharField(max_length=50, blank=True, null=True)
    sn_segundo_apellido            = models.CharField(max_length=50, blank=True, null=True)
    ln_nombre                       = models.CharField(max_length=100, blank=True, null=True)
    es_pob_indigena                 = models.CharField(max_length=5, blank=True, null=True)
    cultivo                         = models.CharField(max_length=100, blank=True, null=True)
    ton_dap_entregada               = models.FloatField(blank=True, null=True)
    ton_urea_entregada              = models.FloatField(blank=True, null=True)
    fecha_entrega                   = models.DateField(blank=True, null=True)
    folio_persona                   = models.CharField(max_length=30, blank=True, null=True)
    cuadernillo                     = models.CharField(max_length=50, blank=True, null=True)
    nombre_ddr                      = models.CharField(max_length=100, blank=True, null=True)
    clave_ddr                       = models.CharField(max_length=20, blank=True, null=True)
    nombre_cader_ventanilla         = models.CharField(max_length=100, blank=True, null=True)
    clave_cader_ventanilla          = models.CharField(max_length=20, blank=True, null=True)
    dap_anio_actual                 = models.FloatField(blank=True, null=True)
    urea_anio_actual                = models.FloatField(blank=True, null=True)
    dap_remanente                   = models.FloatField(blank=True, null=True)
    urea_remanente                  = models.FloatField(blank=True, null=True)
    superficie_apoyada             = models.FloatField(blank=True, null=True)
    unidad_operativa               = models.CharField(max_length=100, blank=True, null=True)
    estado                         = models.CharField(max_length=100, blank=True, null=True)
    id_ceda_agricultura            = models.CharField(max_length=20, blank=True, null=True)

    class Meta:
        managed = False
        db_table = "vw_derechohabientes_con_contexto"


class ComentarioCEDA(models.Model):
    id_ceda_agricultura = models.CharField(max_length=50)  # 👈 cambio clave
    comentario = models.TextField()
    fecha = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'comentarios_ceda'
        ordering = ['-fecha']