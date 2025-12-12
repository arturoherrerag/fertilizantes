# fertilizantes/templatetags/formatos.py

from django import template
from django.template.defaultfilters import floatformat
from django.contrib.humanize.templatetags.humanize import intcomma

register = template.Library()

@register.filter
def formato_mx(value, decimales=0):
    """
    Aplica formato regional de México: separador de miles con coma y punto como decimal.
    Ejemplos:
        {{ valor|formato_mx }} → 1,000
        {{ valor|formato_mx:2 }} → 1,000.25
    """
    try:
        valor = float(value)
    except (ValueError, TypeError):
        return value
    return intcomma(floatformat(valor, decimales))
