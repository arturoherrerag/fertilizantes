#!/usr/bin/env python

from django import template
register = template.Library()

@register.filter
def get_item(value, key):
    """
    Uso en plantilla:  {{ fila|get_item:"campo" }}
    Devuelve fila['campo'] o cadena vacía si no existe.
    """
    if isinstance(value, dict):
        return value.get(key, "")
    return ""
