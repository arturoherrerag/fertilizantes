from django import template

register = template.Library()

@register.filter(name="diff_cat")
def diff_categoria(valor):
    """
    Convierte la diferencia numérica en intervalo textual.
    • negativos  : "< -1 ton" , "< -10 ton" , "> -10 ton"
    • positivos  : "< 1 ton"  , "< 10 ton"  , "> 10 ton"
    • cero / None: "0.000"
    """
    if valor is None:
        return "—"

    try:
        v = float(valor)
    except (ValueError, TypeError):
        return valor

    if v < 0:                                # NEGATIVOS
        if v > -1:
            return "< -1 ton"
        elif v > -10:
            return "< -10 ton"
        else:
            return "> -10 ton"
    elif v > 0:                              # POSITIVOS
        if v < 1:
            return "< 1 ton"
        elif v < 10:
            return "< 10 ton"
        else:
            return "> 10 ton"
    else:                                    # exactamente 0
        return "0.000"