# fertilizantes/forms.py
from django import forms
from .models import ComentarioCEDA

class ComentarioCEDAForm(forms.ModelForm):
    class Meta:
        model = ComentarioCEDA
        fields = ['id_ceda_agricultura', 'comentario']
        widgets = {
            'comentario': forms.Textarea(attrs={'rows': 2, 'placeholder': 'Escribe un comentario...'}),
            'id_ceda_agricultura': forms.HiddenInput()
        }