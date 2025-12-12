function cargarEstados(url, unidad, selectorEstado, selectorZona) {
  fetch(`${url}?unidad_operativa=${encodeURIComponent(unidad)}`)
    .then(res => res.json())
    .then(data => {
      selectorEstado.innerHTML = '<option value="">-- Todos --</option>';
      data.estados.forEach(e => {
        selectorEstado.innerHTML += `<option value="${e}">${e}</option>`;
      });
      if (selectorZona) {
        selectorZona.innerHTML = '<option value="">-- Todas --</option>';
      }
    });
}

function cargarZonas(url, estado, unidad, selectorZona) {
  fetch(`${url}?estado=${encodeURIComponent(estado)}&unidad_operativa=${encodeURIComponent(unidad)}`)
    .then(res => res.json())
    .then(data => {
      selectorZona.innerHTML = '<option value="">-- Todas --</option>';
      data.zonas.forEach(z => {
        selectorZona.innerHTML += `<option value="${z}">${z}</option>`;
      });
    });
}
