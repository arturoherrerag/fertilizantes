document.addEventListener("DOMContentLoaded", () => {
  // 1. Paleta Institucional Global
  window.COLORES = {
    abasto: "#004634",     // Verde Oscuro
    entregado: "#146B4D",  // Verde Medio
    dh: "#6A1B3F",         // Guinda
    superficie: "#A1760E", // Dorado
    gris: "#e0e0e0"        // Gris claro (fondo de donas/barras)
  };

  // 2. Detectar si estamos en el Dashboard Nacional
  const kpiContainer = document.getElementById("kpi-cards");
  if (kpiContainer) {
    console.log("📊 Iniciando Dashboard Nacional...");
    cargarKpi();
    cargarFiltros("select"); 

    const form = document.getElementById("filtros-form");
    if (form) {
      form.addEventListener("submit", (e) => {
        e.preventDefault();
        const params = new FormData(form);
        cargarKpi(Object.fromEntries(params));
      });
    }
  }

  // 3. Detectar si estamos en el Resumen Estatal
  const tablaEstados = document.getElementById("tabla_estados");
  if (tablaEstados) {
    console.log("📍 Iniciando Resumen Estatal...");
    cargarResumen();
    cargarFiltros("datalist");

    // Listeners para recarga automática al cambiar filtros
    ["filtro_unidad", "filtro_estado", "filtro_tipo_meta"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener("change", cargarResumen);
    });
  }
});

// ==========================================
//  SECCIÓN 1: DASHBOARD NACIONAL (KPIs)
// ==========================================

function cargarKpi(params = {}) {
  const container = document.getElementById("kpi-cards");
  // Loader simple mientras carga
  container.innerHTML = '<div class="col-12 text-center py-5"><div class="spinner-border text-secondary" role="status"></div></div>';

  const q = new URLSearchParams(params).toString();
  
  fetch(`/api/kpi/?${q}`)
    .then(r => r.json())
    .then(data => {
      if (data.error) throw new Error(data.error);
      const cards = transformarDatosKPI(data);
      pintarTarjetas(cards);
    })
    .catch(err => {
      console.error("Error KPI:", err);
      container.innerHTML = `<div class="alert alert-danger">Error al cargar datos: ${err.message}</div>`;
    });
}

function transformarDatosKPI(d) {
  return [
    {
      id: "abasto",
      titulo: "Abasto Recibido (t)",
      meta: d.meta_total_ton,
      avance: d.abasto_recibido,
      es_entero: false
    },
    {
      id: "entregado",
      titulo: "Fertilizante Entregado (t)",
      meta: d.meta_total_ton,
      avance: d.entregado,
      es_entero: false
    },
    {
      id: "dh",
      titulo: "Derechohabientes",
      meta: d.meta_dh,
      avance: d.derechohabientes_apoyados,
      es_entero: true
    },
    {
      id: "superficie",
      titulo: "Superficie (ha)",
      meta: d.meta_ha,
      avance: d.superficie_beneficiada,
      es_entero: true
    }
  ];
}

function pintarTarjetas(cards) {
  const cont = document.getElementById("kpi-cards");
  cont.innerHTML = "";

  cards.forEach(c => {
    const meta = parseFloat(c.meta) || 0;
    const avance = parseFloat(c.avance) || 0;
    const pendiente = Math.max(meta - avance, 0);
    
    // Calcular porcentaje (tope visual 100%)
    const pct = meta > 0 ? (avance / meta) * 100 : 0;
    
    // Formateador de números
    const fmt = new Intl.NumberFormat('en-US', {
      minimumFractionDigits: c.es_entero ? 0 : 2,
      maximumFractionDigits: c.es_entero ? 0 : 2
    });

    const colorTema = window.COLORES[c.id];

    // HTML de la tarjeta moderna
    const html = `
      <div class="col-12 col-md-6 col-lg-3 mb-4">
        <div class="card-kpi h-100">
          
          <!-- Encabezado sólido -->
          <div class="kpi-header" style="background-color: ${colorTema};">
            ${c.titulo}
          </div>
          
          <div class="kpi-body">
            <div class="kpi-value">${fmt.format(avance)}</div>
            
            <div class="kpi-meta">
              Meta ${fmt.format(meta)}
            </div>
            
            <div class="kpi-pendiente">
              Pendiente ${fmt.format(pendiente)}
            </div>
            
            <div class="chart-container">
              <canvas id="chart-${c.id}"></canvas>
              <div class="donut-percent" style="color: ${colorTema};">
                ${pct.toFixed(0)}%
              </div>
            </div>

          </div>
        </div>
      </div>
    `;
    cont.insertAdjacentHTML("beforeend", html);

    renderDonut(`chart-${c.id}`, avance, pendiente, c.id);
  });
}

function renderDonut(canvasId, avance, pendiente, tipo) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;

  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: ["Avance", "Pendiente"],
      datasets: [{
        data: [avance, pendiente],
        backgroundColor: [window.COLORES[tipo], window.COLORES.gris],
        borderWidth: 0,
        hoverOffset: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "65%", // Dona gruesa
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false }
      }
    }
  });
}

// ==========================================
//  SECCIÓN 2: RESUMEN ESTATAL (Tabla)
// ==========================================

function cargarResumen() {
  const tbody = document.querySelector("#tabla_estados tbody");
  if (!tbody) return;

  const u = document.getElementById("filtro_unidad")?.value || "";
  const e = document.getElementById("filtro_estado")?.value || "";
  const tm = document.getElementById("filtro_tipo_meta")?.value || "operativa";
  
  const params = new URLSearchParams({ unidad_operativa: u, estado: e, tipo_meta: tm });
  
  fetch(`/api/kpi/resumen-por-estado/?${params}`)
    .then(r => r.json())
    .then(resp => {
      const data = resp.resultados || [];
      tbody.innerHTML = "";

      if (data.length === 0) {
        tbody.innerHTML = `<tr><td colspan="17" class="text-center py-4 text-muted">No hay datos disponibles con los filtros seleccionados.</td></tr>`;
        return;
      }

      // Ordenar por porcentaje de entrega descendente
      data.sort((a, b) => b.pct_entregado - a.pct_entregado);

      // Variables para totales
      let tMetaTon=0, tAbasto=0, tEnt=0, tMetaDh=0, tDh=0, tMetaHa=0, tHa=0;

      data.forEach(r => {
        // Acumular totales
        tMetaTon += r.meta_total_ton; 
        tAbasto += r.abasto; 
        tEnt += r.entregado;
        tMetaDh += r.meta_dh; 
        tDh += r.dh_apoyados;
        tMetaHa += r.meta_ha; 
        tHa += r.ha_apoyadas;

        // Insertar fila
        tbody.insertAdjacentHTML("beforeend", `
          <tr>
            <td class="col-estado ps-2">${r.estado}</td>
            ${colTabla(r.meta_total_ton, r.abasto, "abasto")}
            ${colTabla(r.meta_total_ton, r.entregado, "entregado")}
            ${colTabla(r.meta_dh, r.dh_apoyados, "dh", true)}
            ${colTabla(r.meta_ha, r.ha_apoyadas, "superficie", true)}
          </tr>
        `);
      });

      // Insertar fila de TOTALES
      tbody.insertAdjacentHTML("beforeend", `
        <tr class="row-total">
          <td class="ps-2">TOTAL NACIONAL</td>
          ${colTabla(tMetaTon, tAbasto, "abasto", false, true)}
          ${colTabla(tMetaTon, tEnt, "entregado", false, true)}
          ${colTabla(tMetaDh, tDh, "dh", true, true)}
          ${colTabla(tMetaHa, tHa, "superficie", true, true)}
        </tr>
      `);
    });
}

/**
 * Genera el HTML de las 4 columnas (Meta, Avance, Pendiente, %) para un tipo de dato.
 * @param {number} meta - Valor meta
 * @param {number} avance - Valor avance
 * @param {string} tipo - Clave del color ('abasto', 'entregado', 'dh', 'superficie')
 * @param {boolean} esEntero - Si true, formatea sin decimales
 * @param {boolean} esTotal - Si true, aplica estilos de fila de totales (texto blanco)
 */
function colTabla(meta, avance, tipo, esEntero = false, esTotal = false) {
  meta = parseFloat(meta) || 0;
  avance = parseFloat(avance) || 0;
  const pend = Math.max(meta - avance, 0);
  
  let pct = 0;
  if (meta > 0) {
    pct = (avance / meta) * 100;
  }
  const widthPct = Math.min(pct, 100); 
  
  const fmt = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: esEntero ? 0 : 1,
    maximumFractionDigits: esEntero ? 0 : 1
  });

  // Obtener color HEX exacto para inyectar en el estilo (evita override de Bootstrap)
  const colorHex = window.COLORES[tipo] || "#000";

  // Estilos condicionales para texto
  const classMeta = esTotal ? "text-white opacity-75" : "text-muted";
  const classAvance = esTotal ? "text-white" : "text-dark fw-bold";
  const classPend = esTotal ? "text-white opacity-75" : "text-danger small";
  
  // Si la barra es muy pequeña, oscurecemos el texto del porcentaje para que se lea
  const classPctText = widthPct < 20 ? "text-dark-shadow" : "";

  return `
    <td class="text-numero ${classMeta}">${fmt.format(meta)}</td>
    <td class="text-numero ${classAvance}">${fmt.format(avance)}</td>
    <td class="text-numero ${classPend}">${fmt.format(pend)}</td>
    
    <td class="td-porcentaje align-middle">
      <div class="pf-progress-container">
        <div class="pf-progress-bar" 
             style="width: ${widthPct}%; background-color: ${colorHex} !important;">
        </div>
        <span class="pf-progress-text ${classPctText}">
          ${pct.toFixed(0)}%
        </span>
      </div>
    </td>
  `;
}

// ==========================================
//  SECCIÓN 3: FILTROS DINÁMICOS
// ==========================================

function cargarFiltros(modo) {
  fetch("/api/filtros_kpi/")
    .then(r => r.json())
    .then(data => {
      if (modo === "select") {
        llenarSelect("unidad_operativa", data.unidades);
        llenarSelect("estado", data.estados);
      } else {
        llenarDatalist("coord", data.unidades);
        llenarDatalist("estados", data.estados);
      }
    });
}

function llenarSelect(id, items) {
  const el = document.getElementById(id);
  if(el) {
    el.innerHTML = '<option value="">-- Todas --</option>';
    items.forEach(i => el.add(new Option(i, i)));
  }
}

function llenarDatalist(id, items) {
  const el = document.getElementById(id);
  if(el) {
    el.innerHTML = "";
    items.forEach(i => {
      const op = document.createElement("option");
      op.value = i;
      el.appendChild(op);
    });
  }
}