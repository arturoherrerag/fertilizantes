document.addEventListener("DOMContentLoaded", () => {
  // Paleta Institucional (Basada en tus archivos originales)
  window.COLORES = {
    abasto: "#004634",     // Verde Oscuro
    entregado: "#146B4D",  // Verde Medio
    dh: "#6A1B3F",         // Vino
    superficie: "#A1760E", // Dorado
    gris: "#e0e0e0"        // Gris para el fondo de la dona
  };

  // 1. Detectar Dashboard Nacional
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

  // 2. Detectar Resumen Estatal
  const tablaEstados = document.getElementById("tabla_estados");
  if (tablaEstados) {
    console.log("📍 Iniciando Resumen Estatal...");
    cargarResumen();
    cargarFiltros("datalist");

    // Listeners
    ["filtro_unidad", "filtro_estado", "filtro_tipo_meta"].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener("change", cargarResumen);
    });
  }
});

// ==========================================
//  DASHBOARD NACIONAL (KPIs)
// ==========================================

function cargarKpi(params = {}) {
  const container = document.getElementById("kpi-cards");
  // Loader simple
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
      container.innerHTML = `<div class="alert alert-danger">Error al cargar datos.</div>`;
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
    // Calcular porcentaje (tope 100 visualmente para la gráfica)
    const pct = meta > 0 ? (avance / meta) * 100 : 0;
    const pctVisual = Math.min(100, pct);
    
    // Formateador de números (miles y decimales)
    const fmt = new Intl.NumberFormat('en-US', {
      minimumFractionDigits: c.es_entero ? 0 : 2,
      maximumFractionDigits: c.es_entero ? 0 : 2
    });

    const html = `
      <div class="col-12 col-md-6 col-lg-3 mb-4">
        <div class="card shadow-sm h-100 border-0 overflow-hidden">
          <div class="card-header text-white fw-bold text-center py-2" 
               style="background-color: ${window.COLORES[c.id]}; font-size: 1.1rem;">
            ${c.titulo}
          </div>
          
          <div class="card-body text-center position-relative">
            <h2 class="display-6 fw-bold mb-0 text-dark">${fmt.format(avance)}</h2>
            
            <div class="small text-muted mb-3">
              Meta: ${fmt.format(meta)} <span class="mx-1">|</span> Pendiente: ${fmt.format(pendiente)}
            </div>
            
            <div style="height: 160px; position: relative;">
              <canvas id="chart-${c.id}"></canvas>
              
              <div class="position-absolute top-50 start-50 translate-middle fw-bold" 
                   style="color: ${window.COLORES[c.id]}; font-size: 1.5rem;">
                ${pct.toFixed(1)}%
              </div>
            </div>

          </div>
        </div>
      </div>
    `;
    cont.insertAdjacentHTML("beforeend", html);

    // Dibujar la gráfica
    renderDonut(`chart-${c.id}`, avance, pendiente, c.id, fmt);
  });
}

function renderDonut(canvasId, avance, pendiente, tipo, formatter) {
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
      // 🔥 AQUÍ ESTÁ EL AJUSTE DE GROSOR:
      // 50% = Dona gruesa. (Antes estaba en 75% o 60%)
      cutout: "50%", 
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: 'rgba(0, 0, 0, 0.8)',
          callbacks: {
            label: function(context) {
              let label = context.label || '';
              if (label) label += ': ';
              if (context.parsed !== null) {
                label += formatter.format(context.parsed);
              }
              return label;
            }
          }
        }
      }
    }
  });
}

// ==========================================
//  RESUMEN ESTATAL (Tabla)
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
        tbody.innerHTML = `<tr><td colspan="17" class="text-center py-4 text-muted">No hay datos.</td></tr>`;
        return;
      }

      data.sort((a, b) => b.pct_entregado - a.pct_entregado);

      let tMetaTon=0, tAbasto=0, tEnt=0, tMetaDh=0, tDh=0, tMetaHa=0, tHa=0;

      data.forEach(r => {
        tMetaTon += r.meta_total_ton; tAbasto += r.abasto; tEnt += r.entregado;
        tMetaDh += r.meta_dh; tDh += r.dh_apoyados;
        tMetaHa += r.meta_ha; tHa += r.ha_apoyadas;

        tbody.insertAdjacentHTML("beforeend", `
          <tr>
            <td class="estado bg-light fw-bold text-start ps-3">${r.estado}</td>
            ${colTabla(r.meta_total_ton, r.abasto, "abasto")}
            ${colTabla(r.meta_total_ton, r.entregado, "entregado")}
            ${colTabla(r.meta_dh, r.dh_apoyados, "dh", true)}
            ${colTabla(r.meta_ha, r.ha_apoyadas, "superficie", true)}
          </tr>
        `);
      });

      tbody.insertAdjacentHTML("beforeend", `
        <tr class="fw-bold table-secondary border-top border-3 border-dark">
          <td class="text-start ps-3">TOTAL NACIONAL</td>
          ${colTabla(tMetaTon, tAbasto, "abasto")}
          ${colTabla(tMetaTon, tEnt, "entregado")}
          ${colTabla(tMetaDh, tDh, "dh", true)}
          ${colTabla(tMetaHa, tHa, "superficie", true)}
        </tr>
      `);

      setTimeout(() => {
        document.querySelectorAll(".progress-bar").forEach(b => b.style.width = b.dataset.width);
      }, 50);
    });
}

function colTabla(meta, avance, tipo, esEntero = false) {
  meta = parseFloat(meta) || 0;
  avance = parseFloat(avance) || 0;
  const pend = Math.max(meta - avance, 0);
  const pct = meta > 0 ? Math.min(100, (avance / meta) * 100) : 0;
  
  const fmt = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: esEntero ? 0 : 1,
    maximumFractionDigits: esEntero ? 0 : 1
  });

  return `
    <td class="meta text-muted small">${fmt.format(meta)}</td>
    <td class="avance fw-bold">${fmt.format(avance)}</td>
    <td class="pendiente text-muted small">${fmt.format(pend)}</td>
    <td class="col-porcentaje p-1 align-middle">
      <div class="progress position-relative" style="height: 18px; background-color: #e9ecef;">
        <div class="progress-bar ${tipo}" data-width="${pct}%" style="width: 0%;"></div>
        <span class="position-absolute w-100 text-center text-dark" 
              style="font-size: 0.7rem; top: 1px; font-weight: bold; text-shadow: 0 0 2px white;">
          ${pct.toFixed(1)}%
        </span>
      </div>
    </td>
  `;
}

// ==========================================
//  FILTROS
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