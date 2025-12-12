document.addEventListener("DOMContentLoaded", () => {
  // === Tarjetas KPI nacionales ===
  const tarjetas = document.getElementById("kpi-cards");
  const formKPI = document.getElementById("filtros-form");

  if (tarjetas && formKPI) {
    console.log("📊 Cargando tarjetas KPI...");
    cargarKpi();
    cargarFiltros();

    formKPI.addEventListener("submit", e => {
      e.preventDefault();
      const params = Object.fromEntries(new FormData(formKPI));
      cargarKpi(params);
    });
  }

  // === Tabla resumen por estado ===
  const tabla = document.getElementById("tabla_estados");
  if (tabla) {
    console.log("📊 Iniciando carga de resumen por estado...");
    cargarResumenPorEstado();

    const unidad = document.getElementById("filtro_unidad");
    const estado = document.getElementById("filtro_estado");
    const tipoMeta = document.getElementById("filtro_tipo_meta");

    if (unidad) unidad.addEventListener("input", cargarResumenPorEstado);
    if (estado) estado.addEventListener("input", cargarResumenPorEstado);
    if (tipoMeta) tipoMeta.addEventListener("change", cargarResumenPorEstado);
  }
});

// ====== Tarjetas KPI ======
function cargarKpi(params = {}) {
  fetch(`/api/kpi/?${new URLSearchParams(params)}`)
    .then(r => r.json())
    .then(data => pintarTarjetas(data));
}

function pintarTarjetas(cards) {
  const cont = document.getElementById("kpi-cards");
  if (!cont) return;

  cont.innerHTML = "";

  cards.forEach(c => {
    c.pct = parseFloat(c.pct);  // ✅ Asegurar número

    cont.insertAdjacentHTML("beforeend", `
      <div class="col-6 col-lg-3 mb-4">
        <div class="card shadow-sm text-center border-top-0 rounded-3 overflow-hidden">
          <div class="kpi-header bg-${c.id}">${c.titulo}</div>
          <div class="card-body">
            <h2 class="display-6">${c.avance_fmt}</h2>
            <div class="kpi-meta">Meta ${c.meta_fmt}</div>
            <div class="kpi-pendiente">Pendiente ${c.pendiente_fmt}</div>
            <div class="position-relative">
              <canvas id="donut-${c.id}" height="130"></canvas>
              <div class="donut-label fw-bold" style="color:${colorPorId(c.id)}">${c.pct.toFixed(0)}%</div>
            </div>
          </div>
        </div>
      </div>
    `);
    dibujarDonut(`donut-${c.id}`, c.pct, c.id);
  });
}

function dibujarDonut(id, porcentaje, tipo) {
  const ctx = document.getElementById(id);
  if (!ctx) return;

  new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: ["Avance", "Pendiente"],
      datasets: [{
        data: [porcentaje, 100 - porcentaje],
        backgroundColor: [colorPorId(tipo), "#e0e0e0"],
        borderWidth: 0
      }]
    },
    options: {
      cutout: "60%",
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false }
      }
    }
  });
}

function colorPorId(id) {
  const colores = {
    abasto: "#004634",     // Verde oscuro
    entregado: "#146B4D",  // Verde medio
    dh: "#6A1B3F",         // Vino
    superficie: "#A1760E"  // Oro oscuro
  };
  return colores[id] || "#999";
}

// ====== Filtros KPI ======
function cargarFiltros() {
  fetch("/api/filtros_kpi/")
    .then(r => r.json())
    .then(data => {
      const unidadSel = document.getElementById("unidad_operativa");
      const estadoSel = document.getElementById("estado");

      if (unidadSel && estadoSel) {
        data.unidades.forEach(u => {
          const opt = document.createElement("option");
          opt.value = u;
          opt.textContent = u;
          unidadSel.appendChild(opt);
        });

        data.estados.forEach(e => {
          const opt = document.createElement("option");
          opt.value = e;
          opt.textContent = e;
          estadoSel.appendChild(opt);
        });
      }
    });
}

// ====== Tabla Resumen por Estado ======
function cargarResumenPorEstado() {
  const unidad = document.getElementById("filtro_unidad")?.value.trim() || "";
  const estado = document.getElementById("filtro_estado")?.value.trim() || "";
  const tipo_meta = document.getElementById("filtro_tipo_meta")?.value || "operativa";

  const params = new URLSearchParams();
  if (unidad) params.append("unidad_operativa", unidad);
  if (estado) params.append("estado", estado);
  params.append("tipo_meta", tipo_meta);

  console.log("📤 Enviando solicitud con filtros:", { unidad, estado, tipo_meta });

  fetch(`/api/kpi/resumen-por-estado/?${params.toString()}`)
    .then(response => response.json())
    .then(data => {
      const tbody = document.querySelector("#tabla_estados tbody");
      if (!tbody) return;

      tbody.innerHTML = "";

      // 🔽 Ordenar por % de derechohabientes apoyados en orden descendente
      data.sort((a, b) => {
        const pctA = a.meta_dh ? a.dh_apoyados / a.meta_dh : 0;
        const pctB = b.meta_dh ? b.dh_apoyados / b.meta_dh : 0;
        return pctB - pctA;
      });

      // 📊 Acumulador de totales nacionales
      const total = {
        estado: "TOTAL",
        meta_total_ton: 0,
        abasto: 0,
        entregado: 0,
        meta_dh: 0,
        dh_apoyados: 0,
        meta_ha: 0,
        ha_apoyadas: 0
      };

      // 🖊️ Pintar la tabla ordenada y acumular totales
      data.forEach(r => {
        total.meta_total_ton += parseFloat(r.meta_total_ton || 0);
        total.abasto += parseFloat(r.abasto || 0);
        total.entregado += parseFloat(r.entregado || 0);
        total.meta_dh += parseFloat(r.meta_dh || 0);
        total.dh_apoyados += parseFloat(r.dh_apoyados || 0);
        total.meta_ha += parseFloat(r.meta_ha || 0);
        total.ha_apoyadas += parseFloat(r.ha_apoyadas || 0);

        tbody.insertAdjacentHTML("beforeend", `
          <tr>
            <td class="estado">${r.estado}</td>
            ${colKPI(r.meta_total_ton, r.abasto, "abasto")}
            ${colKPI(r.meta_total_ton, r.entregado, "entregado")}
            ${colKPI(r.meta_dh, r.dh_apoyados, "dh")}
            ${colKPI(r.meta_ha, r.ha_apoyadas, "superficie")}
          </tr>
        `);
      });

      // ➕ Fila total nacional al final
      tbody.insertAdjacentHTML("beforeend", `
        <tr class="fw-bold">
          <td class="estado bg-secondary text-white">TOTAL NACIONAL</td>
          ${colKPI(total.meta_total_ton, total.abasto, "abasto", true)}
          ${colKPI(total.meta_total_ton, total.entregado, "entregado", true)}
          ${colKPI(total.meta_dh, total.dh_apoyados, "dh", true)}
          ${colKPI(total.meta_ha, total.ha_apoyadas, "superficie", true)}
        </tr>
      `);

      // 🟢 Activar animación progresiva de barras
      setTimeout(() => {
        document.querySelectorAll("#tabla_estados .progress-bar").forEach(bar => {
          const pct = bar.dataset.pct;
          bar.style.width = `${pct}%`;
        });
      }, 10);

      console.log("📥 Datos recibidos y ordenados:", data);
    })
    .catch(error => {
      console.error("Error cargando resumen por estado:", error);
    });
}


function colKPI(meta, avance, tipo, esTotal = false) {
  const numMeta = parseFloat(meta) || 0;
  const numAvance = parseFloat(avance) || 0;
  const pendiente = Math.max(numMeta - numAvance, 0);
  const pct = numMeta ? Math.min(100, Math.floor(numAvance * 100 / numMeta)) : 0;
  const color = colorPorId(tipo);

  const formatoNumerico = (n) => {
    const num = parseFloat(n) || 0;
    if (tipo === "abasto" || tipo === "entregado") {
      return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    } else {
      return num.toLocaleString('en-US'); // sin decimales
    }
  };

  const fondo = esTotal ? `bg-${tipo} text-white` : "";

  return `
    <td class="meta ${fondo}">${formatoNumerico(numMeta)}</td>
    <td class="avance ${fondo}">${formatoNumerico(numAvance)}</td>
    <td class="pendiente ${fondo}">${formatoNumerico(pendiente)}</td>
    <td class="col-porcentaje ${fondo}">
      <div class="progress">
        <div class="progress-bar ${tipo}" data-pct="${pct}" style="width: 0%">
          ${pct}%
        </div>
      </div>
    </td>
  `;
}





