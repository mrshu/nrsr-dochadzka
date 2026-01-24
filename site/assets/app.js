const MANIFEST_URL = "assets/data/manifest.json";

function $(id) {
  const el = document.getElementById(id);
  if (!el) throw new Error(`Missing element: ${id}`);
  return el;
}

function formatPct(value) {
  if (typeof value !== "number") return "";
  return `${(value * 100).toFixed(2)}%`;
}

function formatInt(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") return value.toLocaleString("sk-SK");
  return String(value);
}

function compareValues(a, b, type) {
  if (type === "number") {
    const av = typeof a === "number" ? a : Number.NEGATIVE_INFINITY;
    const bv = typeof b === "number" ? b : Number.NEGATIVE_INFINITY;
    return av - bv;
  }
  return String(a ?? "").localeCompare(String(b ?? ""), "sk");
}

function bindSortableTable(table, getRows, render) {
  const state = { key: null, dir: "desc", type: "text" };

  function resort(key, type) {
    if (state.key === key) state.dir = state.dir === "asc" ? "desc" : "asc";
    else {
      state.key = key;
      state.type = type || "text";
      state.dir = type === "number" ? "desc" : "asc";
    }
    const rows = [...getRows()];
    rows.sort((ra, rb) => {
      const cmp = compareValues(ra[key], rb[key], state.type);
      return state.dir === "asc" ? cmp : -cmp;
    });
    render(rows);
  }

  table.querySelectorAll("thead th[data-key]").forEach((th) => {
    th.addEventListener("click", () => resort(th.dataset.key, th.dataset.type));
  });

  return { resort };
}

function renderMpTable(rows) {
  const tbody = $("mpTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.mp_name ?? ""}</td>
      <td class="num">${formatPct(r.participation_rate)}</td>
      <td class="num">${formatInt(r.present_count)}</td>
      <td class="num">${formatInt(r.absent_count)}</td>
      <td class="num">${formatInt(r.total_votes)}</td>
    `;
    tbody.appendChild(tr);
  }
}

function renderClubTable(rows) {
  const tbody = $("clubTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.club ?? ""}</td>
      <td class="num">${formatPct(r.participation_rate)}</td>
      <td class="num">${formatInt(r.present_count)}</td>
      <td class="num">${formatInt(r.absent_count)}</td>
      <td class="num">${formatInt(r.total_votes)}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed ${res.status}: ${url}`);
  return res.json();
}

async function loadTerm(termId) {
  const url = `assets/data/term/${termId}/overview.json`;
  const data = await fetchJson(url);
  return { mps: data.mps ?? [], clubs: data.clubs ?? [], generated_at_utc: data.generated_at_utc };
}

async function main() {
  const termSelect = $("termSelect");
  const meta = $("meta");

  const manifest = await fetchJson(MANIFEST_URL);
  const terms = manifest.terms ?? [];
  const defaultTermId = manifest.default_term_id ?? terms[0];

  termSelect.innerHTML = "";
  for (const t of terms) {
    const opt = document.createElement("option");
    opt.value = String(t);
    opt.textContent = String(t);
    termSelect.appendChild(opt);
  }
  termSelect.disabled = false;

  let current = { mps: [], clubs: [] };
  const mpSorter = bindSortableTable($("mpTable"), () => current.mps, renderMpTable);
  const clubSorter = bindSortableTable($("clubTable"), () => current.clubs, renderClubTable);

  async function refresh(termId) {
    current = await loadTerm(termId);
    meta.textContent = `Updated: ${manifest.last_updated_utc ?? "?"}`;
    renderMpTable(current.mps);
    renderClubTable(current.clubs);
    mpSorter.resort("participation_rate", "number");
    clubSorter.resort("participation_rate", "number");
  }

  termSelect.value = String(defaultTermId);
  await refresh(defaultTermId);

  termSelect.addEventListener("change", async () => {
    await refresh(Number(termSelect.value));
  });
}

main().catch((err) => {
  console.error(err);
  $("meta").textContent = `Error: ${err.message ?? String(err)}`;
});

