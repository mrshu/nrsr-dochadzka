import { $, fetchJson, formatInt, formatPct, setMeta } from "./common.js";

function setCards(summary) {
  const cards = $("cards");
  cards.innerHTML = "";
  const items = [
    { label: "Dochádzka", value: formatPct(summary.participation_rate) },
    { label: "Prítomný", value: formatInt(summary.present_count) },
    { label: "Neprítomný", value: formatInt(summary.absent_count) },
    { label: "Hlasoval", value: formatInt(summary.voted_count) },
    { label: "Spolu", value: formatInt(summary.total_votes) },
  ];
  for (const it of items) {
    const div = document.createElement("div");
    div.className = "card";
    div.innerHTML = `<div class="label">${it.label}</div><div class="value">${it.value}</div>`;
    cards.appendChild(div);
  }
}

function renderClubRows(rows) {
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

function renderRecentVotes(rows) {
  const tbody = $("votesTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.vote_datetime_local ?? ""}</td>
      <td>${r.title ?? ""}</td>
      <td class="num">${r.vote_code ?? ""}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function loadOverview(termId) {
  return fetchJson(`term/${termId}/overview.json`);
}

async function loadMp(termId, mpId) {
  return fetchJson(`term/${termId}/mp/${mpId}.json`);
}

async function main() {
  const termSelect = $("termSelect");
  const mpSelect = $("mpSelect");
  const mpTitle = $("mpTitle");

  const manifest = await fetchJson("manifest.json");
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

  let overview = null;
  async function refreshOverview(termId) {
    overview = await loadOverview(termId);
    mpSelect.innerHTML = "";
    for (const mp of overview.mps ?? []) {
      const opt = document.createElement("option");
      opt.value = String(mp.mp_id);
      opt.textContent = mp.mp_name ?? String(mp.mp_id);
      mpSelect.appendChild(opt);
    }
    mpSelect.disabled = false;
    setMeta(`Updated: ${manifest.last_updated_utc ?? "?"}`);
  }

  async function refreshMp(termId, mpId) {
    const data = await loadMp(termId, mpId);
    mpTitle.textContent = data.mp_name ?? "Profil";
    setCards(data.summary ?? {});
    renderClubRows(data.clubs_at_vote_time ?? []);
    renderRecentVotes(data.recent_votes ?? []);
  }

  termSelect.value = String(defaultTermId);
  await refreshOverview(defaultTermId);

  const firstMpId = Number(mpSelect.value);
  if (!Number.isNaN(firstMpId)) {
    await refreshMp(defaultTermId, firstMpId);
  } else {
    mpTitle.textContent = "No MP data (missing mp pages in bundle)";
  }

  termSelect.addEventListener("change", async () => {
    const termId = Number(termSelect.value);
    await refreshOverview(termId);
    const mpId = Number(mpSelect.value);
    if (!Number.isNaN(mpId)) await refreshMp(termId, mpId);
  });

  mpSelect.addEventListener("change", async () => {
    const termId = Number(termSelect.value);
    const mpId = Number(mpSelect.value);
    if (!Number.isNaN(mpId)) await refreshMp(termId, mpId);
  });
}

main().catch((err) => {
  console.error(err);
  setMeta(`Error: ${err.message ?? String(err)}`);
});

