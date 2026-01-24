import { $, fetchJson, formatInt, formatPct, setMeta } from "./common.js";

function renderVotes(rows, onClick) {
  const tbody = $("votesTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.style.cursor = "pointer";
    tr.innerHTML = `
      <td>${r.vote_datetime_local ?? ""}</td>
      <td>${r.title ?? ""}</td>
      <td class="num">${formatInt(r.meeting_nr)}</td>
      <td class="num">${formatInt(r.vote_number)}</td>
    `;
    tr.addEventListener("click", () => onClick(r.vote_id));
    tbody.appendChild(tr);
  }
}

function renderClubs(rows) {
  const tbody = $("clubsTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.club ?? ""}</td>
      <td class="num">${formatPct(r.presence_rate)}</td>
      <td class="num">${formatInt(r.present)}</td>
      <td class="num">${formatInt(r.absent)}</td>
      <td class="num">${formatInt(r.total)}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function loadVotes(termId) {
  return fetchJson(`term/${termId}/votes.json`);
}

async function loadVote(termId, voteId) {
  return fetchJson(`term/${termId}/vote/${voteId}.json`);
}

async function main() {
  const termSelect = $("termSelect");
  const searchInput = $("searchInput");
  const voteMeta = $("voteMeta");
  const detailWrap = $("detailWrap");

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
  setMeta(`Updated: ${manifest.last_updated_utc ?? "?"}`);

  let allVotes = [];
  function applyFilter() {
    const q = (searchInput.value ?? "").trim().toLowerCase();
    const filtered = q
      ? allVotes.filter((v) => String(v.title ?? "").toLowerCase().includes(q))
      : allVotes;
    renderVotes(filtered.slice(0, 300), async (voteId) => {
      const termId = Number(termSelect.value);
      const data = await loadVote(termId, voteId);
      const title = data.vote?.title ?? data.vote?.vote_id ?? voteId;
      voteMeta.textContent = String(title);
      detailWrap.hidden = false;
      renderClubs(data.clubs ?? []);
    });
  }

  async function refresh(termId) {
    allVotes = await loadVotes(termId);
    voteMeta.textContent = "Kliknite na hlasovanie v tabuľke.";
    detailWrap.hidden = true;
    applyFilter();
  }

  termSelect.value = String(defaultTermId);
  await refresh(defaultTermId);

  termSelect.addEventListener("change", async () => {
    await refresh(Number(termSelect.value));
  });
  searchInput.addEventListener("input", () => applyFilter());
}

main().catch((err) => {
  console.error(err);
  setMeta(`Error: ${err.message ?? String(err)}`);
});

