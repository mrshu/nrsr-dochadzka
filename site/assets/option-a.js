import { $, fetchJson, formatInt, formatPct, setMeta } from "./common.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function percentile(sortedValues, p) {
  if (!sortedValues.length) return null;
  const idx = (sortedValues.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedValues[lo];
  const w = idx - lo;
  return sortedValues[lo] * (1 - w) + sortedValues[hi] * w;
}

function safeNumber(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : null;
}

function hashToHue(value) {
  const s = String(value ?? "");
  let h = 2166136261;
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return Math.abs(h) % 360;
}

function renderKpis(mps) {
  const kpis = $("kpis");
  const rates = mps
    .map((m) => safeNumber(m.participation_rate))
    .filter((v) => typeof v === "number")
    .sort((a, b) => a - b);
  const p50 = percentile(rates, 0.5);
  const best = rates.length ? rates[rates.length - 1] : null;
  const worst = rates.length ? rates[0] : null;
  const items = [
    { label: "Poslanci", value: formatInt(mps.length) },
    { label: "Medián účasti", value: p50 === null ? "—" : formatPct(p50) },
    { label: "Najlepšia", value: best === null ? "—" : formatPct(best) },
    { label: "Najhoršia", value: worst === null ? "—" : formatPct(worst) },
  ];
  kpis.innerHTML = "";
  for (const it of items) {
    const div = document.createElement("div");
    div.className = "kpi";
    div.innerHTML = `<div class="kpi-label">${escapeHtml(it.label)}</div><div class="kpi-value">${escapeHtml(it.value)}</div>`;
    kpis.appendChild(div);
  }
}

function normalizeClubKey(mp) {
  return mp?.club_key ?? (mp?.club ? String(mp.club).toLowerCase() : "unknown");
}

function buildClubOptions({ clubs, mps }) {
  const seen = new Map();
  for (const c of clubs ?? []) {
    const key = c.club_key ?? "unknown";
    const label = c.club ?? key;
    seen.set(String(key), String(label));
  }
  for (const m of mps ?? []) {
    const key = normalizeClubKey(m);
    const label = m.club ?? key;
    if (!seen.has(String(key))) seen.set(String(key), String(label));
  }
  const out = [...seen.entries()].map(([key, label]) => ({ key, label }));
  out.sort((a, b) => a.label.localeCompare(b.label, "sk"));
  return out;
}

function renderClubBars({ clubs, selectedClubKey, onPick }) {
  const el = $("clubBars");
  const rows = [...(clubs ?? [])].sort(
    (a, b) => (safeNumber(b.participation_rate) ?? -1) - (safeNumber(a.participation_rate) ?? -1),
  );
  el.innerHTML = rows
    .map((c) => {
      const key = String(c.club_key ?? "unknown");
      const rate = safeNumber(c.participation_rate);
      const hue = hashToHue(key);
      const active = selectedClubKey === key;
      return `
        <button class="club-bar-row ${active ? "active" : ""}" type="button" data-club="${escapeHtml(key)}">
          <div class="club-left">
            <span class="dot" style="--h:${hue}"></span>
            <span class="club-name">${escapeHtml(c.club ?? key)}</span>
          </div>
          <div class="club-mid">
            <div class="bar"><span style="--h:${hue}; width:${rate === null ? 0 : Math.max(0, Math.min(100, rate * 100))}%"></span></div>
          </div>
          <div class="club-right">${rate === null ? "—" : escapeHtml(formatPct(rate))}</div>
        </button>
      `;
    })
    .join("");
  el.querySelectorAll("button.club-bar-row").forEach((btn) => {
    btn.addEventListener("click", () => onPick(btn.dataset.club));
  });
}

function rankRows(mps, { clubKey, query }) {
  const q = (query ?? "").trim().toLowerCase();
  return (mps ?? [])
    .filter((m) => (clubKey ? String(normalizeClubKey(m)) === String(clubKey) : true))
    .filter((m) => (q ? String(m.mp_name ?? "").toLowerCase().includes(q) : true))
    .filter((m) => typeof m.participation_rate === "number")
    .slice();
}

function renderRankList({ elId, rows, title, onPick }) {
  const el = $(elId);
  el.innerHTML = rows
    .map((m, idx) => {
      const rate = safeNumber(m.participation_rate);
      const clubKey = normalizeClubKey(m);
      const hue = hashToHue(clubKey);
      return `
        <button class="rank-row" type="button" data-mp="${escapeHtml(String(m.mp_id))}">
          <div class="rank">${escapeHtml(String(idx + 1).padStart(2, "0"))}</div>
          <div class="who">
            <div class="name">${escapeHtml(m.mp_name ?? "")}</div>
            <div class="sub">
              <span class="badge" style="--h:${hue}">${escapeHtml(m.club ?? "(unknown)")}</span>
              <span class="muted">abs: ${escapeHtml(formatInt(m.absent_count))}</span>
              <span class="muted">n: ${escapeHtml(formatInt(m.total_votes))}</span>
            </div>
          </div>
          <div class="score">
            <div class="pct">${rate === null ? "—" : escapeHtml(formatPct(rate))}</div>
            <div class="mini"><span style="--h:${hue}; width:${rate === null ? 0 : Math.max(0, Math.min(100, rate * 100))}%"></span></div>
          </div>
        </button>
      `;
    })
    .join("");
  if (!rows.length) el.innerHTML = `<div class="empty">${escapeHtml(title)}</div>`;
  el.querySelectorAll("button.rank-row").forEach((btn) => {
    btn.addEventListener("click", () => onPick(Number(btn.dataset.mp)));
  });
}

function renderMpModal({ overviewMp, payload }) {
  $("mpModalTitle").textContent = payload?.mp_name ?? overviewMp?.mp_name ?? "";
  $("mpModalSub").textContent = payload?.clubs_at_vote_time?.[0]?.club ?? overviewMp?.club ?? "";

  const summary = payload?.summary ?? overviewMp ?? {};
  const recent = payload?.recent_votes ?? [];
  const clubs = payload?.clubs_at_vote_time ?? [];

  const cards = [
    { k: "Účasť", v: typeof summary.participation_rate === "number" ? formatPct(summary.participation_rate) : "—" },
    { k: "Prítomný", v: formatInt(summary.present_count) },
    { k: "Neprítomný", v: formatInt(summary.absent_count) },
    { k: "Spolu", v: formatInt(summary.total_votes) },
  ];

  $("mpModalBody").innerHTML = `
    <div class="modal-cards">
      ${cards.map((c) => `<div class="mcard"><div class="k">${escapeHtml(c.k)}</div><div class="v">${escapeHtml(c.v)}</div></div>`).join("")}
    </div>
    <div class="modal-split">
      <div>
        <h3>Klubové členstvo (podľa hlasovaní)</h3>
        <div class="clubs-mini">
          ${
            clubs.length
              ? clubs
                  .slice(0, 8)
                  .map((c) => {
                    const rate = safeNumber(c.participation_rate);
                    return `
                      <div class="club-mini">
                        <div class="club-mini-name">${escapeHtml(c.club ?? "")}</div>
                        <div class="club-mini-bar"><span style="width:${rate === null ? 0 : Math.max(0, Math.min(100, rate * 100))}%"></span></div>
                        <div class="club-mini-rate">${rate === null ? "—" : escapeHtml(formatPct(rate))}</div>
                      </div>
                    `;
                  })
                  .join("")
              : `<div class="empty">No MP payload (run build with --include-mp-pages).</div>`
          }
        </div>
      </div>
      <div>
        <h3>Posledné hlasovania</h3>
        <div class="recent-mini">
          ${
            recent.length
              ? recent
                  .slice(0, 10)
                  .map(
                    (v) => `
                      <div class="recent-mini-row">
                        <div class="d">${escapeHtml(String(v.vote_datetime_local ?? "").slice(0, 16))}</div>
                        <div class="t">${escapeHtml(v.title ?? "")}</div>
                        <div class="c">${escapeHtml(v.vote_code ?? "")}</div>
                      </div>
                    `,
                  )
                  .join("")
              : `<div class="empty">No recent votes in payload.</div>`
          }
        </div>
      </div>
    </div>
  `;
}

async function loadOverview(termId) {
  return fetchJson(`term/${termId}/overview.json`);
}

async function loadMp(termId, mpId) {
  return fetchJson(`term/${termId}/mp/${mpId}.json`);
}

async function main() {
  const termSelect = $("termSelect");
  const clubSelect = $("clubSelect");
  const searchInput = $("searchInput");
  const dialog = $("mpDialog");

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

  let overview = null;
  let selectedClubKey = "";

  function refreshLists() {
    const filtered = rankRows(overview?.mps ?? [], {
      clubKey: selectedClubKey || null,
      query: searchInput.value,
    });
    const top = filtered
      .slice()
      .sort((a, b) => (b.participation_rate ?? -1) - (a.participation_rate ?? -1))
      .slice(0, 10);
    const bottom = filtered
      .slice()
      .sort((a, b) => (a.participation_rate ?? 2) - (b.participation_rate ?? 2))
      .slice(0, 10);

    renderKpis(filtered);
    renderRankList({
      elId: "topList",
      rows: top,
      title: "No matching MPs.",
      onPick: openMp,
    });
    renderRankList({
      elId: "bottomList",
      rows: bottom,
      title: "No matching MPs.",
      onPick: openMp,
    });
  }

  function refreshClubSelect() {
    const options = buildClubOptions({ clubs: overview?.clubs ?? [], mps: overview?.mps ?? [] });
    clubSelect.innerHTML = "";
    clubSelect.appendChild(new Option("Všetky kluby", ""));
    for (const o of options) {
      clubSelect.appendChild(new Option(o.label, o.key));
    }
    clubSelect.disabled = false;
    clubSelect.value = selectedClubKey;
  }

  async function openMp(mpId) {
    const termId = Number(termSelect.value);
    const mp = (overview?.mps ?? []).find((m) => m.mp_id === mpId) ?? null;
    let payload = null;
    try {
      payload = await loadMp(termId, mpId);
    } catch {
      payload = null;
    }
    renderMpModal({ overviewMp: mp, payload });
    if (typeof dialog.showModal === "function") dialog.showModal();
  }

  function closeDialog() {
    if (typeof dialog.close === "function") dialog.close();
  }

  $("mpClose").addEventListener("click", closeDialog);
  dialog.addEventListener("click", (e) => {
    if (e.target === dialog) closeDialog();
  });

  async function refresh(termId) {
    overview = await loadOverview(termId);
    selectedClubKey = "";
    refreshClubSelect();
    const handlePick = (clubKey) => {
      selectedClubKey = selectedClubKey === clubKey ? "" : clubKey;
      clubSelect.value = selectedClubKey;
      renderClubBars({ clubs: overview.clubs ?? [], selectedClubKey, onPick: handlePick });
      refreshLists();
    };
    renderClubBars({ clubs: overview.clubs ?? [], selectedClubKey, onPick: handlePick });
    refreshLists();
  }

  termSelect.value = String(defaultTermId);
  await refresh(defaultTermId);

  termSelect.addEventListener("change", async () => {
    await refresh(Number(termSelect.value));
  });

  clubSelect.addEventListener("change", () => {
    selectedClubKey = String(clubSelect.value ?? "");
    refreshLists();
    const handlePick = (clubKey) => {
      selectedClubKey = selectedClubKey === clubKey ? "" : clubKey;
      clubSelect.value = selectedClubKey;
      renderClubBars({ clubs: overview?.clubs ?? [], selectedClubKey, onPick: handlePick });
      refreshLists();
    };
    renderClubBars({ clubs: overview?.clubs ?? [], selectedClubKey, onPick: handlePick });
  });

  searchInput.addEventListener("input", () => refreshLists());
}

main().catch((err) => {
  console.error(err);
  setMeta(`Error: ${err.message ?? String(err)}`);
});
