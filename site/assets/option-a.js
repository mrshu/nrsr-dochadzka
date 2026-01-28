import { $, fetchJson, formatInt, formatPct, normalizeText, setMeta, slugify } from "./common.js";

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

function buildClubHueMap(clubs) {
  const keys = (clubs ?? [])
    .map((c) => String(c.club_key ?? c.club ?? "unknown"))
    .filter(Boolean);
  const unique = [...new Set(keys)].sort((a, b) => a.localeCompare(b, "sk"));
  const step = unique.length ? Math.floor(360 / unique.length) : 0;
  const map = new Map();
  unique.forEach((key, idx) => {
    map.set(key, (idx * step) % 360);
  });
  return map;
}

function buildClubColorMap(clubs) {
  const map = new Map();
  for (const c of clubs ?? []) {
    const key = String(c.club_key ?? c.club ?? "unknown");
    const color = typeof c.club_color === "string" ? c.club_color : null;
    if (color) map.set(key, color);
  }
  return map;
}

function hueForClub(map, key) {
  if (map && map.has(key)) return map.get(key);
  return hashToHue(key);
}

function clubColorFor(map, key, row) {
  if (row && typeof row.club_color === "string") return row.club_color;
  if (map && map.has(key)) return map.get(key);
  return null;
}

function clubStyle({ hue, color }) {
  return `--h:${hue};${color ? `--club-color:${color};` : ""}`;
}

// URL state management
function getParam(name) {
  const url = new URL(window.location.href);
  return url.searchParams.get(name);
}

function setParam(name, value) {
  const url = new URL(window.location.href);
  if (value === null || value === undefined || value === "") {
    url.searchParams.delete(name);
  } else {
    url.searchParams.set(name, String(value));
  }
  window.history.replaceState(null, "", url.toString());
}

function renderKpis(overview, mps) {
  const kpis = $("kpis");
  const rates = mps
    .map((m) => safeNumber(m.participation_rate))
    .filter((v) => typeof v === "number")
    .sort((a, b) => a - b);
  const p50 = percentile(rates, 0.5);
  const avg =
    rates.length ? rates.reduce((acc, v) => acc + v, 0) / Math.max(1, rates.length) : null;
  const best = rates.length ? rates[rates.length - 1] : null;
  const worst = rates.length ? rates[0] : null;
  const totalVotes = mps
    .map((m) => (typeof m.total_votes === "number" ? m.total_votes : null))
    .filter((v) => v !== null)
    .reduce((acc, v) => Math.max(acc, v ?? 0), 0);
  const items = [
    { label: "Hlasovania", value: formatInt(totalVotes) },
    { label: "Medián účasti", value: p50 === null ? "—" : formatPct(p50) },
    { label: "Priemer účasti", value: avg === null ? "—" : formatPct(avg) },
    { label: "Najlepšia účasť", value: best === null ? "—" : formatPct(best) },
    { label: "Najhoršia účasť", value: worst === null ? "—" : formatPct(worst) },
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

function renderClubBars({ clubs, selectedClubKey, onPick, hueMap, colorMap }) {
  const el = $("clubBars");
  const rows = [...(clubs ?? [])].sort(
    (a, b) => (safeNumber(b.participation_rate) ?? -1) - (safeNumber(a.participation_rate) ?? -1),
  );
  el.innerHTML = rows
    .map((c) => {
      const key = String(c.club_key ?? "unknown");
      const rate = safeNumber(c.participation_rate);
      const hue = hueForClub(hueMap, key);
      const color = clubColorFor(colorMap, key, c);
      const style = clubStyle({ hue, color });
      const active = selectedClubKey === key;
      return `
        <button class="club-bar-row ${active ? "active" : ""}" type="button" data-club="${escapeHtml(key)}">
          <div class="club-left">
            <span class="dot" style="${style}"></span>
            <span class="club-name">${escapeHtml(c.club ?? key)}</span>
          </div>
          <div class="club-mid">
            <div class="bar"><span style="${style} width:${rate === null ? 0 : Math.max(0, Math.min(100, rate * 100))}%"></span></div>
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

function renderSelectedClubBanner({ club, onClear, hueMap, colorMap }) {
  const el = $("selectedClubBanner");
  if (!club) {
    el.innerHTML = "";
    el.style.display = "none";
    return;
  }
  const key = club.club_key ?? "unknown";
  const hue = hueForClub(hueMap, key);
  const color = clubColorFor(colorMap, key, club);
  const style = clubStyle({ hue, color });
  el.style.display = "flex";
  el.innerHTML = `
    <span class="banner-label">Filtrované podľa klubu:</span>
    <span class="badge" style="${style}">${escapeHtml(club.club ?? key)}</span>
    <button class="banner-clear" type="button">Zrušiť filter</button>
  `;
  el.querySelector(".banner-clear").addEventListener("click", onClear);
}

function rankRows(mps, { clubKey, query }) {
  const q = normalizeText((query ?? "").trim());
  return (mps ?? [])
    .filter((m) => (clubKey ? String(normalizeClubKey(m)) === String(clubKey) : true))
    .filter((m) => (q ? normalizeText(m.mp_name ?? "").includes(q) : true))
    .filter((m) => typeof m.participation_rate === "number")
    .slice();
}

function renderRankList({ elId, rows, title, hrefFor, hueMap, colorMap }) {
  const el = $(elId);
  el.innerHTML = rows
    .map((m, idx) => {
      const rate = safeNumber(m.participation_rate);
      const clubKey = normalizeClubKey(m);
      const hue = hueForClub(hueMap, clubKey);
      const color = clubColorFor(colorMap, clubKey, m);
      const style = clubStyle({ hue, color });
      const href = hrefFor ? hrefFor(m) : "#";
      return `
        <a class="rank-row" href="${escapeHtml(href)}" data-mp="${escapeHtml(String(m.mp_id))}">
          <div class="rank">${escapeHtml(String(idx + 1).padStart(2, "0"))}</div>
          <div class="who">
            <div class="name">${escapeHtml(m.mp_name ?? "")}</div>
            <div class="sub">
              <span class="badge" style="${style}">${escapeHtml(m.club ?? "(unknown)")}</span>
              <span class="muted">abs: ${escapeHtml(formatInt(m.absent_count))}</span>
              <span class="muted">n: ${escapeHtml(formatInt(m.total_votes))}</span>
            </div>
          </div>
          <div class="score">
            <div class="pct">${rate === null ? "—" : escapeHtml(formatPct(rate))}</div>
            <div class="mini"><span style="${style} width:${rate === null ? 0 : Math.max(0, Math.min(100, rate * 100))}%"></span></div>
          </div>
        </a>
      `;
    })
    .join("");
  if (!rows.length) el.innerHTML = `<div class="empty">${escapeHtml(title)}</div>`;
}

function renderMpModal({ overviewMp, payload, hueMap, colorMap }) {
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
                    const key = c.club_key ?? c.club ?? "unknown";
                    const hue = hueForClub(hueMap, key);
                    const color = clubColorFor(colorMap, key, c);
                    const style = clubStyle({ hue, color });
                    return `
                      <div class="club-mini">
                        <div class="club-mini-name">${escapeHtml(c.club ?? "")}</div>
                        <div class="club-mini-bar"><span style="${style} width:${rate === null ? 0 : Math.max(0, Math.min(100, rate * 100))}%"></span></div>
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

async function loadMp(termId, mpId) {
  return fetchJson(`term/${termId}/mp/${mpId}.json`);
}

function overviewPath(termId, windowKey, absenceKey) {
  const w = windowKey === "180d" ? "180d" : "full";
  const a = absenceKey === "abs0" ? "abs0" : "abs0n";
  return `term/${termId}/overview.${w}.${a}.json`;
}

function mpProfileUrl({ mpId, mpName, termId, windowKey, absenceKey }) {
  const slug = slugify(mpName ?? "");
  const base = new URL(".", window.location.href);
  const url = new URL(`mp/${mpId}-${slug}/`, base);
  url.searchParams.set("term", String(termId));
  url.searchParams.set("window", windowKey === "180d" ? "180d" : "full");
  url.searchParams.set("absence", absenceKey === "abs0" ? "abs0" : "abs0n");
  return url.toString();
}

function setModalProfileLink(href) {
  const body = $("mpModalBody");
  let row = body.querySelector(".profile-link-row");
  if (!row) {
    row = document.createElement("div");
    row.className = "profile-link-row";
    body.prepend(row);
  }
  row.innerHTML = `<a class="profile-link" href="${href}">Otvoriť profil poslanca</a>`;
}

async function main() {
  const termSelect = $("termSelect");
  const windowSelect = $("windowSelect");
  const absenceSelect = $("absenceSelect");
  const clubSelect = $("clubSelect");
  const searchInput = $("searchInput");
  const dialog = $("mpDialog");

  const manifest = await fetchJson("manifest.json");
  const terms = manifest.terms ?? [];
  const defaultTermId = manifest.default_term_id ?? terms[0];

  // Read initial state from URL
  let termId = Number(getParam("term")) || defaultTermId;
  let windowKey = getParam("window") || "180d";
  let absenceKey = getParam("absence") || "abs0n";
  let selectedClubKey = getParam("club") || "";

  termSelect.innerHTML = "";
  for (const t of terms) {
    const opt = document.createElement("option");
    opt.value = String(t);
    opt.textContent = String(t);
    termSelect.appendChild(opt);
  }
  termSelect.value = String(termId);
  windowSelect.value = windowKey;
  absenceSelect.value = absenceKey;
  termSelect.disabled = false;
  windowSelect.disabled = false;
  absenceSelect.disabled = false;
  setMeta(`Updated: ${manifest.last_updated_utc ?? "?"}`);

  let overview = null;
  let clubHueMap = null;
  let clubColorMap = null;

  function updateUrl() {
    setParam("term", termId);
    setParam("window", windowKey);
    setParam("absence", absenceKey);
    setParam("club", selectedClubKey || null);
  }

  function getSelectedClub() {
    if (!selectedClubKey) return null;
    return (overview?.clubs ?? []).find((c) => c.club_key === selectedClubKey) ?? null;
  }

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

    renderKpis(overview, filtered);
    const hrefFor = (mp) =>
      mpProfileUrl({ mpId: mp.mp_id, mpName: mp.mp_name, termId, windowKey, absenceKey });
    renderRankList({
      elId: "topList",
      rows: top,
      title: "No matching MPs.",
      hrefFor,
      hueMap: clubHueMap,
      colorMap: clubColorMap,
    });
    renderRankList({
      elId: "bottomList",
      rows: bottom,
      title: "No matching MPs.",
      hrefFor,
      hueMap: clubHueMap,
      colorMap: clubColorMap,
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

  function handleClubPick(clubKey) {
    selectedClubKey = selectedClubKey === clubKey ? "" : clubKey;
    clubSelect.value = selectedClubKey;
    updateUrl();
    renderClubBars({
      clubs: overview?.clubs ?? [],
      selectedClubKey,
      onPick: handleClubPick,
      hueMap: clubHueMap,
      colorMap: clubColorMap,
    });
    renderSelectedClubBanner({
      club: getSelectedClub(),
      onClear: () => handleClubPick(selectedClubKey),
      hueMap: clubHueMap,
      colorMap: clubColorMap,
    });
    refreshLists();
  }

  async function openMp(mpId) {
    const mp = (overview?.mps ?? []).find((m) => m.mp_id === mpId) ?? null;
    let payload = null;
    try {
      payload = await loadMp(termId, mpId);
    } catch {
      payload = null;
    }
    renderMpModal({ overviewMp: mp, payload, hueMap: clubHueMap, colorMap: clubColorMap });
    setModalProfileLink(mpProfileUrl({ mpId, mpName: mp?.mp_name, termId, windowKey, absenceKey }));
    if (typeof dialog.showModal === "function") dialog.showModal();
  }

  function closeDialog() {
    if (typeof dialog.close === "function") dialog.close();
  }

  $("mpClose").addEventListener("click", closeDialog);
  dialog.addEventListener("click", (e) => {
    if (e.target === dialog) closeDialog();
  });

  async function refresh() {
    overview = await fetchJson(overviewPath(termId, windowKey, absenceKey));
    clubHueMap = buildClubHueMap(overview?.clubs ?? []);
    clubColorMap = buildClubColorMap(overview?.clubs ?? []);

    // Validate selectedClubKey exists in current data
    const validClubKeys = new Set((overview?.clubs ?? []).map((c) => c.club_key));
    if (selectedClubKey && !validClubKeys.has(selectedClubKey)) {
      selectedClubKey = "";
    }

    refreshClubSelect();
    renderClubBars({
      clubs: overview?.clubs ?? [],
      selectedClubKey,
      onPick: handleClubPick,
      hueMap: clubHueMap,
      colorMap: clubColorMap,
    });
    renderSelectedClubBanner({
      club: getSelectedClub(),
      onClear: () => handleClubPick(selectedClubKey),
      hueMap: clubHueMap,
      colorMap: clubColorMap,
    });
    refreshLists();
    updateUrl();

    const w = overview?.window ?? {};
    const a = overview?.absence ?? {};
    const label =
      w.kind === "rolling"
        ? `Window: last ${w.days ?? 180}d (${String(w.from_utc ?? "").slice(0, 10)} → ${String(
            w.to_utc ?? "",
          ).slice(0, 10)})`
        : "Window: full term";
    const absLabel =
      a.kind === "abs0"
        ? "Absence: 0 only"
        : "Absence: 0 + N";
    setMeta(`Updated: ${manifest.last_updated_utc ?? "?"} • ${label} • ${absLabel}`);
  }

  await refresh();

  termSelect.addEventListener("change", async () => {
    termId = Number(termSelect.value);
    await refresh();
  });

  windowSelect.addEventListener("change", async () => {
    windowKey = String(windowSelect.value ?? "full");
    await refresh();
  });

  absenceSelect.addEventListener("change", async () => {
    absenceKey = String(absenceSelect.value ?? "abs0n");
    await refresh();
  });

  clubSelect.addEventListener("change", () => {
    selectedClubKey = String(clubSelect.value ?? "");
    updateUrl();
    renderClubBars({
      clubs: overview?.clubs ?? [],
      selectedClubKey,
      onPick: handleClubPick,
      hueMap: clubHueMap,
      colorMap: clubColorMap,
    });
    renderSelectedClubBanner({
      club: getSelectedClub(),
      onClear: () => handleClubPick(selectedClubKey),
      hueMap: clubHueMap,
      colorMap: clubColorMap,
    });
    refreshLists();
  });

  searchInput.addEventListener("input", () => refreshLists());
}

main().catch((err) => {
  console.error(err);
  setMeta(`Error: ${err.message ?? String(err)}`);
});
