import { $, fetchJson, formatInt, formatPct, setMeta, slugify } from "./common.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
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

function overviewPath(termId, windowKey, absenceKey) {
  const w = windowKey === "180d" ? "180d" : "full";
  const a = absenceKey === "abs0" ? "abs0" : "abs0n";
  return `term/${termId}/overview.${w}.${a}.json`;
}

function getParam(name) {
  const url = new URL(window.location.href);
  return url.searchParams.get(name);
}

function setParam(name, value) {
  const url = new URL(window.location.href);
  if (value === null || value === undefined || value === "") url.searchParams.delete(name);
  else url.searchParams.set(name, String(value));
  window.history.replaceState(null, "", url.toString());
}

function mpIdFromPath() {
  const path = window.location.pathname.replace(/\/+/g, "/");
  const match = path.match(/\/mp\/(\d+)(?:-[^/]+)?\/?$/);
  if (!match) return null;
  return Number(match[1]);
}

function basePathForMp() {
  const path = window.location.pathname.replace(/\/+/g, "/");
  const idx = path.indexOf("/mp/");
  if (idx === -1) {
    return path.replace(/\/[^/]*$/, "");
  }
  return path.slice(0, idx);
}

function canonicalMpUrl(mpId, mpName) {
  const base = basePathForMp();
  const slug = slugify(mpName ?? "");
  const baseNormalized = base === "/" ? "" : base.replace(/\/$/, "");
  return `${window.location.origin}${baseNormalized}/mp/${mpId}-${slug}/`;
}

function statCards(summary) {
  const cards = [
    { label: "Účasť", value: formatPct(summary.participation_rate) },
    { label: "Prítomný", value: formatInt(summary.present_count) },
    { label: "Neprítomný", value: formatInt(summary.absent_count) },
    { label: "Hlasoval", value: formatInt(summary.voted_count) },
    { label: "Spolu", value: formatInt(summary.total_votes) },
  ];
  return cards
    .map(
      (c) =>
        `<div class="mcard"><div class="k">${escapeHtml(c.label)}</div><div class="v">${escapeHtml(
          c.value,
        )}</div></div>`,
    )
    .join("");
}

function renderKpis(summary) {
  const el = $("kpis");
  el.innerHTML = statCards(summary);
}

function renderProfile(summary, mpRow) {
  const el = document.getElementById("profile");
  if (!el) return;
  const club = mpRow?.club ?? "";
  const current = mpRow?.current_club ?? "";
  const primary = mpRow?.primary_club ?? "";
  el.innerHTML = `
    <div class="hint">
      Klub (pre leaderboard): ${escapeHtml(club)}
      ${current && current !== club ? ` • current: ${escapeHtml(current)}` : ""}
      ${primary && primary !== club ? ` • primary: ${escapeHtml(primary)}` : ""}
    </div>
    <div class="hint" style="margin-top: 10px">Pozn.: absencia sa riadi zvoleným prepínačom.</div>
  `;
}

function renderClubs(rows, hueMap, colorMap) {
  const el = $("clubs");
  if (!rows.length) {
    el.innerHTML = `<div class="empty">No MP payload (run build with --include-mp-pages).</div>`;
    return;
  }
  el.innerHTML = `
    <div class="clubs-mini">
      ${rows
        .slice(0, 20)
        .map((c) => {
          const r = typeof c.participation_rate === "number" ? c.participation_rate : null;
          const total = formatInt(c.total_votes);
          const present = formatInt(c.present_count);
          const key = c.club_key ?? c.club ?? "unknown";
          const hue = hueForClub(hueMap, key);
          const color = clubColorFor(colorMap, key, c);
          const style = clubStyle({ hue, color });
          return `
            <div class="club-mini">
              <div class="club-mini-name">${escapeHtml(c.club ?? "")}</div>
              <div class="club-mini-bar"><span style="${style} width:${r === null ? 0 : Math.max(0, Math.min(100, r * 100))}%"></span></div>
              <div class="club-mini-meta">
                <div class="club-mini-count">${escapeHtml(present)} / ${escapeHtml(total)}</div>
                <div class="club-mini-rate">${r === null ? "—" : escapeHtml(formatPct(r))}</div>
              </div>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderRecent(rows) {
  const el = $("recent");
  if (!rows.length) {
    el.innerHTML = `<div class="empty">No recent votes in payload.</div>`;
    return;
  }
  el.innerHTML = `
    <div class="recent-mini">
      ${rows
        .slice(0, 20)
        .map(
          (v) => `
            <div class="recent-mini-row">
              <div class="d">${escapeHtml(String(v.vote_datetime_local ?? "").slice(0, 16))}</div>
              <div class="t">${escapeHtml(v.title ?? "")}</div>
              <div class="c">${escapeHtml(v.vote_code ?? "")}</div>
            </div>
          `,
        )
        .join("")}
    </div>
  `;
}

async function loadMpPayload(termId, mpId, windowKey, absenceKey) {
  const w = windowKey === "180d" ? "180d" : "full";
  const a = absenceKey === "abs0" ? "abs0" : "abs0n";
  try {
    return await fetchJson(`term/${termId}/mp/${w}.${a}/${mpId}.json`);
  } catch {
    return fetchJson(`term/${termId}/mp/${mpId}.json`);
  }
}

async function main() {
  const termSelect = $("termSelect");
  const windowSelect = $("windowSelect");
  const absenceSelect = $("absenceSelect");
  const subtitle = document.getElementById("subtitle");

  const bodyMp = Number(document.body?.dataset?.mpId || "");
  const pathMp = mpIdFromPath();
  const mpId = Number.isFinite(bodyMp) && bodyMp > 0 ? bodyMp : Number(getParam("mp") || pathMp);
  if (!Number.isFinite(mpId)) {
    if (subtitle) subtitle.textContent = "Chýba parameter mp";
    setMeta("Error: missing ?mp=<id>");
    return;
  }

  const manifest = await fetchJson("manifest.json");
  const terms = manifest.terms ?? [];
  const defaultTermId = Number(getParam("term")) || (manifest.default_term_id ?? terms[0]);

  for (const t of terms) {
    const opt = document.createElement("option");
    opt.value = String(t);
    opt.textContent = String(t);
    termSelect.appendChild(opt);
  }
  termSelect.disabled = false;
  windowSelect.disabled = false;
  absenceSelect.disabled = false;

  windowSelect.value = getParam("window") || "180d";
  absenceSelect.value = getParam("absence") || "abs0n";
  termSelect.value = String(defaultTermId);

  async function refresh() {
    const termId = Number(termSelect.value);
    const windowKey = String(windowSelect.value ?? "180d");
    const absenceKey = String(absenceSelect.value ?? "abs0n");
    setParam("term", termId);
    setParam("window", windowKey);
    setParam("absence", absenceKey);

    const overview = await fetchJson(overviewPath(termId, windowKey, absenceKey));
    const clubHueMap = buildClubHueMap(overview?.clubs ?? []);
    const clubColorMap = buildClubColorMap(overview?.clubs ?? []);
    const mpRow = (overview.mps ?? []).find((m) => m.mp_id === mpId) ?? null;

    if (!mpRow) {
      $("mpTitle").textContent = `MP ${mpId}`;
      $("mpHint").textContent = "MP not found in this window (no votes / filtered out).";
      const profile = document.getElementById("profile");
      if (profile) profile.innerHTML = `<div class="empty">No data for this MP in selected window.</div>`;
      $("clubs").innerHTML = "";
      $("recent").innerHTML = "";
      const w = overview?.window ?? {};
      const a = overview?.absence ?? {};
      setMeta(
        `Updated: ${manifest.last_updated_utc ?? "?"} • ${w.kind === "rolling" ? `last ${w.days}d` : "full"} • ${
          a.kind === "abs0" ? "abs0" : "abs0n"
        }`,
      );
      return;
    }

    const mpTitle = document.getElementById("mpTitle");
    if (mpTitle) mpTitle.textContent = mpRow.mp_name ?? `MP ${mpId}`;
    const mpHint = document.getElementById("mpHint");
    if (mpHint) mpHint.textContent = `MP ID: ${mpId}`;
    const heroTitle = document.getElementById("mpHeroTitle");
    if (heroTitle) heroTitle.textContent = mpRow.mp_name ?? `MP ${mpId}`;
    const heroSub = document.getElementById("mpHeroSub");
    if (heroSub) {
      const clubLabel = mpRow.club ? String(mpRow.club) : "—";
      const clubKey = mpRow.club_key ?? mpRow.club ?? "unknown";
      const hue = hueForClub(clubHueMap, clubKey);
      const color = clubColorFor(clubColorMap, clubKey, mpRow);
      const style = clubStyle({ hue, color });
      heroSub.innerHTML = `
        <span class="badge hero-badge" style="${style}">${escapeHtml(clubLabel)}</span>
        <span class="hero-sub-meta">MP ID: ${escapeHtml(String(mpId))}</span>
      `;
    }
    renderKpis(mpRow);
    renderProfile(mpRow, mpRow);

    let payload = null;
    try {
      payload = await loadMpPayload(termId, mpId, windowKey, absenceKey);
    } catch {
      payload = null;
    }
    renderClubs(payload?.clubs_at_vote_time ?? [], clubHueMap, clubColorMap);
    renderRecent(payload?.recent_votes ?? []);

    const w = overview?.window ?? {};
    const a = overview?.absence ?? {};
    const label =
      w.kind === "rolling"
        ? `last ${w.days ?? 180}d (${String(w.from_utc ?? "").slice(0, 10)} → ${String(w.to_utc ?? "").slice(0, 10)})`
        : "full term";
    setMeta(
      `Updated: ${manifest.last_updated_utc ?? "?"} • ${label} • ${a.kind === "abs0" ? "Absence: 0" : "Absence: 0+N"}`,
    );

    const canonical = canonicalMpUrl(mpId, mpRow?.mp_name ?? "");
    let link = document.querySelector("link[rel='canonical']");
    if (!link) {
      link = document.createElement("link");
      link.setAttribute("rel", "canonical");
      document.head.appendChild(link);
    }
    link.setAttribute("href", canonical);
  }

  await refresh();

  termSelect.addEventListener("change", refresh);
  windowSelect.addEventListener("change", refresh);
  absenceSelect.addEventListener("change", refresh);
}

main().catch((err) => {
  console.error(err);
  setMeta(`Error: ${err.message ?? String(err)}`);
});
