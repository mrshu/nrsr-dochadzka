import { $, fetchJson, formatInt, formatPct, setMeta } from "./common.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
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
  const path = window.location.pathname.replace(/\\/+/g, "/");
  const match = path.match(/\\/mp\\/(\\d+)\\/?$/);
  if (!match) return null;
  return Number(match[1]);
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
  const el = $("profile");
  const club = mpRow?.club ?? "";
  const current = mpRow?.current_club ?? "";
  const primary = mpRow?.primary_club ?? "";
  el.innerHTML = `
    <div class="hint">
      Klub (pre leaderboard): ${escapeHtml(club)}
      ${current && current !== club ? ` • current: ${escapeHtml(current)}` : ""}
      ${primary && primary !== club ? ` • primary: ${escapeHtml(primary)}` : ""}
    </div>
    <div class="modal-cards" style="margin-top: 12px">
      ${statCards(summary)}
    </div>
    <div class="hint" style="margin-top: 10px">Pozn.: absencia sa riadi zvoleným prepínačom.</div>
  `;
}

function renderClubs(rows) {
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
          return `
            <div class="club-mini">
              <div class="club-mini-name">${escapeHtml(c.club ?? "")}</div>
              <div class="club-mini-bar"><span style="width:${r === null ? 0 : Math.max(0, Math.min(100, r * 100))}%"></span></div>
              <div class="club-mini-rate">${r === null ? "—" : escapeHtml(formatPct(r))}</div>
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

async function loadMpPayload(termId, mpId) {
  return fetchJson(`term/${termId}/mp/${mpId}.json`);
}

async function main() {
  const termSelect = $("termSelect");
  const windowSelect = $("windowSelect");
  const absenceSelect = $("absenceSelect");
  const subtitle = $("subtitle");

  const bodyMp = Number(document.body?.dataset?.mpId || "");
  const pathMp = mpIdFromPath();
  const mpId = Number.isFinite(bodyMp) && bodyMp > 0 ? bodyMp : Number(getParam("mp") || pathMp);
  if (!Number.isFinite(mpId)) {
    subtitle.textContent = "Chýba parameter mp";
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
    const mpRow = (overview.mps ?? []).find((m) => m.mp_id === mpId) ?? null;

    if (!mpRow) {
      $("mpTitle").textContent = `MP ${mpId}`;
      $("mpHint").textContent = "MP not found in this window (no votes / filtered out).";
      $("profile").innerHTML = `<div class="empty">No data for this MP in selected window.</div>`;
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

    $("mpTitle").textContent = mpRow.mp_name ?? `MP ${mpId}`;
    $("mpHint").textContent = `MP ID: ${mpId}`;
    renderKpis(mpRow);
    renderProfile(mpRow, mpRow);

    let payload = null;
    try {
      payload = await loadMpPayload(termId, mpId);
    } catch {
      payload = null;
    }
    renderClubs(payload?.clubs_at_vote_time ?? []);
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

    const canonical = `${window.location.origin}${window.location.pathname}`.replace(/\\/index\\.html$/, "/");
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
