const DATA_BASE = new URL("./data/", import.meta.url);

export function dataUrl(path) {
  return new URL(path, DATA_BASE).toString();
}

export async function fetchJson(path) {
  const url = typeof path === "string" ? dataUrl(path) : String(path);
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed ${res.status}: ${url}`);
  return res.json();
}

export function $(id) {
  const el = document.getElementById(id);
  if (!el) throw new Error(`Missing element: ${id}`);
  return el;
}

export function formatPct(value) {
  if (typeof value !== "number") return "";
  return `${(value * 100).toFixed(2)}%`;
}

export function formatInt(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") return value.toLocaleString("sk-SK");
  return String(value);
}

export function setMeta(text) {
  const el = document.getElementById("meta");
  if (el) el.textContent = text;
}

export function slugify(value) {
  const normalized = String(value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const slug = normalized
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  return slug || "unknown";
}

export function normalizeText(value) {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}
