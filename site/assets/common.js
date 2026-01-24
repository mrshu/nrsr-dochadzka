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

