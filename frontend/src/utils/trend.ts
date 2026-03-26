import type { PgTrendPoint } from "@/types/data";

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function toStringValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function parseYearMonth(label: string): number | null {
  const plain = label.replace(/[^\d]/g, "");
  if (!/^\d{6}$/.test(plain)) return null;
  const ym = Number(plain);
  const month = ym % 100;
  if (month < 1 || month > 12) return null;
  return ym;
}

function normalizeTrendPoint(item: Record<string, unknown>): PgTrendPoint | null {
  const rawLabel =
    toStringValue(item.label) ||
    toStringValue(item.dk) ||
    toStringValue(item.year_month) ||
    (() => {
      const year = toNumber(item.year);
      const month = toNumber(item.month);
      if (year === null || month === null) return "";
      return `${Math.trunc(year)}${String(Math.trunc(month)).padStart(2, "0")}`;
    })();
  if (!rawLabel) return null;

  return {
    label: rawLabel,
    searches:
      toNumber(item.searches) ??
      toNumber(item.sales) ??
      toNumber(item.total_searches) ??
      toNumber(item.search) ??
      toNumber(item.value),
    rank: toNumber(item.rank),
    rank_growth_rate: toNumber(item.rankGrowthRate) ?? toNumber(item.rank_growth_rate),
    searches_growth_rate: toNumber(item.searchesGrowthRate) ?? toNumber(item.searches_growth_rate),
  };
}

function objectArrayFromRaw(raw: unknown): Record<string, unknown>[] {
  if (Array.isArray(raw)) {
    return raw.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"));
  }
  if (raw && typeof raw === "object") {
    const obj = raw as Record<string, unknown>;
    const nested = obj.trends ?? obj.items ?? obj.data ?? obj.list;
    if (Array.isArray(nested)) {
      return nested.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"));
    }
    return [obj];
  }
  if (typeof raw === "string") {
    const text = raw.trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      return objectArrayFromRaw(parsed);
    } catch {
      return [];
    }
  }
  return [];
}

export function parsePgTrendPoints(raw: unknown): PgTrendPoint[] {
  const rows = objectArrayFromRaw(raw);
  const parsed = rows
    .map((item) => normalizeTrendPoint(item))
    .filter((item): item is PgTrendPoint => Boolean(item));

  const uniqueByLabel = new Map<string, PgTrendPoint>();
  for (const item of parsed) {
    uniqueByLabel.set(item.label, item);
  }
  const output = Array.from(uniqueByLabel.values());
  output.sort((a, b) => {
    const aYm = parseYearMonth(a.label);
    const bYm = parseYearMonth(b.label);
    if (aYm !== null && bYm !== null) return aYm - bYm;
    if (aYm !== null) return -1;
    if (bYm !== null) return 1;
    return a.label.localeCompare(b.label);
  });
  return output;
}

export function formatTrendLabel(label: string): string {
  const ym = parseYearMonth(label);
  if (ym === null) return label;
  return `${Math.floor(ym / 100)}-${String(ym % 100).padStart(2, "0")}`;
}
