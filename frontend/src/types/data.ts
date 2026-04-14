export interface PgItemsResponse {
  columns: string[];
  items: Record<string, unknown>[];
  total: number;
  page: number;
  page_size: number;
  average_total_searches?: number | null;
  average_label?: string | null;
}

export interface PgAllItemsResponse {
  columns: string[];
  items: Record<string, unknown>[];
  total: number;
}

export interface YearMonthsResponse {
  items: number[];
}

export interface PgFilterOption {
  value: string;
  label: string;
  count: number;
}

export interface PgFilterOptionsResponse {
  items: PgFilterOption[];
}

export interface WordFrequencyTrendPoint {
  year_month: number | null;
  year: number | null;
  month: number | null;
  freq: number | null;
  freq_ratio: number | null;
  coverage: number | null;
  total_searches: number | null;
  rank?: number | null;
  rank_growth_rate?: number | null;
  searches_growth_rate?: number | null;
}

export interface PgTrendPoint {
  label: string;
  searches: number | null;
  rank: number | null;
  rank_growth_rate: number | null;
  searches_growth_rate: number | null;
}

export interface WordFrequencyTrendInfo {
  translation_zh?: string | null;
  tag_label?: string | null;
  reason?: string | null;
  object_category?: string | null;
  plushable?: string | null;
  plushable_bool?: boolean | null;
}

export interface WordFrequencyTrendResponse {
  word: string;
  info: WordFrequencyTrendInfo;
  points: WordFrequencyTrendPoint[];
  latest_year_month?: number | null;
}

export interface WeightedBlanketsPoundsSummaryItem {
  pounds: number | null;
  pounds_label: string;
  product_count: number;
  total_units: number;
  total_amount: number;
  avg_price: number | null;
  active_periods: number;
}

export interface WeightedBlanketsPoundsSummaryResponse {
  view: "yearly" | "monthly";
  year: number | null;
  month: number | null;
  items: WeightedBlanketsPoundsSummaryItem[];
  total_products: number;
  total_units: number;
  total_amount: number;
}

export interface WeightedBlanketsPoundsDetailProduct {
  asin: string;
  title?: string | null;
  brand?: string | null;
  imageurl?: string | null;
  weight?: string | null;
  dimensions?: string | null;
  sellername?: string | null;
  parent?: string | null;
  total_units: number;
  total_amount: number;
  avg_price: number | null;
  active_periods: number;
  latest_year_month?: number | null;
}

export interface WeightedBlanketsPoundsDetailSummary {
  product_count: number;
  total_units: number;
  total_amount: number;
  avg_price: number | null;
  active_periods: number;
}

export interface WeightedBlanketsPoundsDetailResponse {
  view: "yearly" | "monthly";
  year: number | null;
  month: number | null;
  pounds: number;
  pounds_label: string;
  summary: WeightedBlanketsPoundsDetailSummary | null;
  products: WeightedBlanketsPoundsDetailProduct[];
}
export interface WordFrequencyItemUpdatePayload {
  word_zh?: string | null;
  tag_label?: string | null;
  reason?: string | null;
}

export interface WordFrequencyItemUpdateResponse {
  item: Record<string, unknown>;
}

export interface WordFrequencyShieldPayload {
  word: string;
  source_scope?: "word_frequency" | "aba";
  word_zh?: string | null;
  tag_label?: string | null;
  reason?: string | null;
  shielded?: boolean;
}

export interface WordFrequencyShieldResponse {
  word: string;
  source_scope: "word_frequency" | "aba";
  word_zh?: string | null;
  tag_label?: string | null;
  reason?: string | null;
  shielded: boolean;
  updated_count: number;
}

export interface ShieldedWordFrequencyItem {
  word: string;
  source_scope?: "word_frequency" | "aba";
  word_zh?: string | null;
  tag_label?: string | null;
  reason?: string | null;
  year_month?: number | null;
  updated_at?: string | null;
}

export interface ShieldedWordFrequencyListResponse {
  items: ShieldedWordFrequencyItem[];
}
