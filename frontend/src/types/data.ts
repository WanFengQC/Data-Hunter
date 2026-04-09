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
