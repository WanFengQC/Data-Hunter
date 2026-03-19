export interface CategorySummary {
  category: string;
  count: number;
  avg_content_length: number;
}

export interface ProcessedSummary {
  generated_at: string | null;
  categories: CategorySummary[];
}

export interface PgItemsResponse {
  columns: string[];
  items: Record<string, unknown>[];
  total: number;
  page: number;
  page_size: number;
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
