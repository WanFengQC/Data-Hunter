export interface CategorySummary {
  category: string;
  count: number;
  avg_content_length: number;
}

export interface ProcessedSummary {
  generated_at: string | null;
  categories: CategorySummary[];
}
