import apiClient from "@/api/client";
import type {
  PgFilterOption,
  PgFilterOptionsResponse,
  PgItemsResponse,
  ProcessedSummary,
  YearMonthsResponse,
} from "@/types/data";

export async function fetchProcessedSummary(): Promise<ProcessedSummary> {
  const { data } = await apiClient.get<ProcessedSummary>("/data/processed/summary");
  return data;
}

export async function triggerCrawlPipeline(): Promise<void> {
  await apiClient.post("/crawl/trigger");
}

export async function fetchPgYearMonths(): Promise<number[]> {
  const { data } = await apiClient.get<YearMonthsResponse>("/pg/year-months");
  return data.items;
}

export async function fetchPgItems(params: {
  year?: number;
  month?: number;
  page?: number;
  pageSize?: number;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  textFilters?: Record<string, string>;
  valueFilters?: Record<string, string[]>;
}): Promise<PgItemsResponse> {
  const payload: Record<string, unknown> = {
    year: params.year,
    month: params.month,
    page: params.page ?? 1,
    page_size: params.pageSize ?? 20,
    sort_by: params.sortBy,
    sort_dir: params.sortDir,
  };

  if (params.textFilters && Object.keys(params.textFilters).length > 0) {
    payload.text_filters = JSON.stringify(params.textFilters);
  }
  if (params.valueFilters && Object.keys(params.valueFilters).length > 0) {
    payload.value_filters = JSON.stringify(params.valueFilters);
  }

  const { data } = await apiClient.get<PgItemsResponse>("/pg/items", {
    params: payload,
  });
  return data;
}

export async function fetchPgFilterOptions(params: {
  column: string;
  year?: number;
  month?: number;
  keyword?: string;
  limit?: number;
  textFilters?: Record<string, string>;
  valueFilters?: Record<string, string[]>;
}): Promise<PgFilterOption[]> {
  const payload: Record<string, unknown> = {
    column: params.column,
    year: params.year,
    month: params.month,
    keyword: params.keyword,
    limit: params.limit ?? 300,
  };

  if (params.textFilters && Object.keys(params.textFilters).length > 0) {
    payload.text_filters = JSON.stringify(params.textFilters);
  }
  if (params.valueFilters && Object.keys(params.valueFilters).length > 0) {
    payload.value_filters = JSON.stringify(params.valueFilters);
  }

  const { data } = await apiClient.get<PgFilterOptionsResponse>("/pg/filter-options", {
    params: payload,
  });
  return data.items;
}
