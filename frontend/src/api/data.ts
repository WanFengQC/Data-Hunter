import apiClient from "@/api/client";
import type {
  PgFilterOption,
  PgFilterOptionsResponse,
  PgItemsResponse,
  ProcessedSummary,
  WordFrequencyTrendResponse,
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

export async function fetchPgYearMonthsByTable(table?: string): Promise<number[]> {
  const payload: Record<string, unknown> = {};
  if (table) payload.table = table;
  const { data } = await apiClient.get<YearMonthsResponse>("/pg/year-months", {
    params: payload,
  });
  return data.items;
}

export async function fetchWordFrequencyTrend(word: string): Promise<WordFrequencyTrendResponse> {
  const { data } = await apiClient.get<WordFrequencyTrendResponse>("/pg/word-frequency-trend", {
    params: { word },
  });
  return data;
}

export async function fetchPgGrowthTop10(params: {
  mode: "monthly" | "quarterly" | "searches";
  year?: number;
  month?: number;
  searchMin?: number;
  searchMax?: number;
  table?: string;
  limit?: number;
}): Promise<PgItemsResponse> {
  const payload: Record<string, unknown> = {
    mode: params.mode,
    year: params.year,
    month: params.month,
    search_min: params.searchMin,
    search_max: params.searchMax,
    table: params.table,
    limit: params.limit ?? 10,
  };
  const { data } = await apiClient.get<PgItemsResponse>("/pg/growth-top10", {
    params: payload,
  });
  return data;
}

export async function fetchPgItems(params: {
  year?: number;
  month?: number;
  page?: number;
  pageSize?: number;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  textFilters?: Record<string, unknown>;
  valueFilters?: Record<string, string[]>;
  table?: string;
}): Promise<PgItemsResponse> {
  const payload: Record<string, unknown> = {
    year: params.year,
    month: params.month,
    page: params.page ?? 1,
    page_size: params.pageSize ?? 20,
    sort_by: params.sortBy,
    sort_dir: params.sortDir,
    table: params.table,
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

export async function exportPgItemsCsv(params: {
  year?: number;
  month?: number;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  textFilters?: Record<string, unknown>;
  valueFilters?: Record<string, string[]>;
  table?: string;
}): Promise<Blob> {
  const payload: Record<string, unknown> = {
    year: params.year,
    month: params.month,
    sort_by: params.sortBy,
    sort_dir: params.sortDir,
    table: params.table,
  };

  if (params.textFilters && Object.keys(params.textFilters).length > 0) {
    payload.text_filters = JSON.stringify(params.textFilters);
  }
  if (params.valueFilters && Object.keys(params.valueFilters).length > 0) {
    payload.value_filters = JSON.stringify(params.valueFilters);
  }

  const { data } = await apiClient.get<Blob>("/pg/export-csv", {
    params: payload,
    responseType: "blob",
    timeout: 0,
  });
  return data;
}

export interface PgExportJob {
  job_id: string;
  status: "pending" | "running" | "completed" | "failed";
  created_at: string;
  updated_at: string;
  file_name?: string | null;
  file_size?: number | null;
  error?: string | null;
}

export async function createPgExportJob(params: {
  year?: number;
  month?: number;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  textFilters?: Record<string, unknown>;
  valueFilters?: Record<string, string[]>;
  table?: string;
}): Promise<PgExportJob> {
  const payload: Record<string, unknown> = {
    year: params.year,
    month: params.month,
    sort_by: params.sortBy,
    sort_dir: params.sortDir,
    table: params.table,
  };

  if (params.textFilters && Object.keys(params.textFilters).length > 0) {
    payload.text_filters = JSON.stringify(params.textFilters);
  }
  if (params.valueFilters && Object.keys(params.valueFilters).length > 0) {
    payload.value_filters = JSON.stringify(params.valueFilters);
  }

  const { data } = await apiClient.post<PgExportJob>("/pg/export-csv/jobs", null, {
    params: payload,
  });
  return data;
}

export async function fetchPgExportJob(jobId: string): Promise<PgExportJob> {
  const { data } = await apiClient.get<PgExportJob>(`/pg/export-csv/jobs/${jobId}`);
  return data;
}

export async function downloadPgExportJob(jobId: string): Promise<Blob> {
  const { data } = await apiClient.get<Blob>(`/pg/export-csv/jobs/${jobId}/download`, {
    responseType: "blob",
    timeout: 0,
  });
  return data;
}

export async function fetchPgFilterOptions(params: {
  column: string;
  year?: number;
  month?: number;
  keyword?: string;
  limit?: number;
  textFilters?: Record<string, unknown>;
  valueFilters?: Record<string, string[]>;
  table?: string;
}): Promise<PgFilterOption[]> {
  const payload: Record<string, unknown> = {
    column: params.column,
    year: params.year,
    month: params.month,
    keyword: params.keyword,
    limit: params.limit ?? 300,
    table: params.table,
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
