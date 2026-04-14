import apiClient from "@/api/client";
import type {
  PgAllItemsResponse,
  PgFilterOption,
  PgFilterOptionsResponse,
  PgItemsResponse,
  ShieldedWordFrequencyListResponse,
  WordFrequencyShieldPayload,
  WordFrequencyShieldResponse,
  WordFrequencyItemUpdatePayload,
  WordFrequencyItemUpdateResponse,
  WeightedBlanketsPoundsDetailResponse,
  WeightedBlanketsPoundsSummaryResponse,
  WordFrequencyTrendResponse,
  YearMonthsResponse,
} from "@/types/data";

const YEAR_MONTHS_TIMEOUT_MS = 120000;
const YEAR_MONTHS_CACHE_TTL_MS = 5 * 60 * 1000;
const YEAR_MONTHS_RETRY_DELAYS_MS = [1000, 2000, 4000];
const YEAR_MONTHS_CACHE_KEY = "__default__";

type YearMonthsCacheEntry = {
  items: number[];
  expiresAt: number;
};

const yearMonthsCache = new Map<string, YearMonthsCacheEntry>();
const yearMonthsInFlight = new Map<string, Promise<number[]>>();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function getYearMonthsCacheKey(table?: string): string {
  const normalized = String(table || "").trim();
  return normalized || YEAR_MONTHS_CACHE_KEY;
}

async function requestPgYearMonths(table?: string): Promise<number[]> {
  const payload: Record<string, unknown> = {};
  if (table) payload.table = table;
  const { data } = await apiClient.get<YearMonthsResponse>("/pg/year-months", {
    params: payload,
    timeout: YEAR_MONTHS_TIMEOUT_MS,
  });
  return Array.isArray(data.items) ? data.items : [];
}

export async function fetchPgYearMonths(): Promise<number[]> {
  return fetchPgYearMonthsByTable();
}

export async function fetchPgYearMonthsByTable(table?: string): Promise<number[]> {
  const cacheKey = getYearMonthsCacheKey(table);
  const now = Date.now();
  const cached = yearMonthsCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return [...cached.items];
  }

  const inFlight = yearMonthsInFlight.get(cacheKey);
  if (inFlight) {
    return inFlight;
  }

  const task = (async () => {
    let lastError: unknown = null;
    for (let attempt = 0; attempt <= YEAR_MONTHS_RETRY_DELAYS_MS.length; attempt += 1) {
      try {
        const items = await requestPgYearMonths(table);
        yearMonthsCache.set(cacheKey, {
          items: [...items],
          expiresAt: Date.now() + YEAR_MONTHS_CACHE_TTL_MS,
        });
        return [...items];
      } catch (error) {
        lastError = error;
        if (attempt >= YEAR_MONTHS_RETRY_DELAYS_MS.length) {
          break;
        }
        await sleep(YEAR_MONTHS_RETRY_DELAYS_MS[attempt]);
      }
    }

    if (cached) {
      console.warn("fetchPgYearMonthsByTable failed, use stale cache:", table, lastError);
      return [...cached.items];
    }
    throw lastError;
  })();

  yearMonthsInFlight.set(cacheKey, task);
  try {
    return await task;
  } finally {
    yearMonthsInFlight.delete(cacheKey);
  }
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
  page?: number;
  pageSize?: number;
}): Promise<PgItemsResponse> {
  const payload: Record<string, unknown> = {
    mode: params.mode,
    year: params.year,
    month: params.month,
    search_min: params.searchMin,
    search_max: params.searchMax,
    table: params.table,
    page: params.page ?? 1,
    page_size: params.pageSize ?? 10,
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
  signal?: AbortSignal;
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
    signal: params.signal,
  });
  return data;
}

export async function fetchPgAllItems(params: {
  year?: number;
  month?: number;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  textFilters?: Record<string, unknown>;
  valueFilters?: Record<string, string[]>;
  table?: string;
  columns?: string[];
}): Promise<PgAllItemsResponse> {
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
  if (params.columns && params.columns.length > 0) {
    payload.columns = params.columns.join(",");
  }

  const { data } = await apiClient.get<PgAllItemsResponse>("/pg/items-all", {
    params: payload,
    timeout: 0,
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

export async function fetchWeightedBlanketsPoundsSummary(params: {
  view: "yearly" | "monthly";
  year?: number;
  month?: number;
  table?: string;
}): Promise<WeightedBlanketsPoundsSummaryResponse> {
  const { data } = await apiClient.get<WeightedBlanketsPoundsSummaryResponse>(
    "/pg/weighted-blankets/pounds-summary",
    {
      params: {
        view: params.view,
        year: params.year,
        month: params.month,
        table: params.table,
      },
    }
  );
  return data;
}

export async function fetchWeightedBlanketsPoundsDetail(params: {
  pounds: number;
  view: "yearly" | "monthly";
  year?: number;
  month?: number;
  limit?: number;
  table?: string;
}): Promise<WeightedBlanketsPoundsDetailResponse> {
  const { data } = await apiClient.get<WeightedBlanketsPoundsDetailResponse>(
    "/pg/weighted-blankets/pounds-detail",
    {
      params: {
        pounds: params.pounds,
        view: params.view,
        year: params.year,
        month: params.month,
        limit: params.limit ?? 100,
        table: params.table,
      },
    }
  );
  return data;
}
export async function updateWordFrequencyItem(params: {
  itemId: number;
  table?: string;
  payload: WordFrequencyItemUpdatePayload;
}): Promise<WordFrequencyItemUpdateResponse> {
  const { data } = await apiClient.put<WordFrequencyItemUpdateResponse>(
    `/pg/word-frequency-items/${params.itemId}`,
    params.payload,
    {
      params: {
        table: params.table,
      },
    }
  );
  return data;
}

export async function shieldWordFrequencyItems(params: {
  table?: string;
  payload: WordFrequencyShieldPayload;
}): Promise<WordFrequencyShieldResponse> {
  const { data } = await apiClient.put<WordFrequencyShieldResponse>("/pg/word-frequency-shield", params.payload, {
    params: {
      table: params.table,
    },
  });
  return data;
}

export async function fetchShieldedWordFrequencyItems(params: {
  table?: string;
  limit?: number;
  sourceScope?: "word_frequency" | "aba";
} = {}): Promise<ShieldedWordFrequencyListResponse> {
  const { data } = await apiClient.get<ShieldedWordFrequencyListResponse>("/pg/word-frequency-shield", {
    params: {
      table: params.table,
      limit: params.limit ?? 500,
      source_scope: params.sourceScope,
    },
  });
  return data;
}
