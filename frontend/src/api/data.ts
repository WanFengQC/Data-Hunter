import apiClient from "@/api/client";
import type { ProcessedSummary } from "@/types/data";

export async function fetchProcessedSummary(): Promise<ProcessedSummary> {
  const { data } = await apiClient.get<ProcessedSummary>("/data/processed/summary");
  return data;
}

export async function triggerCrawlPipeline(): Promise<void> {
  await apiClient.post("/crawl/trigger");
}
