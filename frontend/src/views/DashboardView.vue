<template>
  <main class="dashboard">
    <section class="dashboard-header">
      <h1>Data Hunter Dashboard</h1>
      <p>采集、处理和分析结果总览</p>
      <button class="primary-btn" @click="handleTrigger" :disabled="loading">
        {{ loading ? "处理中..." : "触发采集与处理" }}
      </button>
    </section>

    <section class="stats-grid">
      <StatCard title="分类数量" :value="summary.categories.length" subtitle="当前快照分类" />
      <StatCard title="总记录数" :value="totalCount" subtitle="processed 总数" />
      <StatCard title="最近生成时间" :value="generatedAt" subtitle="analytics_snapshots" />
    </section>

    <section class="chart-panel">
      <h2>类别分布与平均文本长度</h2>
      <TrendChart :rows="summary.categories" />
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";

import { fetchProcessedSummary, triggerCrawlPipeline } from "@/api/data";
import StatCard from "@/components/StatCard.vue";
import TrendChart from "@/components/TrendChart.vue";
import type { ProcessedSummary } from "@/types/data";

const loading = ref(false);
const summary = ref<ProcessedSummary>({ generated_at: null, categories: [] });

const totalCount = computed(() =>
  summary.value.categories.reduce((sum, row) => sum + row.count, 0),
);

const generatedAt = computed(() => {
  if (!summary.value.generated_at) return "暂无";
  return new Date(summary.value.generated_at).toLocaleString();
});

async function loadSummary(): Promise<void> {
  summary.value = await fetchProcessedSummary();
}

async function handleTrigger(): Promise<void> {
  loading.value = true;
  try {
    await triggerCrawlPipeline();
    await loadSummary();
  } finally {
    loading.value = false;
  }
}

onMounted(async () => {
  await loadSummary();
});
</script>
