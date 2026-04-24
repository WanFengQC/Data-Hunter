<template>
  <main class="dashboard pounds-page">
    <section class="pounds-hero">
      <div>
        <div class="pounds-eyebrow">Weighted Blankets</div>
        <h2>查磅数</h2>
        <p>{{ focusDescription }}</p>
      </div>
      <div class="pounds-hero-stats">
        <article class="pounds-stat-card">
          <span class="pounds-stat-label">磅数档位</span>
          <strong>{{ loading ? "加载中..." : summary.items.length }}</strong>
        </article>
        <article class="pounds-stat-card">
          <span class="pounds-stat-label">总销量</span>
          <strong>{{ loading ? "加载中..." : formatNumber(summary.total_units) }}</strong>
        </article>
        <article class="pounds-stat-card">
          <span class="pounds-stat-label">总销售额</span>
          <strong>{{ loading ? "加载中..." : formatCurrency(summary.total_amount) }}</strong>
        </article>
      </div>
    </section>

    <section class="pounds-panel">
      <div class="pounds-panel-header">
        <h3>筛选</h3>
      </div>
      <div class="pounds-filters">
        <div class="pounds-segmented">
          <button
            type="button"
            class="pounds-segmented-btn"
            :class="{ active: datasetMode === 'weighted_blankets' }"
            @click="datasetMode = 'weighted_blankets'"
          >
            Weighted Blankets
          </button>
          <button
            type="button"
            class="pounds-segmented-btn"
            :class="{ active: datasetMode === 'stuffed_animals_child' }"
            @click="datasetMode = 'stuffed_animals_child'"
          >
            Stuffed Animals & Teddy Bears
          </button>
          <button
            type="button"
            class="pounds-segmented-btn"
            :class="{ active: datasetMode === 'animal_pillow_ca_child' }"
            @click="datasetMode = 'animal_pillow_ca_child'"
          >
            Animal & Pillow（CA）
          </button>
        </div>

        <label class="pounds-field">
          <span>年份</span>
          <select v-model.number="selectedYear">
            <option :value="0">全部</option>
            <option v-for="year in yearOptions" :key="year" :value="year">{{ year }}</option>
          </select>
        </label>

        <label class="pounds-field">
          <span>月份</span>
          <select v-model.number="selectedMonth">
            <option :value="0">全部（按年份）</option>
            <option v-for="month in monthOptions" :key="month" :value="month">
              {{ String(month).padStart(2, "0") }}
            </option>
          </select>
        </label>
      </div>
    </section>

    <section v-if="error" class="table-error">{{ error }}</section>

    <section class="pounds-panel">
      <div class="pounds-panel-header">
        <div>
          <h3>磅数分布</h3>
          <p>{{ chartSubtitle }}</p>
        </div>
        <div class="pounds-panel-tools">
          <div class="pounds-segmented">
            <button
              type="button"
              class="pounds-segmented-btn"
              :class="{ active: metricMode === 'units' }"
              @click="metricMode = 'units'"
            >
              销量
            </button>
            <button
              type="button"
              class="pounds-segmented-btn"
              :class="{ active: metricMode === 'amount' }"
              @click="metricMode = 'amount'"
            >
              销售额
            </button>
          </div>
          <span class="pounds-chip">{{ focusLabel }}</span>
        </div>
      </div>
      <div v-if="loading" class="pounds-loading">加载中...</div>
      <div v-else-if="!summary.items.length" class="pounds-empty">当前筛选下暂无可展示的磅数数据。</div>
      <ReportChart
        v-else
        :option="chartOption"
        height="620px"
        @chart-click="handleChartClick"
      />
    </section>

    <section class="pounds-panel">
      <div class="pounds-panel-header">
        <div>
          <h3>磅数摘要</h3>
          <p>显示全部磅数档位，点击任一卡片或图表扇区即可查看商品明细。</p>
        </div>
      </div>
      <div v-if="loading" class="pounds-loading">加载中...</div>
      <div v-else class="pounds-summary-grid">
        <button
          v-for="item in summaryCards"
          :key="item.pounds_label"
          type="button"
          class="pounds-summary-card"
          @click="openDetailFromItem(item)"
        >
          <div class="pounds-summary-top">
            <span class="pounds-badge">{{ item.pounds_label }} lb</span>
            <span class="pounds-summary-rank">查看详情</span>
          </div>
          <strong>{{ formatMetricValue(item) }}</strong>
          <span>{{ formatSecondaryMetricValue(item) }}</span>
          <small>{{ item.product_count }} 个商品</small>
        </button>
      </div>
    </section>

    <div v-if="detailOpen" class="pounds-modal-backdrop" @click.self="closeDetail">
      <section class="pounds-modal">
        <div class="pounds-modal-header">
          <div>
            <div class="pounds-eyebrow">Pounds Detail</div>
            <h3>{{ activePoundsLabel }}</h3>
            <p>{{ focusLabel }}</p>
          </div>
          <button type="button" class="secondary-btn" @click="closeDetail">关闭</button>
        </div>

        <div v-if="detailLoading" class="pounds-loading">加载详情中...</div>
        <template v-else-if="detailData">
          <div class="pounds-detail-stats">
            <article class="pounds-stat-card">
              <span class="pounds-stat-label">商品数</span>
              <strong>{{ detailData.summary?.product_count ?? 0 }}</strong>
            </article>
            <article class="pounds-stat-card">
              <span class="pounds-stat-label">销量</span>
              <strong>{{ formatNumber(detailData.summary?.total_units ?? 0) }}</strong>
            </article>
            <article class="pounds-stat-card">
              <span class="pounds-stat-label">销售额</span>
              <strong>{{ formatCurrency(detailData.summary?.total_amount ?? 0) }}</strong>
            </article>
          </div>

          <div class="pounds-products-table-wrap">
            <table class="pounds-products-table">
              <thead>
                <tr>
                  <th>商品</th>
                  <th>品牌</th>
                  <th>销量</th>
                  <th>销售额</th>
                  <th>重量</th>
                  <th>卖家</th>
                  <th>最近月份</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="item in detailData.products" :key="item.asin">
                  <td>
                    <div class="pounds-product-cell">
                      <img
                        v-if="item.imageurl"
                        :src="item.imageurl"
                        :alt="item.title || item.asin"
                        class="pounds-product-image"
                        loading="lazy"
                      />
                      <div class="pounds-product-copy">
                        <a
                          class="pounds-product-title"
                          :href="amazonUrl(item.asin)"
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          {{ item.title || item.asin }}
                        </a>
                        <div class="pounds-product-meta">ASIN: {{ item.asin }}</div>
                        <div v-if="item.dimensions" class="pounds-product-meta">{{ item.dimensions }}</div>
                      </div>
                    </div>
                  </td>
                  <td>{{ item.brand || "-" }}</td>
                  <td>{{ formatNumber(item.total_units) }}</td>
                  <td>{{ formatCurrency(item.total_amount) }}</td>
                  <td>{{ item.weight || "-" }}</td>
                  <td>{{ item.sellername || "-" }}</td>
                  <td>{{ formatYearMonth(item.latest_year_month) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </template>
        <div v-else class="pounds-empty">暂无详情。</div>
      </section>
    </div>
  </main>
</template>
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import type { EChartsOption } from "echarts";

import ReportChart from "@/components/ReportChart.vue";
import {
  fetchPgYearMonthsByTable,
  fetchWeightedBlanketsPoundsDetail,
  fetchWeightedBlanketsPoundsSummary,
} from "@/api/data";
import type {
  WeightedBlanketsPoundsDetailResponse,
  WeightedBlanketsPoundsSummaryItem,
  WeightedBlanketsPoundsSummaryResponse,
} from "@/types/data";

type DatasetMode = "weighted_blankets" | "stuffed_animals_child" | "animal_pillow_ca_child";

const DATASET_CONFIG: Record<DatasetMode, { table: string; label: string }> = {
  weighted_blankets: {
    table: "weighted_blankets_competitor_items",
    label: "Weighted Blankets",
  },
  stuffed_animals_child: {
    table: "stuffed_animals_teddy_bears_child_competitor_items",
    label: "Stuffed Animals & Teddy Bears",
  },
  animal_pillow_ca_child: {
    table: "animal_pillow_ca_child_competitor_items",
    label: "Animal & Pillow (CA)",
  },
};

const loading = ref(false);
const error = ref("");
const datasetMode = ref<DatasetMode>("weighted_blankets");
const metricMode = ref<"units" | "amount">("units");
const selectedYear = ref(0);
const selectedMonth = ref(0);
const availableYearMonths = ref<number[]>([]);
const summary = ref<WeightedBlanketsPoundsSummaryResponse>({
  view: "yearly",
  year: null,
  month: null,
  items: [],
  total_products: 0,
  total_units: 0,
  total_amount: 0,
});

const detailOpen = ref(false);
const detailLoading = ref(false);
const detailData = ref<WeightedBlanketsPoundsDetailResponse | null>(null);
const activePounds = ref<number | null>(null);
let summaryAbortController: AbortController | null = null;
let detailAbortController: AbortController | null = null;
let datasetLoadToken = 0;
let summaryLoadToken = 0;
let detailLoadToken = 0;
let isApplyingDatasetSelection = false;

const currentDataset = computed(() => DATASET_CONFIG[datasetMode.value]);
const effectiveViewMode = computed<"yearly" | "monthly">(() =>
  selectedMonth.value ? "monthly" : "yearly"
);
const currentBucketStep = computed<number | undefined>(() =>
  datasetMode.value === "stuffed_animals_child" || datasetMode.value === "animal_pillow_ca_child" ? 0.5 : undefined
);

const yearOptions = computed(() =>
  [...new Set(availableYearMonths.value.map((item) => Math.floor(item / 100)))].sort((a, b) => b - a)
);

const monthOptions = computed(() => {
  const months = availableYearMonths.value
    .filter((item) => !selectedYear.value || Math.floor(item / 100) === selectedYear.value)
    .map((item) => item % 100);
  return [...new Set(months)].sort((a, b) => a - b);
});

const focusLabel = computed(() => {
  if (selectedYear.value && selectedMonth.value) {
    return `${selectedYear.value}-${String(selectedMonth.value).padStart(2, "0")}`;
  }
  if (selectedYear.value) {
    return `${selectedYear.value} 年`;
  }
  return "全部时间";
});

const focusDescription = computed(() =>
  effectiveViewMode.value === "monthly"
    ? `按月份查看 ${currentDataset.value.label} 各磅数的销量和销售额分布。`
    : `按年份汇总 ${currentDataset.value.label} 各磅数的销量和销售额分布。`
);

const chartSubtitle = computed(() =>
  effectiveViewMode.value === "monthly"
    ? `按磅数查看当月${metricMode.value === "units" ? "销量" : "销售额"}占比，点击扇区查看商品详情。`
    : `按磅数查看所选年份累计${metricMode.value === "units" ? "销量" : "销售额"}占比，点击扇区查看该磅数下的商品表现。`
);

const summaryCards = computed(() =>
  [...summary.value.items].sort((a, b) => {
    const poundsA = Number(a.pounds || 0);
    const poundsB = Number(b.pounds || 0);
    return poundsA - poundsB;
  })
);

const activePoundsLabel = computed(() =>
  detailData.value?.pounds_label
    ? `${detailData.value.pounds_label} lb`
    : activePounds.value === null
      ? "-"
      : `${formatPlainNumber(activePounds.value)} lb`
);

const metricTotal = computed(() =>
  metricMode.value === "units" ? Number(summary.value.total_units || 0) : Number(summary.value.total_amount || 0)
);

const chartOption = computed<EChartsOption>(() => {
  const items = [...summary.value.items].sort((a, b) => {
    const metricDiff = metricValue(b) - metricValue(a);
    if (Math.abs(metricDiff) > 0.0001) return metricDiff;
    return Number(a.pounds || 0) - Number(b.pounds || 0);
  });
  const amounts = items.map((item) => item.total_amount);
  const minAmount = Math.min(...amounts, 0);
  const maxAmount = Math.max(...amounts, 0);
  const isUnits = metricMode.value === "units";
  return {
    tooltip: {
      trigger: "item",
      formatter: (params: { dataIndex?: number }) => {
        const item = typeof params?.dataIndex === "number" ? items[params.dataIndex] : null;
        if (!item) return "";
        return [
          `${item.pounds_label} lb`,
          `占比: ${formatPercent(metricTotal.value ? metricValue(item) / metricTotal.value : 0)}`,
          `销量: ${formatNumber(item.total_units)}`,
          `销售额: ${formatCurrency(item.total_amount)}`,
          `商品数: ${item.product_count}`,
        ].join("<br/>");
      },
    },
    legend: {
      type: "scroll",
      bottom: 8,
      left: "center",
      textStyle: {
        color: "#4a5a76",
      },
    },
    series: [
      {
        type: "pie",
        radius: ["34%", "58%"],
        center: ["50%", "40%"],
        minAngle: 3,
        clockwise: false,
        startAngle: 120,
        avoidLabelOverlap: true,
        selectedMode: false,
        itemStyle: {
          borderRadius: 8,
          borderColor: "#ffffff",
          borderWidth: 2,
        },
        label: {
          color: "#31435f",
          formatter: (params: { data?: { poundsLabel?: string; value?: number } }) => {
            const poundsLabel = params.data?.poundsLabel || "";
            const value = Number(params.data?.value || 0);
            return `${poundsLabel} lb\n${isUnits ? formatShortNumber(value) : formatCurrencyShort(value)}`;
          },
        },
        labelLine: {
          length: 18,
          length2: 18,
          maxSurfaceAngle: 70,
        },
        emphasis: {
          scale: true,
          scaleSize: 8,
        },
        data: items.map((item) => ({
          value: metricValue(item),
          name: `${item.pounds_label} lb`,
          pounds: item.pounds,
          poundsLabel: item.pounds_label,
          totalAmount: item.total_amount,
          productCount: item.product_count,
          itemStyle: {
            color: interpolateColor(item.total_amount, minAmount, maxAmount),
          },
        })),
      },
    ],
  };
});

function isCanceledError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const maybe = err as { code?: string; name?: string };
  return maybe.code === "ERR_CANCELED" || maybe.name === "CanceledError";
}

async function loadYearMonths(token: number): Promise<void> {
  const table = currentDataset.value.table;
  const months = await fetchPgYearMonthsByTable(table);
  if (token !== datasetLoadToken) return;

  availableYearMonths.value = months;
  isApplyingDatasetSelection = true;
  try {
    if (!availableYearMonths.value.length) {
      selectedYear.value = 0;
      selectedMonth.value = 0;
      return;
    }
    const latest = availableYearMonths.value[0];
    selectedYear.value = Math.floor(latest / 100);
    selectedMonth.value = 0;
  } finally {
    isApplyingDatasetSelection = false;
  }
}

async function loadSummary(): Promise<void> {
  if (effectiveViewMode.value === "monthly" && !selectedYear.value) {
    summary.value = {
      view: effectiveViewMode.value,
      year: selectedYear.value || null,
      month: selectedMonth.value || null,
      items: [],
      total_products: 0,
      total_units: 0,
      total_amount: 0,
    };
    return;
  }
  const requestToken = ++summaryLoadToken;
  summaryAbortController?.abort();
  const controller = new AbortController();
  summaryAbortController = controller;

  loading.value = true;
  error.value = "";
  try {
    const data = await fetchWeightedBlanketsPoundsSummary({
      view: effectiveViewMode.value,
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      bucketStep: currentBucketStep.value,
      table: currentDataset.value.table,
      signal: controller.signal,
    });
    if (requestToken !== summaryLoadToken) return;
    summary.value = data;
  } catch (err) {
    if (isCanceledError(err)) return;
    const message = err instanceof Error ? err.message : "加载磅数数据失败";
    error.value = message;
  } finally {
    if (requestToken === summaryLoadToken) {
      loading.value = false;
    }
  }
}

async function openDetail(pounds: number): Promise<void> {
  const requestToken = ++detailLoadToken;
  detailAbortController?.abort();
  const controller = new AbortController();
  detailAbortController = controller;

  activePounds.value = pounds;
  detailOpen.value = true;
  detailLoading.value = true;
  detailData.value = null;
  try {
    const data = await fetchWeightedBlanketsPoundsDetail({
      pounds,
      view: effectiveViewMode.value,
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      bucketStep: currentBucketStep.value,
      table: currentDataset.value.table,
      signal: controller.signal,
    });
    if (requestToken !== detailLoadToken) return;
    detailData.value = data;
  } catch (err) {
    if (!isCanceledError(err)) {
      detailData.value = null;
    }
  } finally {
    if (requestToken === detailLoadToken) {
      detailLoading.value = false;
    }
  }
}

function openDetailFromItem(item: WeightedBlanketsPoundsSummaryItem): void {
  if (!item.pounds) return;
  void openDetail(item.pounds);
}

function metricValue(item: WeightedBlanketsPoundsSummaryItem): number {
  return metricMode.value === "units" ? Number(item.total_units || 0) : Number(item.total_amount || 0);
}

function formatMetricValue(item: WeightedBlanketsPoundsSummaryItem): string {
  return metricMode.value === "units" ? formatNumber(item.total_units) : formatCurrency(item.total_amount);
}

function formatSecondaryMetricValue(item: WeightedBlanketsPoundsSummaryItem): string {
  return metricMode.value === "units" ? formatCurrency(item.total_amount) : formatNumber(item.total_units);
}

function closeDetail(): void {
  detailAbortController?.abort();
  detailOpen.value = false;
  detailData.value = null;
  activePounds.value = null;
}

function handleChartClick(params: unknown): void {
  if (typeof params !== "object" || !params) return;
  const pounds =
    "data" in params && params.data && typeof params.data === "object" && "pounds" in params.data
      ? Number(params.data.pounds)
      : Number.NaN;
  if (!Number.isFinite(pounds)) return;
  void openDetail(pounds);
}

function formatNumber(value: number | null | undefined): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Number(value || 0));
}

function formatPlainNumber(value: number | null | undefined): string {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(Number(value || 0));
}

function formatShortNumber(value: number | null | undefined): string {
  const amount = Number(value || 0);
  if (Math.abs(amount) >= 1_000_000) return `${(amount / 1_000_000).toFixed(1)}M`;
  if (Math.abs(amount) >= 1_000) return `${(amount / 1_000).toFixed(1)}K`;
  return `${Math.round(amount)}`;
}

function formatPercent(value: number | null | undefined): string {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function formatCurrency(value: number | null | undefined): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(Number(value || 0));
}

function formatCurrencyShort(value: number | null | undefined): string {
  const amount = Number(value || 0);
  if (Math.abs(amount) >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`;
  if (Math.abs(amount) >= 1_000) return `$${(amount / 1_000).toFixed(1)}K`;
  return formatCurrency(amount);
}

function formatYearMonth(value: number | null | undefined): string {
  if (!value) return "-";
  const year = Math.floor(value / 100);
  const month = value % 100;
  return `${year}-${String(month).padStart(2, "0")}`;
}

function amazonUrl(asin: string): string {
  return `https://www.amazon.com/dp/${asin}`;
}

function interpolateColor(value: number, min: number, max: number): string {
  if (max <= min) return "#2f6df6";
  const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
  const start = { r: 169, g: 209, b: 255 };
  const end = { r: 38, g: 92, b: 214 };
  const r = Math.round(start.r + (end.r - start.r) * ratio);
  const g = Math.round(start.g + (end.g - start.g) * ratio);
  const b = Math.round(start.b + (end.b - start.b) * ratio);
  return `rgb(${r}, ${g}, ${b})`;
}

watch(selectedYear, () => {
  if (isApplyingDatasetSelection) return;
  if (!selectedYear.value && selectedMonth.value) {
    selectedMonth.value = 0;
    return;
  }
  if (selectedMonth.value && !monthOptions.value.includes(selectedMonth.value)) {
    selectedMonth.value = 0;
  }
  void loadSummary();
});

watch(selectedMonth, () => {
  if (isApplyingDatasetSelection) return;
  void loadSummary();
});

watch(datasetMode, () => {
  datasetLoadToken += 1;
  const token = datasetLoadToken;
  summaryAbortController?.abort();
  loading.value = true;
  closeDetail();
  void (async () => {
    await loadYearMonths(token);
    if (token !== datasetLoadToken) return;
    await loadSummary();
  })();
});

onMounted(() => {
  datasetLoadToken += 1;
  const token = datasetLoadToken;
  void (async () => {
    await loadYearMonths(token);
    if (token !== datasetLoadToken) return;
    await loadSummary();
  })();
});

onBeforeUnmount(() => {
  summaryAbortController?.abort();
  detailAbortController?.abort();
});
</script>

<style scoped>
.pounds-page {
  padding-top: 1rem;
}

.pounds-hero,
.pounds-panel {
  width: min(1760px, 96vw);
  margin: 0 auto 1rem;
}

.pounds-hero {
  display: grid;
  grid-template-columns: 1.4fr 1fr;
  gap: 1rem;
  padding: 1.25rem 1.4rem;
  border-radius: 22px;
  background:
    radial-gradient(circle at top left, rgba(255, 222, 173, 0.85), transparent 32%),
    linear-gradient(135deg, #f7fbff 0%, #eef4ff 45%, #fffaf2 100%);
  border: 1px solid rgba(210, 223, 245, 0.95);
  box-shadow: 0 20px 44px rgba(25, 46, 89, 0.08);
}

.pounds-eyebrow {
  font-size: 0.78rem;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: #6b7f99;
}

.pounds-hero h2,
.pounds-panel h3 {
  margin: 0.25rem 0 0;
  color: #11233f;
}

.pounds-hero p,
.pounds-panel-header p {
  margin: 0.4rem 0 0;
  color: #607089;
}

.pounds-hero-stats,
.pounds-detail-stats,
.pounds-summary-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.9rem;
}

.pounds-detail-stats {
  margin-bottom: 1rem;
}

.pounds-summary-grid {
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
}

.pounds-stat-card,
.pounds-summary-card {
  padding: 1rem;
  border-radius: 16px;
  background: rgba(255, 255, 255, 0.88);
  border: 1px solid #dde7f8;
}

.pounds-summary-card {
  appearance: none;
  text-align: left;
  cursor: pointer;
  transition:
    transform 0.16s ease,
    box-shadow 0.16s ease,
    border-color 0.16s ease;
}

.pounds-summary-card:hover {
  transform: translateY(-2px);
  border-color: #b9cff5;
  box-shadow: 0 14px 26px rgba(29, 78, 216, 0.08);
}

.pounds-stat-card strong,
.pounds-summary-card strong {
  display: block;
  margin-top: 0.35rem;
  font-size: 1.4rem;
  color: #143260;
}

.pounds-stat-label,
.pounds-summary-card small,
.pounds-summary-card span {
  color: #6a7b95;
}

.pounds-summary-card span {
  display: block;
  margin-top: 0.4rem;
}

.pounds-summary-card small {
  display: block;
  margin-top: 0.28rem;
}

.pounds-panel {
  padding: 1.2rem 1.3rem;
  border-radius: 20px;
  background: #fff;
  border: 1px solid #dfe8f7;
  box-shadow: 0 14px 34px rgba(20, 38, 74, 0.06);
}

.pounds-panel-header {
  display: flex;
  align-items: start;
  justify-content: space-between;
  gap: 1rem;
  margin-bottom: 1rem;
}

.pounds-panel-tools {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 0.75rem;
  flex-wrap: wrap;
}

.pounds-filters {
  display: flex;
  flex-wrap: wrap;
  gap: 0.9rem;
  align-items: end;
}

.pounds-field {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
  min-width: 120px;
  color: #41536f;
  font-size: 0.9rem;
}

.pounds-field select {
  height: 38px;
  border-radius: 10px;
  border: 1px solid #cad7ef;
  background: #fff;
  padding: 0 0.8rem;
}

.pounds-segmented {
  display: inline-flex;
  padding: 4px;
  background: #eff4fd;
  border-radius: 999px;
  border: 1px solid #d8e4f6;
}

.pounds-segmented-btn {
  border: 0;
  background: transparent;
  color: #5b6d88;
  padding: 0.52rem 1rem;
  border-radius: 999px;
  font-weight: 700;
  cursor: pointer;
}

.pounds-segmented-btn.active {
  background: linear-gradient(135deg, #2f6df6, #1f56c8);
  color: #fff;
  box-shadow: 0 8px 18px rgba(47, 109, 246, 0.25);
}

.pounds-chip,
.pounds-badge,
.pounds-summary-rank {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0.28rem 0.65rem;
  border-radius: 999px;
  font-size: 0.78rem;
  font-weight: 700;
}

.pounds-chip {
  color: #2254be;
  background: #edf4ff;
  border: 1px solid #cfe0ff;
}

.pounds-badge {
  color: #8c4308;
  background: #fff3e7;
}

.pounds-summary-rank {
  color: #2050ab;
  background: #eef4ff;
}

.pounds-summary-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.6rem;
  margin-bottom: 0.6rem;
}

.pounds-loading,
.pounds-empty {
  min-height: 120px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #5f708c;
}

.pounds-modal-backdrop {
  position: fixed;
  inset: 0;
  z-index: 90;
  display: flex;
  align-items: stretch;
  justify-content: flex-end;
  background: rgba(15, 23, 42, 0.3);
  backdrop-filter: blur(3px);
}

.pounds-modal {
  width: min(1180px, 92vw);
  height: 100vh;
  overflow: auto;
  background: #fff;
  padding: 1.2rem 1.2rem 1.6rem;
  box-shadow: -18px 0 42px rgba(15, 23, 42, 0.18);
}

.pounds-modal-header {
  display: flex;
  justify-content: space-between;
  gap: 1rem;
  align-items: start;
  margin-bottom: 1rem;
}

.pounds-products-table-wrap {
  overflow: auto;
  border: 1px solid #dfe8f7;
  border-radius: 16px;
}

.pounds-products-table {
  width: 100%;
  border-collapse: collapse;
}

.pounds-products-table th,
.pounds-products-table td {
  padding: 0.85rem 0.8rem;
  border-bottom: 1px solid #edf2fb;
  text-align: left;
  vertical-align: top;
  color: #30425d;
  font-size: 0.9rem;
}

.pounds-products-table th {
  position: sticky;
  top: 0;
  background: #f8fbff;
  color: #446087;
}

.pounds-product-cell {
  display: flex;
  gap: 0.75rem;
  min-width: 320px;
}

.pounds-product-image {
  width: 64px;
  height: 64px;
  object-fit: cover;
  border-radius: 10px;
  border: 1px solid #e1e8f3;
  background: #fff;
  flex: 0 0 auto;
}

.pounds-product-copy {
  min-width: 0;
}

.pounds-product-title {
  color: #1d4ed8;
  text-decoration: none;
  font-weight: 700;
}

.pounds-product-title:hover {
  color: #f97316;
}

.pounds-product-meta {
  margin-top: 0.25rem;
  color: #6b7b93;
  font-size: 0.82rem;
}

@media (max-width: 1100px) {
  .pounds-hero {
    grid-template-columns: 1fr;
  }

  .pounds-hero-stats,
  .pounds-detail-stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 720px) {
  .pounds-hero-stats,
  .pounds-detail-stats {
    grid-template-columns: 1fr;
  }

  .pounds-modal {
    width: 100vw;
  }
}
</style>


