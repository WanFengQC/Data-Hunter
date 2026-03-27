<template>
  <main class="dashboard competitor-report-page">
    <section class="report-hero">
      <div>
        <div class="report-eyebrow">类目竞品报告</div>
        <h2>{{ reportTitle }}</h2>
        <p>{{ reportSubtitle }}</p>
      </div>
      <div class="report-actions">
        <button class="secondary-btn" type="button" @click="goBack">返回竞品表</button>
      </div>
    </section>

    <section v-if="loading" class="report-loading-panel">
      <div class="report-loading-title">报告生成中...</div>
      <div class="report-loading-subtitle">{{ loadingText }}</div>
    </section>

    <section v-else-if="error" class="table-error">{{ error }}</section>

    <template v-else>
      <section class="report-stats-grid">
        <article v-for="card in statCards" :key="card.label" class="report-stat-card">
          <div class="report-stat-label">{{ card.label }}</div>
          <div class="report-stat-value">{{ card.value }}</div>
          <div class="report-stat-sub">{{ card.sub }}</div>
        </article>
      </section>

      <section class="report-panel">
        <div class="report-panel-header">
          <h3>类目概览</h3>
        </div>
        <div class="report-insights">
          <div v-for="item in overviewInsights" :key="item" class="report-insight">{{ item }}</div>
        </div>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel">
          <div class="report-panel-header">
            <h3>行业销售趋势</h3>
          </div>
          <ReportChart :option="salesTrendOption" height="320px" />
        </article>

        <article class="report-panel">
          <div class="report-panel-header">
            <h3>卖家国家分布</h3>
          </div>
          <ReportChart :option="sellerNationOption" height="320px" />
        </article>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel">
          <div class="report-panel-header">
            <h3>价格分布</h3>
          </div>
          <ReportChart :option="priceDistributionOption" height="320px" />
        </article>

        <article class="report-panel">
          <div class="report-panel-header">
            <h3>评分分布</h3>
          </div>
          <ReportChart :option="ratingDistributionOption" height="320px" />
        </article>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel">
          <div class="report-panel-header">
            <h3>新品分析</h3>
          </div>
          <div class="report-mini-grid">
            <div class="report-mini-card">
              <span>新品数量</span>
              <strong>{{ formatInteger(metrics.newItemCount) }}</strong>
            </div>
            <div class="report-mini-card">
              <span>新品占比</span>
              <strong>{{ formatPercent(metrics.newItemRatio) }}</strong>
            </div>
            <div class="report-mini-card">
              <span>新品均价</span>
              <strong>{{ formatCurrency(metrics.newItemAvgPrice) }}</strong>
            </div>
            <div class="report-mini-card">
              <span>新品平均评分</span>
              <strong>{{ formatDecimal(metrics.newItemAvgRating, 2) }}</strong>
            </div>
          </div>
          <ReportChart :option="listingAgeOption" height="280px" />
        </article>

        <article class="report-panel">
          <div class="report-panel-header">
            <h3>卖家结构与内容质量</h3>
          </div>
          <div class="report-mini-grid">
            <div class="report-mini-card">
              <span>FBA占比</span>
              <strong>{{ formatPercent(metrics.fbaRatio) }}</strong>
            </div>
            <div class="report-mini-card">
              <span>视频占比</span>
              <strong>{{ formatPercent(metrics.videoRatio) }}</strong>
            </div>
            <div class="report-mini-card">
              <span>A+占比</span>
              <strong>{{ formatPercent(metrics.ebcRatio) }}</strong>
            </div>
            <div class="report-mini-card">
              <span>平均卖家数</span>
              <strong>{{ formatDecimal(metrics.avgSellers, 1) }}</strong>
            </div>
          </div>
          <ReportChart :option="sellerTypeOption" height="280px" />
        </article>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel">
          <div class="report-panel-header">
            <h3>品牌集中度</h3>
          </div>
          <div class="report-share-strip">
            <span>Top10品牌销售额占比</span>
            <strong>{{ formatPercent(metrics.brandTop10Share) }}</strong>
          </div>
          <table class="report-table">
            <thead>
              <tr>
                <th>品牌</th>
                <th>商品数</th>
                <th>销售额</th>
                <th>销量</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in topBrands" :key="`brand-${item.name}`">
                <td>{{ item.name }}</td>
                <td>{{ formatInteger(item.count) }}</td>
                <td>{{ formatCurrency(item.amount) }}</td>
                <td>{{ formatInteger(item.units) }}</td>
              </tr>
            </tbody>
          </table>
        </article>

        <article class="report-panel">
          <div class="report-panel-header">
            <h3>店铺集中度</h3>
          </div>
          <div class="report-share-strip">
            <span>Top10卖家销售额占比</span>
            <strong>{{ formatPercent(metrics.sellerTop10Share) }}</strong>
          </div>
          <table class="report-table">
            <thead>
              <tr>
                <th>卖家</th>
                <th>商品数</th>
                <th>销售额</th>
                <th>销量</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in topSellers" :key="`seller-${item.name}`">
                <td>{{ item.name }}</td>
                <td>{{ formatInteger(item.count) }}</td>
                <td>{{ formatCurrency(item.amount) }}</td>
                <td>{{ formatInteger(item.units) }}</td>
              </tr>
            </tbody>
          </table>
        </article>
      </section>
    </template>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import type { EChartsOption } from "echarts";

import ReportChart from "@/components/ReportChart.vue";
import { fetchPgItems } from "@/api/data";
import { parsePgTrendPoints, formatTrendLabel } from "@/utils/trend";

type ItemRow = Record<string, unknown>;
type RankedSummary = { name: string; count: number; units: number; amount: number };
type Metrics = {
  totalProducts: number;
  totalBrands: number;
  totalSellers: number;
  totalUnits: number;
  totalAmount: number;
  avgPrice: number | null;
  avgRating: number | null;
  avgReviews: number | null;
  avgSellers: number | null;
  videoRatio: number;
  ebcRatio: number;
  fbaRatio: number;
  newItemCount: number;
  newItemRatio: number;
  newItemAvgPrice: number | null;
  newItemAvgRating: number | null;
  brandTop10Share: number;
  sellerTop10Share: number;
};

const route = useRoute();
const router = useRouter();

const loading = ref(true);
const loadingText = ref("准备数据...");
const error = ref("");
const rows = ref<ItemRow[]>([]);

const selectedYear = computed(() => {
  const raw = Number(route.query.year || 0);
  return Number.isFinite(raw) ? raw : 0;
});
const selectedMonth = computed(() => {
  const raw = Number(route.query.month || 0);
  return Number.isFinite(raw) ? raw : 0;
});

const reportTitle = computed(() => {
  if (selectedYear.value && selectedMonth.value) {
    return `${selectedYear.value}年${String(selectedMonth.value).padStart(2, "0")}月 竞品类目报告`;
  }
  if (selectedYear.value) return `${selectedYear.value}年 竞品类目报告`;
  return "竞品类目报告";
});
const reportSubtitle = computed(() => `基于当前竞品库自动生成，样本商品 ${formatInteger(rows.value.length)} 个。`);

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function toText(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function average(values: Array<number | null>): number | null {
  const filtered = values.filter((item): item is number => typeof item === "number" && Number.isFinite(item));
  if (!filtered.length) return null;
  return filtered.reduce((sum, item) => sum + item, 0) / filtered.length;
}

function topSummary(rowsInput: ItemRow[], field: string): RankedSummary[] {
  const grouped = new Map<string, RankedSummary>();
  for (const row of rowsInput) {
    const name = toText(row[field]) || "未知";
    const current = grouped.get(name) || { name, count: 0, units: 0, amount: 0 };
    current.count += 1;
    current.units += toNumber(row.totalunits) || 0;
    current.amount += toNumber(row.totalamount) || 0;
    grouped.set(name, current);
  }
  return Array.from(grouped.values()).sort((a, b) => b.amount - a.amount).slice(0, 10);
}

function aggregateTrend(rowsInput: ItemRow[]): Array<{ label: string; value: number }> {
  const grouped = new Map<string, number>();
  for (const row of rowsInput) {
    const points = parsePgTrendPoints(row.trends || row.salesTrend);
    for (const point of points) {
      const label = formatTrendLabel(point.label);
      const current = grouped.get(label) || 0;
      grouped.set(label, current + (point.searches || 0));
    }
  }
  return Array.from(grouped.entries())
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => a.label.localeCompare(b.label));
}

function buildBuckets(values: number[], edges: number[], labels: string[]): Array<{ label: string; value: number }> {
  const counts = labels.map((label) => ({ label, value: 0 }));
  for (const value of values) {
    for (let i = 0; i < edges.length - 1; i += 1) {
      if (value >= edges[i] && value < edges[i + 1]) {
        counts[i].value += 1;
        break;
      }
    }
  }
  return counts;
}

function groupCount(rowsInput: ItemRow[], field: string, limit = 8): Array<{ label: string; value: number }> {
  const grouped = new Map<string, number>();
  for (const row of rowsInput) {
    const key = toText(row[field]) || "未知";
    grouped.set(key, (grouped.get(key) || 0) + 1);
  }
  return Array.from(grouped.entries())
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => b.value - a.value)
    .slice(0, limit);
}

const topBrands = computed(() => topSummary(rows.value, "brand"));
const topSellers = computed(() => topSummary(rows.value, "sellername"));

const metrics = computed<Metrics>(() => {
  const items = rows.value;
  const totalProducts = items.length;
  const totalAmount = items.reduce((sum, row) => sum + (toNumber(row.totalamount) || 0), 0);
  const totalUnits = items.reduce((sum, row) => sum + (toNumber(row.totalunits) || 0), 0);
  const totalBrands = new Set(items.map((row) => toText(row.brand)).filter(Boolean)).size;
  const totalSellers = new Set(items.map((row) => toText(row.sellername)).filter(Boolean)).size;
  const avgPrice = average(items.map((row) => toNumber(row.price)));
  const avgRating = average(items.map((row) => toNumber(row.rating)));
  const avgReviews = average(items.map((row) => toNumber(row.reviews)));
  const avgSellers = average(items.map((row) => toNumber(row.sellers)));
  const videoRatio = totalProducts ? items.filter((row) => toText(row.video).toUpperCase() === "Y").length / totalProducts : 0;
  const ebcRatio = totalProducts ? items.filter((row) => toText(row.ebc).toUpperCase() === "Y").length / totalProducts : 0;
  const fbaRatio = totalProducts ? items.filter((row) => toText(row.sellertype).toUpperCase() === "FBA").length / totalProducts : 0;
  const newItems = items.filter((row) => {
    const days = toNumber(row.availabledays);
    return typeof days === "number" && days <= 180;
  });
  const brandTop10Amount = topBrands.value.reduce((sum, item) => sum + item.amount, 0);
  const sellerTop10Amount = topSellers.value.reduce((sum, item) => sum + item.amount, 0);

  return {
    totalProducts,
    totalBrands,
    totalSellers,
    totalUnits,
    totalAmount,
    avgPrice,
    avgRating,
    avgReviews,
    avgSellers,
    videoRatio,
    ebcRatio,
    fbaRatio,
    newItemCount: newItems.length,
    newItemRatio: totalProducts ? newItems.length / totalProducts : 0,
    newItemAvgPrice: average(newItems.map((row) => toNumber(row.price))),
    newItemAvgRating: average(newItems.map((row) => toNumber(row.rating))),
    brandTop10Share: totalAmount ? brandTop10Amount / totalAmount : 0,
    sellerTop10Share: totalAmount ? sellerTop10Amount / totalAmount : 0,
  };
});

const statCards = computed(() => [
  { label: "样本商品数", value: formatInteger(metrics.value.totalProducts), sub: `品牌数 ${formatInteger(metrics.value.totalBrands)}` },
  { label: "总销量", value: formatInteger(metrics.value.totalUnits), sub: `销售额 ${formatCurrency(metrics.value.totalAmount)}` },
  { label: "均价", value: formatCurrency(metrics.value.avgPrice), sub: `平均评分 ${formatDecimal(metrics.value.avgRating, 2)}` },
  { label: "评论均值", value: formatDecimal(metrics.value.avgReviews, 0), sub: `平均卖家数 ${formatDecimal(metrics.value.avgSellers, 1)}` },
]);

const overviewInsights = computed(() => {
  const priceBuckets = priceDistribution.value;
  const topPrice = [...priceBuckets].sort((a, b) => b.value - a.value)[0];
  const topNation = sellerNationDistribution.value[0];
  return [
    `样本池共 ${formatInteger(metrics.value.totalProducts)} 个商品，累计销售额 ${formatCurrency(metrics.value.totalAmount)}。`,
    `主流价格带集中在 ${topPrice?.label || "-"}，均价约 ${formatCurrency(metrics.value.avgPrice)}。`,
    `卖家国家以 ${topNation?.label || "-"} 为主，FBA 占比 ${formatPercent(metrics.value.fbaRatio)}。`,
    `视频占比 ${formatPercent(metrics.value.videoRatio)}，A+ 占比 ${formatPercent(metrics.value.ebcRatio)}。`,
  ];
});

const salesTrend = computed(() => aggregateTrend(rows.value));
const sellerNationDistribution = computed(() => groupCount(rows.value, "sellernation", 10));
const sellerTypeDistribution = computed(() => groupCount(rows.value, "sellertype", 6));
const priceDistribution = computed(() =>
  buildBuckets(
    rows.value.map((row) => toNumber(row.price)).filter((item): item is number => item !== null),
    [0, 10, 20, 30, 50, 100, 999999],
    ["$0-10", "$10-20", "$20-30", "$30-50", "$50-100", "$100+"]
  )
);
const ratingDistribution = computed(() =>
  buildBuckets(
    rows.value.map((row) => toNumber(row.rating)).filter((item): item is number => item !== null),
    [0, 3.5, 4.0, 4.5, 5.1],
    ["<3.5", "3.5-4.0", "4.0-4.5", "4.5-5.0"]
  )
);
const listingAgeDistribution = computed(() =>
  buildBuckets(
    rows.value.map((row) => toNumber(row.availabledays)).filter((item): item is number => item !== null),
    [0, 90, 180, 365, 730, 999999],
    ["0-3个月", "3-6个月", "6-12个月", "1-2年", "2年以上"]
  )
);

function baseBarOption(data: Array<{ label: string; value: number }>, color: string): EChartsOption {
  return {
    grid: { left: 56, right: 18, top: 20, bottom: 40, containLabel: true },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    xAxis: {
      type: "category",
      data: data.map((item) => item.label),
      axisLabel: { interval: 0, rotate: data.length > 6 ? 24 : 0 },
    },
    yAxis: { type: "value" },
    series: [{ type: "bar", data: data.map((item) => item.value), itemStyle: { color }, barMaxWidth: 34 }],
  };
}

const salesTrendOption = computed<EChartsOption>(() => ({
  grid: { left: 40, right: 18, top: 24, bottom: 40, containLabel: true },
  tooltip: { trigger: "axis" },
  xAxis: { type: "category", data: salesTrend.value.map((item) => item.label), boundaryGap: false },
  yAxis: { type: "value" },
  series: [
    {
      type: "line",
      smooth: true,
      data: salesTrend.value.map((item) => item.value),
      lineStyle: { width: 3, color: "#2f63da" },
      itemStyle: { color: "#2f63da" },
      areaStyle: { color: "rgba(47, 99, 218, 0.12)" },
    },
  ],
}));
const sellerNationOption = computed(() => baseBarOption(sellerNationDistribution.value, "#3a68d8"));
const priceDistributionOption = computed(() => baseBarOption(priceDistribution.value, "#f97316"));
const ratingDistributionOption = computed(() => baseBarOption(ratingDistribution.value, "#10b981"));
const listingAgeOption = computed(() => baseBarOption(listingAgeDistribution.value, "#7c3aed"));
const sellerTypeOption = computed<EChartsOption>(() => ({
  tooltip: { trigger: "item" },
  legend: { bottom: 0, left: "center" },
  series: [
    {
      type: "pie",
      radius: ["40%", "70%"],
      center: ["50%", "44%"],
      data: sellerTypeDistribution.value.map((item) => ({ name: item.label, value: item.value })),
      label: { formatter: "{b}\n{d}%" },
    },
  ],
}));

function formatInteger(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return Math.round(value).toLocaleString("en-US");
}

function formatDecimal(value: number | null, digits = 1): string {
  if (value === null || Number.isNaN(value)) return "-";
  return value.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function formatCurrency(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `$${value.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

function formatPercent(value: number | null): string {
  if (value === null || Number.isNaN(value)) return "-";
  return `${(value * 100).toLocaleString("en-US", { maximumFractionDigits: 1 })}%`;
}

async function loadAllRows(): Promise<void> {
  const pageSize = 500;
  let page = 1;
  let total = 0;
  const items: ItemRow[] = [];

  do {
    loadingText.value = total
      ? `正在生成报告... ${items.length} / ${total}`
      : "正在读取竞品数据...";
    const data = await fetchPgItems({
      table: "seller_sprite_competitor_items",
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      page,
      pageSize,
      sortBy: "totalunits",
      sortDir: "desc",
    });
    items.push(...(data.items || []));
    total = Number(data.total || 0);
    page += 1;
    if (!data.items?.length) break;
  } while (items.length < total);

  rows.value = items;
}

function goBack(): void {
  router.push({
    name: "competitors",
    query: {
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
    },
  });
}

onMounted(async () => {
  loading.value = true;
  error.value = "";
  try {
    await loadAllRows();
  } catch (err) {
    console.error("load competitor report failed:", err);
    error.value = "报告生成失败，请稍后重试。";
  } finally {
    loading.value = false;
  }
});
</script>

<style scoped>
.competitor-report-page {
  padding-top: 1rem;
}

.report-hero,
.report-panel,
.report-loading-panel {
  border-radius: 14px;
  padding: 16px 18px;
  background: rgba(255, 255, 255, 0.92);
  border: 1px solid #dbeafe;
}

.report-hero {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  margin-bottom: 16px;
}

.report-eyebrow {
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.08em;
  color: #5674b9;
}

.report-hero h2 {
  margin: 8px 0 6px;
  font-size: 1.8rem;
  color: #12203c;
}

.report-hero p {
  margin: 0;
  color: #64748b;
}

.report-loading-panel {
  text-align: center;
}

.report-loading-title {
  font-size: 1.2rem;
  font-weight: 700;
}

.report-loading-subtitle {
  margin-top: 8px;
  color: #64748b;
}

.report-stats-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}

.report-stat-card,
.report-mini-card {
  border-radius: 14px;
  padding: 14px 16px;
  background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
  border: 1px solid #dbe7fb;
}

.report-stat-label,
.report-mini-card span {
  color: #64748b;
  font-size: 13px;
}

.report-stat-value,
.report-mini-card strong {
  display: block;
  margin-top: 8px;
  font-size: 1.45rem;
  font-weight: 700;
  color: #132542;
}

.report-stat-sub {
  margin-top: 6px;
  color: #5674b9;
  font-size: 12px;
}

.report-grid {
  display: grid;
  gap: 16px;
  margin-bottom: 16px;
}

.report-grid-2 {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.report-panel {
  margin-bottom: 16px;
}

.report-panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 14px;
}

.report-panel-header h3 {
  margin: 0;
  font-size: 1.05rem;
  color: #132542;
}

.report-insights {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.report-insight {
  border-radius: 12px;
  padding: 12px 14px;
  background: #f8fbff;
  border: 1px solid #dbe7fb;
  color: #334155;
  line-height: 1.6;
}

.report-mini-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}

.report-share-strip {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
  padding: 10px 12px;
  border-radius: 10px;
  background: #f8fbff;
  border: 1px solid #dbe7fb;
  color: #475569;
}

.report-share-strip strong {
  color: #1d4ed8;
}

.report-table {
  width: 100%;
  border-collapse: collapse;
}

.report-table th,
.report-table td {
  padding: 10px 8px;
  border-bottom: 1px solid #edf2fb;
  text-align: left;
  font-size: 13px;
}

.report-table th {
  color: #5b6b82;
  font-weight: 700;
}

@media (max-width: 1200px) {
  .report-stats-grid,
  .report-grid-2,
  .report-insights,
  .report-mini-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 768px) {
  .report-hero,
  .report-stats-grid,
  .report-grid-2,
  .report-insights,
  .report-mini-grid {
    grid-template-columns: 1fr;
  }

  .report-hero {
    display: grid;
  }
}
</style>
