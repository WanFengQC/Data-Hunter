<template>
  <main class="dashboard competitor-report-page">
    <section class="report-hero">
      <div>
        <div class="report-eyebrow">竞品类目报告</div>
        <h2>{{ reportTitle }}</h2>
        <p>{{ reportSubtitle }}</p>
      </div>
      <div class="report-actions">
        <button class="secondary-btn" type="button" @click="exportReport">导出报告</button>
        <button class="secondary-btn" type="button" @click="goBack">返回查竞品</button>
      </div>
    </section>

    <section v-if="loading" class="report-loading-panel">
      <div class="report-loading-title">报告生成中...</div>
      <div class="report-loading-subtitle">{{ loadingText }}</div>
    </section>
    <section v-else-if="error" class="table-error">{{ error }}</section>

    <template v-else>
      <div v-if="switchingFilters" class="report-switch-mask" aria-live="polite" aria-busy="true">
        <span class="table-loading-spinner" />
        <span class="table-loading-text">加载中...</span>
      </div>
      <section class="report-panel">
        <div class="report-panel-header">
          <h3>筛选</h3>
        </div>
        <div class="report-filter-layout">
          <div class="report-filter-group report-filter-group-category">
            <div class="report-filter-stack">
              <span class="report-filter-label">分类</span>
            </div>
            <div class="report-tab-row">
              <button
                v-for="item in categoryOptions"
                :key="item.path"
                type="button"
                class="report-tab-btn"
                :class="{ active: item.path === currentCategoryPath }"
                @click="selectCategory(item.path)"
              >
                {{ item.locale }}
              </button>
            </div>
          </div>
          <div class="report-filter-group">
            <label class="report-filter-field">
              <span class="report-filter-label">年份</span>
              <select class="report-filter-select" :value="selectedYear" @change="handleYearChange">
                <option :value="0">全部</option>
                <option v-for="year in yearOptions" :key="year" :value="year">{{ year }}</option>
              </select>
            </label>
            <label class="report-filter-field">
              <span class="report-filter-label">月份</span>
              <select class="report-filter-select" :value="selectedMonth" @change="handleMonthChange">
                <option :value="0">全部</option>
                <option v-for="month in monthOptions" :key="month" :value="month">{{ month }}</option>
              </select>
            </label>
          </div>
        </div>
      </section>

      <section class="report-stats-grid">
        <article v-for="card in statCards" :key="card.label" class="report-stat-card">
          <div class="report-stat-label">{{ card.label }}</div>
          <div class="report-stat-value">{{ card.value }}</div>
          <div class="report-stat-sub">{{ card.sub }}</div>
        </article>
      </section>

      <section class="report-panel">
        <div class="report-panel-header">
          <h3>市场概览</h3>
          <div class="report-chip-group">
            <span class="report-chip">快照 {{ focusLabel }}</span>
            <span class="report-chip">样本 {{ formatInteger(snapshotRows.length) }}</span>
          </div>
        </div>
        <div class="report-overview-grid">
          <article class="report-card"><div class="report-card-title">样本概览</div><table class="report-metric-table"><tbody><tr v-for="r in overviewRows" :key="r.label"><th>{{ r.label }}</th><td>{{ r.value }}</td></tr></tbody></table></article>
          <article class="report-card">
            <div class="report-card-title report-card-title-with-tools">
              <span>销量前{{ topProductCount }}商品分析</span>
              <div class="report-card-tools">
                <select v-model="topProductMode" class="report-filter-select report-filter-select-sm">
                  <option value="3">前3商品</option>
                  <option value="5">前5商品</option>
                  <option value="10">前10商品</option>
                  <option value="20">前20商品</option>
                  <option value="50">前50商品</option>
                  <option value="custom">自定义</option>
                </select>
                <input
                  v-if="topProductMode === 'custom'"
                  v-model.number="topProductCustomCount"
                  class="report-filter-input report-filter-input-sm"
                  type="number"
                  min="1"
                  :max="Math.max(snapshotRows.length, 1)"
                  step="1"
                />
              </div>
            </div>
            <table class="report-metric-table"><tbody><tr v-for="r in topRows" :key="r.label"><th>{{ r.label }}</th><td>{{ r.value }}</td></tr></tbody></table>
          </article>
          <article class="report-card">
            <div class="report-card-title report-card-title-with-tools">
              <span>新品分析</span>
              <div class="report-card-tools">
                <select v-model="newProductMode" class="report-filter-select report-filter-select-sm">
                  <option value="1">1个月内上架</option>
                  <option value="3">3个月内上架</option>
                  <option value="6">6个月内上架</option>
                  <option value="12">12个月内上架</option>
                  <option value="custom">自定义</option>
                </select>
                <input
                  v-if="newProductMode === 'custom'"
                  v-model.number="newProductCustomMonths"
                  class="report-filter-input report-filter-input-sm"
                  type="number"
                  min="1"
                  step="1"
                />
              </div>
            </div>
            <table class="report-metric-table"><tbody><tr v-for="r in newRows" :key="r.label"><th>{{ r.label }}</th><td>{{ r.value }}</td></tr></tbody></table>
          </article>
        </div>
      </section>

      <section class="report-panel">
        <div class="report-panel-header">
          <h3>年度月度汇总</h3>
          <div class="report-chip-group">
            <span class="report-chip">分类 {{ currentCategoryLabel }}</span>
            <span class="report-chip">快照 {{ focusLabel }}</span>
          </div>
        </div>
        <table class="report-table report-matrix-table">
          <colgroup>
            <col class="report-col-year" />
            <col class="report-col-metric" />
            <col v-for="m in monthHeaders" :key="`col-${m}`" class="report-col-month" />
            <col class="report-col-total" />
          </colgroup>
          <thead><tr><th>年份</th><th>指标</th><th v-for="m in monthHeaders" :key="m">{{ m }}月</th><th>总计</th></tr></thead>
          <tbody><tr v-for="row in yearTable" :key="row.year"><th>{{ row.year }}年</th><td>体量(万美元)</td><td v-for="m in monthHeaders" :key="`${row.year}-${m}`">{{ formatWanNumber(row.monthMap[m] ?? null) }}</td><td>{{ formatWanNumber(row.total) }}</td></tr></tbody>
        </table>
      </section>

      <section class="report-panel"><div class="report-panel-header"><h3>行业销售趋势</h3></div><ReportChart :option="salesTrendOption" height="360px" /></section>

      <section class="report-grid">
        <article class="report-panel"><div class="report-panel-header"><h3>商品集中度</h3><span class="report-chip">TOP10 {{ formatPercent(productTop10Share) }}</span></div><ReportChart :option="productOption" height="320px" @chart-click="openProductAmazon" /></article>
        <article class="report-panel"><div class="report-panel-header"><h3>品牌集中度</h3><span class="report-chip">TOP10 {{ formatPercent(brandTop10Share) }}</span></div><ReportChart :option="brandOption" height="320px" @chart-click="openBrandBucketModal" /></article>
        <article class="report-panel"><div class="report-panel-header"><h3>卖家集中度</h3><span class="report-chip">TOP10 {{ formatPercent(sellerTop10Share) }}</span></div><ReportChart :option="sellerOption" height="320px" @chart-click="openSellerBucketModal" /></article>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel"><div class="report-panel-header"><h3>品牌销售排行</h3></div><table class="report-table"><thead><tr><th>品牌</th><th>商品数</th><th>销量</th><th>销售额</th><th>销量占比</th></tr></thead><tbody><tr v-for="r in brandRank" :key="r.name"><td>{{ r.name }}</td><td>{{ formatInteger(r.count) }}</td><td>{{ formatInteger(r.units) }}</td><td>{{ formatCurrency(r.amount) }}</td><td>{{ formatPercent(r.share) }}</td></tr></tbody></table></article>
        <article class="report-panel"><div class="report-panel-header"><h3>卖家销售排行</h3></div><table class="report-table"><thead><tr><th>卖家</th><th>商品数</th><th>销量</th><th>销售额</th><th>销量占比</th></tr></thead><tbody><tr v-for="r in sellerRank" :key="r.name"><td>{{ r.name }}</td><td>{{ formatInteger(r.count) }}</td><td>{{ formatInteger(r.units) }}</td><td>{{ formatCurrency(r.amount) }}</td><td>{{ formatPercent(r.share) }}</td></tr></tbody></table></article>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel"><div class="report-panel-header"><h3>卖家类型分布</h3></div><div class="report-grid report-grid-2 report-grid-tight"><ReportChart :option="sellerTypeShareOption" height="320px" /><ReportChart :option="sellerTypeQualityOption" height="320px" /></div></article>
        <article class="report-panel"><div class="report-panel-header"><h3>卖家所属地分布</h3></div><ReportChart :option="sellerNationOption" height="320px" @chart-click="openSellerNationBucketModal" /></article>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel"><div class="report-panel-header"><h3>上架时间分布</h3></div><ReportChart :option="listingAgeOption" height="320px" @chart-click="openListingAgeBucketModal" /></article>
        <article class="report-panel"><div class="report-panel-header"><h3>上架趋势分布</h3></div><ReportChart :option="listingTrendOption" height="320px" @chart-click="openListingTrendBucketModal" /></article>
      </section>

      <section class="report-grid report-grid-2">
        <article class="report-panel"><div class="report-panel-header"><h3>评论数分布</h3></div><ReportChart :option="reviewOption" height="320px" @chart-click="openReviewBucketModal" /></article>
        <article class="report-panel"><div class="report-panel-header"><h3>评分值分布</h3></div><ReportChart :option="ratingOption" height="320px" @chart-click="openRatingBucketModal" /></article>
        <article class="report-panel report-price-panel">
          <div class="report-panel-header">
            <h3>价格分布</h3>
            <div class="report-price-controls">
              <label class="report-filter-field report-filter-field-inline">
                <span class="report-filter-label">起始价格</span>
                <input v-model.number="priceRangeStart" class="report-filter-input" type="number" min="0" step="1" />
              </label>
              <label class="report-filter-field report-filter-field-inline">
                <span class="report-filter-label">跨度</span>
                <input v-model.number="priceRangeStep" class="report-filter-input" type="number" min="1" step="1" />
              </label>
            </div>
          </div>
          <ReportChart :option="priceOption" height="360px" @chart-click="openPriceBucketModal" />
        </article>
      </section>
      <div v-if="priceBucketModal.open" class="report-modal-mask" @click.self="closePriceBucketModal">
        <div class="report-modal-card">
          <div class="report-modal-header">
            <div>
              <h3>{{ priceBucketModal.title }}</h3>
              <p>{{ priceBucketModal.label }} · {{ formatInteger(priceBucketModal.rows.length) }} 个商品</p>
            </div>
            <button class="report-modal-close" type="button" @click="closePriceBucketModal">×</button>
          </div>
          <div class="report-modal-body">
            <ReportChart :option="priceBucketModalOption" height="520px" @chart-click="openPriceBucketAmazon" />
          </div>
        </div>
      </div>
    </template>
  </main>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import type { EChartsOption } from "echarts";
import ReportChart from "@/components/ReportChart.vue";
import { fetchPgItems } from "@/api/data";

type Row = Record<string, unknown>;
type Metric = { label: string; value: string };
type RankRow = { name: string; count: number; units: number; amount: number; newUnits: number; newAmount: number; newCount: number; share: number };
type Dist = { label: string; count: number; units: number; share: number; extra?: number; name?: string; key?: string; rows?: Row[]; row?: Row; amount?: number; newAmount?: number; newCount?: number; entityLabel?: string };
type MonthSum = { ym: number; label: string; amount: number; units: number; avgBsr: number | null };
type CategoryOption = { path: string; locale: string };
const CATEGORY_TEDDY = "Toys & Games:Stuffed Animals & Plush Toys:Stuffed Animals & Teddy Bears";
const CATEGORY_PILLOWS = "Toys & Games:Stuffed Animals & Plush Toys:Plush Pillows";
const CATEGORY_ALL = "ALL";
const SELLER_NATION_ZH: Record<string, string> = {
  US: "美国",
  CN: "中国",
  HK: "中国香港",
  GB: "英国",
  CA: "加拿大",
  SG: "新加坡",
  NL: "荷兰",
  DK: "丹麦",
  IL: "以色列",
  AU: "澳大利亚",
  JP: "日本",
  DE: "德国",
  FR: "法国",
  IT: "意大利",
  ES: "西班牙",
  MX: "墨西哥",
  IN: "印度",
  TW: "中国台湾",
  KR: "韩国",
  TR: "土耳其",
  TH: "泰国",
  VN: "越南",
  MY: "马来西亚",
  ID: "印度尼西亚",
  PH: "菲律宾",
  NA: "未知",
};
const route = useRoute(), router = useRouter(), loading = ref(true), loadingText = ref("准备数据..."), error = ref(""), allRows = ref<Row[]>([]), activeCategoryPath = ref(""), monthHeaders = [1,2,3,4,5,6,7,8,9,10,11,12];
const selectedYear = computed(() => Number(route.query.year || 0) || 0), selectedMonth = computed(() => Number(route.query.month || 0) || 0);
const switchingFilters = ref(false);
const topProductMode = ref<"3" | "5" | "10" | "20" | "50" | "custom">("10");
const topProductCustomCount = ref(10);
const newProductMode = ref<"1" | "3" | "6" | "12" | "custom">("6");
const newProductCustomMonths = ref(6);
const priceRangeStart = ref(5);
const priceRangeStep = ref(5);
const priceBucketModal = ref<{ open: boolean; title: string; label: string; rows: Row[] }>({
  open: false,
  title: "商品销量分布",
  label: "",
  rows: [],
});
const priceBucketTooltipRoot = typeof document !== "undefined" ? document.createElement("div") : null;
let priceBucketTooltipIndex = -1;
const productTooltipRoot = typeof document !== "undefined" ? document.createElement("div") : null;
let productTooltipIndex = -1;
const aggregateTooltipRoot = typeof document !== "undefined" ? document.createElement("div") : null;
let aggregateTooltipIndex = -1;
const priceBucketModalPoints = computed(() => {
  const rows = [...priceBucketModal.value.rows].sort((a, b) => (n(b.totalunits) || 0) - (n(a.totalunits) || 0));
  const totalUnits = sum(rows.map((row) => n(row.totalunits)));
  const totalAmount = sum(rows.map((row) => n(row.totalamount)));
  return rows.map((row, index) => ({
    rank: index + 1,
    title: t(row.title) || t(row.asin) || `#${index + 1}`,
    asin: t(row.asin),
    imageUrl: t(row.imageurl),
    brand: t(row.brand) || "-",
    seller: t(row.sellername) || "-",
    sellerType: t(row.sellertype) || "NA",
    price: n(row.price),
    variants: n(row.variants),
    amount: n(row.totalamount) || 0,
    amountShare: totalAmount ? ((n(row.totalamount) || 0) / totalAmount) * 100 : 0,
    availableDate: dateMs(row),
    reviews: n(row.reviews),
    rating: n(row.rating),
    units: n(row.totalunits) || 0,
    newUnits: isNew(row) ? (n(row.totalunits) || 0) : 0,
    share: totalUnits ? ((n(row.totalunits) || 0) / totalUnits) * 100 : 0,
  }));
});
function renderPriceBucketTooltip(point: ReturnType<typeof priceBucketModalPoints["value"][number]> extends never ? never : (typeof priceBucketModalPoints.value)[number], dataIndex: number) {
  const salesLabel = point.newUnits >= point.units && point.newUnits > 0 ? "新品销量" : "月销量";
  const salesValue = point.newUnits >= point.units && point.newUnits > 0 ? point.newUnits : point.units;
  const imageBlock = point.imageUrl
    ? `<img src="${point.imageUrl}" alt="${point.asin || "product"}" style="width:88px;height:88px;border-radius:10px;object-fit:cover;border:1px solid #e5ebf5;flex:none;" loading="eager" />`
    : "";
  const html = [
    `<div style="max-width:760px;">`,
    `<div style="font-size:14px;font-weight:700;line-height:1.5;color:#12203c;margin-bottom:10px;white-space:normal;word-break:break-word;">${point.title}</div>`,
    `<div style="display:flex;gap:14px;align-items:flex-start;">`,
    imageBlock,
    `<div style="display:grid;grid-template-columns:repeat(4, minmax(90px, auto));gap:6px 14px;font-size:13px;line-height:1.5;color:#334155;">`,
    `<div><span style="color:#64748b;">排名</span> 第${point.rank}名</div>`,
    `<div><span style="color:#64748b;">${salesLabel}</span> ${formatInteger(salesValue)}</div>`,
    point.asin ? `<div><span style="color:#64748b;">ASIN</span> ${point.asin}</div>` : "",
    `<div><span style="color:#64748b;">月销售额</span> ${formatCurrency(point.amount)}</div>`,
    `<div><span style="color:#64748b;">品牌</span> ${point.brand}</div>`,
    `<div><span style="color:#64748b;">销量占比</span> ${point.share.toFixed(2)}%</div>`,
    `<div><span style="color:#64748b;">卖家</span> ${point.seller}</div>`,
    `<div><span style="color:#64748b;">销售额占比</span> ${point.amountShare.toFixed(2)}%</div>`,
    `<div><span style="color:#64748b;">卖家类型</span> ${point.sellerType}</div>`,
    `<div><span style="color:#64748b;">上架时间</span> ${fdate(point.availableDate)}</div>`,
    `<div><span style="color:#64748b;">价格</span> ${formatCurrency(point.price)}</div>`,
    `<div><span style="color:#64748b;">评论数/星级</span> ${formatInteger(point.reviews)}/${fd(point.rating, 1)}</div>`,
    `<div><span style="color:#64748b;">变体数</span> ${formatInteger(point.variants)}</div>`,
    `</div>`,
    `</div>`,
    `</div>`,
  ].filter(Boolean).join("");
  if (!priceBucketTooltipRoot) return html;
  if (priceBucketTooltipIndex !== dataIndex) {
    priceBucketTooltipRoot.innerHTML = html;
    priceBucketTooltipIndex = dataIndex;
  }
  return priceBucketTooltipRoot;
}
function renderProductInfoCard(row: Row, options: { rank: number; share: number; dataIndex: number; root: HTMLDivElement | null; cacheKeyRef: { value: number } }) {
  const salesValue = n(row.totalunits) || 0;
  const newSalesValue = isNew(row) ? salesValue : 0;
  const salesLabel = newSalesValue >= salesValue && newSalesValue > 0 ? "新品销量" : "销量";
  const amount = n(row.totalamount) || 0;
  const amountShare = metric.value.amount ? (amount / metric.value.amount) * 100 : 0;
  const html = [
    `<div style="max-width:760px;">`,
    `<div style="font-size:14px;font-weight:700;line-height:1.5;color:#12203c;margin-bottom:10px;white-space:normal;word-break:break-word;">${t(row.title) || t(row.asin) || `#${options.rank}`}</div>`,
    `<div style="display:flex;gap:14px;align-items:flex-start;">`,
    t(row.imageurl) ? `<img src="${t(row.imageurl)}" alt="${t(row.asin) || "product"}" style="width:88px;height:88px;border-radius:10px;object-fit:cover;border:1px solid #e5ebf5;flex:none;" loading="eager" />` : "",
    `<div style="display:grid;grid-template-columns:repeat(4, minmax(90px, auto));gap:6px 14px;font-size:13px;line-height:1.5;color:#334155;">`,
    `<div><span style="color:#64748b;">排名</span> 第${options.rank}名</div>`,
    `<div><span style="color:#64748b;">${salesLabel}</span> ${formatInteger(newSalesValue >= salesValue && newSalesValue > 0 ? newSalesValue : salesValue)}</div>`,
    t(row.asin) ? `<div><span style="color:#64748b;">ASIN</span> ${t(row.asin)}</div>` : "",
    `<div><span style="color:#64748b;">月销售额</span> ${formatCurrency(amount)}</div>`,
    `<div><span style="color:#64748b;">品牌</span> ${t(row.brand) || "-"}</div>`,
    `<div><span style="color:#64748b;">销量占比</span> ${options.share.toFixed(2)}%</div>`,
    `<div><span style="color:#64748b;">卖家</span> ${t(row.sellername) || "-"}</div>`,
    `<div><span style="color:#64748b;">销售额占比</span> ${amountShare.toFixed(2)}%</div>`,
    `<div><span style="color:#64748b;">卖家类型</span> ${t(row.sellertype) || "NA"}</div>`,
    `<div><span style="color:#64748b;">上架时间</span> ${fdate(dateMs(row))}</div>`,
    `<div><span style="color:#64748b;">价格</span> ${formatCurrency(n(row.price))}</div>`,
    `<div><span style="color:#64748b;">评论数/星级</span> ${formatInteger(n(row.reviews))}/${fd(n(row.rating), 1)}</div>`,
    `<div><span style="color:#64748b;">变体数</span> ${formatInteger(n(row.variants))}</div>`,
    `</div>`,
    `</div>`,
    `</div>`,
  ].filter(Boolean).join("");
  if (!options.root) return html;
  if (options.cacheKeyRef.value !== options.dataIndex) {
    options.root.innerHTML = html;
    options.cacheKeyRef.value = options.dataIndex;
  }
  return options.root;
}
function renderAggregateInfoCard(point: Dist, options: { rank: number; dataIndex: number; root: HTMLDivElement | null; cacheKeyRef: { value: number } }) {
  const units = point.units || 0;
  const newUnits = point.extra || 0;
  const nonNewUnits = Math.max(units - newUnits, 0);
  const amount = point.amount || 0;
  const newAmount = point.newAmount || 0;
  const nonNewAmount = Math.max(amount - newAmount, 0);
  const newCount = point.newCount || 0;
  const amountShare = metric.value.amount ? (amount / metric.value.amount) * 100 : 0;
  const unitNewRatio = units ? (newUnits / units) * 100 : 0;
  const unitNonNewRatio = units ? (nonNewUnits / units) * 100 : 0;
  const amountNewRatio = amount ? (newAmount / amount) * 100 : 0;
  const amountNonNewRatio = amount ? (nonNewAmount / amount) * 100 : 0;
  const html = [
    `<div style="max-width:520px;display:grid;grid-template-columns:140px 1fr;gap:6px 12px;font-size:13px;line-height:1.55;color:#334155;">`,
    `<div style="color:#64748b;">排名</div><div style="text-align:right;color:#12203c;">第${options.rank}名</div>`,
    `<div style="color:#64748b;">${point.entityLabel || "名称"}</div><div style="text-align:right;color:#12203c;">${point.name || point.label || "-"}</div>`,
    `<div style="color:#64748b;">商品数量/新品</div><div style="text-align:right;color:#12203c;">${formatInteger(point.count)}/${formatInteger(newCount)}</div>`,
    `<div style="color:#64748b;">该${point.entityLabel === "卖家名称" ? "卖家" : "品牌"}月销量</div><div style="text-align:right;color:#12203c;">${formatInteger(units)}</div>`,
    `<div style="color:#64748b;">该${point.entityLabel === "卖家名称" ? "卖家" : "品牌"}销量占比</div><div style="text-align:right;color:#12203c;">${Number((point.share * 100).toFixed(2))}% (新品: ${unitNewRatio.toFixed(2)}%, 非新品: ${unitNonNewRatio.toFixed(2)}%)</div>`,
    `<div style="color:#64748b;">该${point.entityLabel === "卖家名称" ? "卖家" : "品牌"}月销售额</div><div style="text-align:right;color:#12203c;">${formatCurrency(amount)}</div>`,
    `<div style="color:#64748b;">该${point.entityLabel === "卖家名称" ? "卖家" : "品牌"}销售额占比</div><div style="text-align:right;color:#12203c;">${amountShare.toFixed(2)}% (新品: ${amountNewRatio.toFixed(2)}%, 非新品: ${amountNonNewRatio.toFixed(2)}%)</div>`,
    `</div>`,
  ].join("");
  if (!options.root) return html;
  if (options.cacheKeyRef.value !== options.dataIndex) {
    options.root.innerHTML = html;
    options.cacheKeyRef.value = options.dataIndex;
  }
  return options.root;
}
const priceBucketModalOption = computed<EChartsOption>(() => ({
  grid: { left: 56, right: 52, top: 42, bottom: 96, containLabel: true },
  tooltip: {
    trigger: "axis",
    confine: true,
    enterable: true,
    formatter: (params) => {
      const seriesList = Array.isArray(params) ? params : [params];
      const dataIndex = Number(seriesList[0]?.dataIndex ?? 0);
      const point = priceBucketModalPoints.value[dataIndex];
      if (!point) return "";
      return renderPriceBucketTooltip(point, dataIndex);
    },
  },
  legend: { top: 0, data: ["销量", "新品销量", "销量占比"] },
  xAxis: {
    type: "category",
    data: priceBucketModalPoints.value.map((item) => String(item.rank)),
  },
  yAxis: [
    { type: "value", name: "销量" },
    { type: "value", name: "销量占比", axisLabel: { formatter: "{value}%" } },
  ],
  dataZoom: [
    { type: "inside", xAxisIndex: 0, start: 0, end: 100 },
    { type: "slider", xAxisIndex: 0, height: 28, bottom: 30, start: 0, end: 100 },
  ],
  series: [
    {
      name: "销量",
      type: "bar",
      barMaxWidth: 24,
      stack: "single-bar",
      data: priceBucketModalPoints.value.map((item) => item.newUnits >= item.units ? 0 : item.units),
      itemStyle: { color: "#ffad2f" },
    },
    {
      name: "新品销量",
      type: "bar",
      barMaxWidth: 24,
      stack: "single-bar",
      data: priceBucketModalPoints.value.map((item) => item.newUnits),
      itemStyle: { color: "#a5b4fc" },
    },
    {
      name: "销量占比",
      type: "line",
      yAxisIndex: 1,
      smooth: true,
      data: priceBucketModalPoints.value.map((item) => Number(item.share.toFixed(2))),
      itemStyle: { color: "#6cc24a" },
      lineStyle: { color: "#6cc24a", width: 2 },
    },
  ],
}));
const n = (v: unknown) => v === null || v === undefined || v === "" ? null : (Number.isFinite(Number(v)) ? Number(v) : null);
const t = (v: unknown) => v === null || v === undefined ? "" : String(v).trim();
const avg = (arr: Array<number | null>) => { const x = arr.filter((v): v is number => typeof v === "number"); return x.length ? x.reduce((a,b)=>a+b,0)/x.length : null; };
const sum = (arr: Array<number | null>) => arr.reduce((a,b)=>a+(b||0),0);
const ym = (row: Row) => n(row.year_month);
const yes = (v: unknown) => ["Y","TRUE","1"].includes(t(v).toUpperCase());
const dateMs = (row: Row) => { const x = n(row.availabledate); if (x !== null) return x; const p = Date.parse(t(row.availabledate)); return Number.isFinite(p) ? p : null; };
const fmtYm = (value: number | null) => value ? `${Math.floor(value/100)}-${String(value%100).padStart(2,"0")}` : "-";
const fi = (v: number | null) => v === null || Number.isNaN(v) ? "-" : Math.round(v).toLocaleString("en-US");
const fd = (v: number | null, d = 1) => v === null || Number.isNaN(v) ? "-" : v.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
const fc = (v: number | null) => v === null || Number.isNaN(v) ? "-" : `$${v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
const fp = (v: number | null) => v === null || Number.isNaN(v) ? "-" : `${(v*100).toLocaleString("en-US", { maximumFractionDigits: 1 })}%`;
const fw = (v: number | null) => v === null || Number.isNaN(v) ? "-" : `${(v/10000).toLocaleString("en-US", { maximumFractionDigits: 1 })} 万美元`;
const fwn = (v: number | null) => v === null || Number.isNaN(v) ? "-" : (v/10000).toLocaleString("en-US", { maximumFractionDigits: 1 });
const fdate = (v: number | null) => !v ? "-" : new Date(v).toISOString().slice(0,10);
const formatInteger = fi;
const formatCurrency = fc;
const formatPercent = fp;
const formatWan = fw;
const formatWanNumber = fwn;
function normalizeCategoryPath(value: unknown): string {
  const text = t(value);
  if (!text) return "";
  if (text.includes("Plush Pillows")) return CATEGORY_PILLOWS;
  if (text.includes("Stuffed Animals & Teddy Bears")) return CATEGORY_TEDDY;
  return "";
}
function getCategoryLabel(path: string): string {
  if (path === CATEGORY_ALL) return CATEGORY_ALL;
  if (path === CATEGORY_PILLOWS) return "Plush Pillows";
  if (path === CATEGORY_TEDDY) return "Stuffed Animals & Teddy Bears";
  return path;
}
const group = (rows: Row[], field: string) => { const total = sum(rows.map(r => n(r.totalunits))); const m = new Map<string, RankRow>(); rows.forEach(row => { const key = t(row[field]) || "未知"; const cur = m.get(key) || { name:key,count:0,units:0,amount:0,newUnits:0,newAmount:0,newCount:0,share:0 }; const units = n(row.totalunits) || 0; const amount = n(row.totalamount) || 0; cur.count += 1; cur.units += units; cur.amount += amount; if (isNew(row)) { cur.newUnits += units; cur.newAmount += amount; cur.newCount += 1; } m.set(key, cur); }); return [...m.values()].sort((a,b)=>b.units-a.units).map(r => ({ ...r, share: total ? r.units/total : 0 })); };
const bucket = (rows: Row[], getter: (row: Row) => number | null, edges: number[], labels: string[]) => { const total = sum(rows.map(r => n(r.totalunits))); const out = labels.map(label => ({ label, count:0, units:0, share:0, rows: [] as Row[] })); rows.forEach(row => { const value = getter(row); if (value === null) return; for (let i=0;i<edges.length-1;i+=1) if (value >= edges[i] && value < edges[i+1]) { out[i].count += 1; out[i].units += n(row.totalunits) || 0; out[i].rows.push(row); break; } }); out.forEach(item => item.share = total ? item.units/total : 0); return out; };
const categoryOptions = computed<CategoryOption[]>(() => {
  const map = new Map<string, string>();
  allRows.value.forEach((row) => {
    const path = normalizeCategoryPath(row.nodelabelpath);
    if (!path || map.has(path)) return;
    map.set(path, path);
  });
  return [{ path: CATEGORY_ALL, locale: CATEGORY_ALL }].concat(
    [...map.entries()]
      .map(([path]) => ({ path, locale: getCategoryLabel(path) }))
      .sort((a, b) => a.path.localeCompare(b.path)),
  );
});
const selectedCategory = computed(() => categoryOptions.value.find((item) => item.path === activeCategoryPath.value) || categoryOptions.value[0] || null);
const currentCategoryPath = computed(() => selectedCategory.value?.path || CATEGORY_ALL);
const currentCategoryLabel = computed(() => selectedCategory.value?.locale || CATEGORY_ALL);
const categoryRows = computed(() => {
  const current = selectedCategory.value;
  if (!current || current.path === CATEGORY_ALL) return allRows.value;
  return allRows.value.filter((row) => normalizeCategoryPath(row.nodelabelpath) === current.path);
});
const historyByMonth = computed<MonthSum[]>(() => { const m = new Map<number, { amount:number; units:number; bsrs:number[] }>(); categoryRows.value.forEach(row => { const key = ym(row); if (!key) return; const cur = m.get(key) || { amount:0, units:0, bsrs:[] }; cur.amount += n(row.totalamount) || 0; cur.units += n(row.totalunits) || 0; if (n(row.bsrrank) !== null) cur.bsrs.push(n(row.bsrrank)!); m.set(key, cur); }); return [...m.entries()].map(([key, cur]) => ({ ym:key, label:fmtYm(key), amount:cur.amount, units:cur.units, avgBsr:avg(cur.bsrs) })).sort((a,b)=>a.ym-b.ym); });
const yearOptions = computed(() => [...new Set(historyByMonth.value.map((item) => Math.floor(item.ym / 100)))].sort((a, b) => a - b));
const monthOptions = computed(() => {
  const months = historyByMonth.value
    .filter((item) => !selectedYear.value || Math.floor(item.ym / 100) === selectedYear.value)
    .map((item) => item.ym % 100);
  return [...new Set(months)].sort((a, b) => a - b);
});
const focusYm = computed(() => {
  const months = historyByMonth.value.map((item) => item.ym);
  if (!months.length) return null;
  if (selectedYear.value && selectedMonth.value) {
    const target = selectedYear.value * 100 + selectedMonth.value;
    return months.includes(target) ? target : null;
  }
  if (selectedYear.value) {
    const sameYear = months.filter((value) => Math.floor(value / 100) === selectedYear.value);
    return sameYear.at(-1) ?? null;
  }
  if (selectedMonth.value) {
    const sameMonth = months.filter((value) => value % 100 === selectedMonth.value);
    return sameMonth.at(-1) ?? null;
  }
  return months.at(-1) ?? null;
});
const focusLabel = computed(() => fmtYm(focusYm.value));
const snapshotRows = computed(() => focusYm.value ? categoryRows.value.filter(row => ym(row) === focusYm.value) : []);
const reportTitle = computed(() => `${currentCategoryLabel.value === CATEGORY_ALL ? "All Categories" : currentCategoryLabel.value} Report`);
const reportSubtitle = computed(() => `快照月份 ${focusLabel.value}，样本商品 ${fi(snapshotRows.value.length)} 个，历史月份 ${fi(historyByMonth.value.length)} 个。`);
const topProductCount = computed(() => {
  const preset = Number(topProductMode.value);
  if (Number.isFinite(preset) && preset > 0) return preset;
  const custom = Math.floor(Number(topProductCustomCount.value) || 10);
  return Math.min(Math.max(custom, 1), Math.max(snapshotRows.value.length, 1));
});
const newProductMonthCount = computed(() => {
  const preset = Number(newProductMode.value);
  if (Number.isFinite(preset) && preset > 0) return preset;
  return Math.max(Math.floor(Number(newProductCustomMonths.value) || 6), 1);
});
const newProductDayLimit = computed(() => newProductMonthCount.value * 30);
const isNew = (row: Row) => (n(row.availabledays) ?? 999999) <= newProductDayLimit.value;
const metric = computed(() => { const rows = snapshotRows.value, dates = rows.map(dateMs).filter((v): v is number => v !== null).sort((a,b)=>a-b), newRows = rows.filter(isNew), ratings = newRows.map(r=>n(r.rating)).filter((v): v is number => v !== null); return { total:rows.length, brands:new Set(rows.map(r=>t(r.brand)).filter(Boolean)).size, sellers:new Set(rows.map(r=>t(r.sellername)).filter(Boolean)).size, units:sum(rows.map(r=>n(r.totalunits))), amount:sum(rows.map(r=>n(r.totalamount))), avgBsr:avg(rows.map(r=>n(r.bsrrank))), avgPrice:avg(rows.map(r=>n(r.price))), avgReviewGrow:avg(rows.map(r=>n(r.reviewsdelta))), avgReviews:avg(rows.map(r=>n(r.reviews))), avgRating:avg(rows.map(r=>n(r.rating))), avgSellerCount:avg(rows.map(r=>n(r.sellers))), newRows, newCount:newRows.length, newRatio:rows.length ? newRows.length/rows.length : 0, newAvgPrice:avg(newRows.map(r=>n(r.price))), newAvgRating:avg(newRows.map(r=>n(r.rating))), newMax:ratings.length ? Math.max(...ratings) : null, newMin:ratings.length ? Math.min(...ratings) : null, newUnits:sum(newRows.map(r=>n(r.totalunits))), newAmount:sum(newRows.map(r=>n(r.totalamount))), firstDate:dates[0] ?? null, lastDate:dates.at(-1) ?? null, fbaRatio:rows.length ? rows.filter(r=>t(r.sellertype).toUpperCase()==="FBA").length/rows.length : 0, videoRatio:rows.length ? rows.filter(r=>yes(r.video)).length/rows.length : 0, ebcRatio:rows.length ? rows.filter(r=>yes(r.ebc)).length/rows.length : 0 }; });
const topProducts = computed(() => [...snapshotRows.value].sort((a,b)=>(n(b.totalunits)||0)-(n(a.totalunits)||0)).slice(0, topProductCount.value));
const brandRank = computed(() => group(snapshotRows.value, "brand").slice(0,10)), sellerRank = computed(() => group(snapshotRows.value, "sellername").slice(0,10));
const productTop10Share = computed(() => metric.value.units ? sum(topProducts.value.map(r=>n(r.totalunits)))/metric.value.units : 0), brandTop10Share = computed(() => metric.value.units ? sum(brandRank.value.map(r=>r.units))/metric.value.units : 0), sellerTop10Share = computed(() => metric.value.units ? sum(sellerRank.value.map(r=>r.units))/metric.value.units : 0);
const statCards = computed(() => [{ label:"样本商品数", value:fi(metric.value.total), sub:`品牌数 ${fi(metric.value.brands)}` }, { label:"近30天销量", value:fi(metric.value.units), sub:`销售额 ${fc(metric.value.amount)}` }, { label:"平均价格", value:fc(metric.value.avgPrice), sub:`平均BSR ${fi(metric.value.avgBsr)}` }, { label:"平均评分", value:fd(metric.value.avgRating,2), sub:`平均评论数 ${fd(metric.value.avgReviews,0)}` }]);
const overviewRows = computed<Metric[]>(() => [{ label:"样本商品数", value:fi(metric.value.total) }, { label:"样本品牌数/卖家数", value:`${fi(metric.value.brands)}/${fi(metric.value.sellers)}` }, { label:"平均BSR", value:fi(metric.value.avgBsr) }, { label:"近30天总销量", value:fi(metric.value.units) }, { label:"近30天总销售额", value:fc(metric.value.amount) }, { label:"平均价格", value:fc(metric.value.avgPrice) }, { label:"近30天评论平均增长数", value:fd(metric.value.avgReviewGrow,1) }, { label:"平均评论数", value:fd(metric.value.avgReviews,0) }, { label:"平均星级", value:fd(metric.value.avgRating,1) }, { label:"平均卖家数", value:fd(metric.value.avgSellerCount,1) }]);
const topRows = computed<Metric[]>(() => {
  const prefix = `销量前${topProductCount.value}商品`;
  return [
    { label:`${prefix}样本总数`, value:fi(topProducts.value.length) },
    { label:`${prefix}BSR均值`, value:fi(avg(topProducts.value.map(r=>n(r.bsrrank)))) },
    { label:`${prefix}近30天总销量`, value:fi(sum(topProducts.value.map(r=>n(r.totalunits)))) },
    { label:`${prefix}近30天总销售额`, value:fc(sum(topProducts.value.map(r=>n(r.totalamount)))) },
    { label:`${prefix}平均价格`, value:fc(avg(topProducts.value.map(r=>n(r.price)))) },
    { label:`${prefix}近30天评论平均增长数`, value:fd(avg(topProducts.value.map(r=>n(r.reviewsdelta))),1) },
    { label:`${prefix}平均评论数`, value:fd(avg(topProducts.value.map(r=>n(r.reviews))),0) },
    { label:`${prefix}平均星级`, value:fd(avg(topProducts.value.map(r=>n(r.rating))),1) },
  ];
});
const newRows = computed<Metric[]>(() => {
  const prefix = `${newProductMonthCount.value}个月内上架新品`;
  return [
    { label:"新品数量", value:fi(metric.value.newCount) },
    { label:"新品占比", value:fp(metric.value.newRatio) },
    { label:"新品评分数(最高/平均/最低)", value:`${fd(metric.value.newMax,1)}/${fd(metric.value.newAvgRating,1)}/${fd(metric.value.newMin,1)}` },
    { label:"新品平均价格", value:fc(metric.value.newAvgPrice) },
    { label:`${prefix}近30天总销量`, value:fi(metric.value.newUnits) },
    { label:`${prefix}近30天总销售额`, value:fc(metric.value.newAmount) },
    { label:"商品首次上架时间", value:fdate(metric.value.firstDate) },
    { label:"商品最新上架时间", value:fdate(metric.value.lastDate) },
  ];
});
const summaryTitle = computed(() => selectedMonth.value ? "月份汇总" : selectedYear.value ? `${selectedYear.value}年月度汇总` : "年度月度汇总");
const yearTable = computed(() => [...new Set(historyByMonth.value.map((r) => Math.floor(r.ym / 100)))]
  .sort((a, b) => a - b)
  .map((year) => {
    const monthMap: Record<number, number> = {};
    const rows = historyByMonth.value.filter((r) => Math.floor(r.ym / 100) === year);
    rows.forEach((r) => {
      monthMap[r.ym % 100] = r.amount;
    });
    return { year, monthMap, total: rows.reduce((acc, item) => acc + item.amount, 0) };
  }));
const lineBar = (points: Dist[], bar: string, line: string, second?: string, rankLabel = false, collapseNewOnly = false, enableZoom = false, stackSecond = false, barMetric: "units" | "count" = "units"): EChartsOption => ({
  grid:{ left:50,right:48,top:28,bottom:enableZoom ? 96 : 58,containLabel:true },
  tooltip:{
    trigger:"axis",
    confine: true,
    position: (_pos, _params, _dom, _rect, size) => {
      const [mouseX, mouseY] = _pos as number[];
      const [contentWidth, contentHeight] = size.contentSize;
      const [viewWidth, viewHeight] = size.viewSize;
      const x = Math.min(Math.max(12, mouseX + 12), Math.max(12, viewWidth - contentWidth - 12));
      const y = Math.min(Math.max(12, mouseY - contentHeight - 16), Math.max(12, viewHeight - contentHeight - 12));
      return [x, y];
    },
    formatter: (params) => {
      const seriesList = Array.isArray(params) ? params : [params];
      const dataIndex = Number(seriesList[0]?.dataIndex ?? 0);
      const point = points[dataIndex];
      if (collapseNewOnly && point?.row) {
        return renderProductInfoCard(point.row, {
          rank: dataIndex + 1,
          share: Number((point.share * 100).toFixed(2)),
          dataIndex,
          root: productTooltipRoot,
          cacheKeyRef: {
            get value() { return productTooltipIndex; },
            set value(next: number) { productTooltipIndex = next; },
          },
        });
      }
      if (stackSecond && point?.name) {
        return renderAggregateInfoCard(point, {
          rank: dataIndex + 1,
          dataIndex,
          root: aggregateTooltipRoot,
          cacheKeyRef: {
            get value() { return aggregateTooltipIndex; },
            set value(next: number) { aggregateTooltipIndex = next; },
          },
        });
      }
      const lines = [`<div style="max-width:420px;white-space:normal;word-break:break-word;">${point?.name || point?.label || "-"}</div>`];
      if (collapseNewOnly && second && point) {
        const totalValue = point.units || point.count || 0;
        const newValue = point.extra || 0;
        const salesLabel = newValue >= totalValue && newValue > 0 ? second : bar;
        const salesValue = newValue >= totalValue && newValue > 0 ? newValue : totalValue;
        const salesMarker = newValue >= totalValue && newValue > 0 ? (seriesList[1]?.marker || "") : (seriesList[0]?.marker || "");
        lines.push(`<div>${salesMarker}${salesLabel}: ${salesValue}</div>`);
        lines.push(`<div>${seriesList.at(-1)?.marker || ""}${line}: ${Number((point.share * 100).toFixed(2))}%</div>`);
        return lines.join("");
      }
      if (point) {
        lines.push(`<div>销量: ${formatInteger(point.units || 0)}</div>`);
      }
      seriesList.forEach((item, index) => {
        let displayValue = item.value;
        if ((collapseNewOnly || stackSecond) && second && point) {
          if (index === 0) displayValue = point.units || point.count;
          if (index === 1) displayValue = point.extra || 0;
        }
        lines.push(`<div>${item.marker}${item.seriesName}: ${displayValue}${item.seriesName === line ? "%" : ""}</div>`);
      });
      return lines.join("");
    },
  },
  legend:{ top:0, data:[bar, ...(second ? [second] : []), line] },
  xAxis:{ type:"category", data:points.map(i=>i.label), axisLabel:{ interval:0, rotate: rankLabel ? 0 : points.length > 10 ? 60 : 0 } },
  yAxis:[{ type:"value", name:bar }, { type:"value", name:"占比", axisLabel:{ formatter:"{value}%" } }],
  dataZoom: enableZoom ? [
    { type:"inside", xAxisIndex: 0, start: 0, end: Math.min(100, points.length > 0 ? Math.max(12, (30 / points.length) * 100) : 100) },
    { type:"slider", xAxisIndex: 0, height: 28, bottom: 30, start: 0, end: Math.min(100, points.length > 0 ? Math.max(12, (30 / points.length) * 100) : 100) },
  ] : undefined,
  series:[
    {
      name:bar,
      type:"bar",
      barMaxWidth:24,
      stack: (collapseNewOnly || stackSecond) && second ? "single-bar" : undefined,
      data:points.map((i) => {
        const totalValue = barMetric === "count" ? i.count : (i.units || i.count);
        const newValue = i.extra || 0;
        if (collapseNewOnly && second && newValue >= totalValue) return 0;
        if (stackSecond && second) return Math.max(totalValue - newValue, 0);
        return totalValue;
      }),
      itemStyle:{ color:"#ffad2f" },
    },
    ...(second ? [{
      name:second,
      type:"bar",
      barMaxWidth:24,
      stack: (collapseNewOnly || stackSecond) ? "single-bar" : undefined,
      data:points.map(i=>i.extra || 0),
      itemStyle:{ color:"#a5b4fc" },
    }] : []),
    { name:line, type:"line", yAxisIndex:1, smooth:true, data:points.map(i=>Number((i.share*100).toFixed(2))), itemStyle:{ color:"#6cc24a" }, lineStyle:{ color:"#6cc24a", width:2 } },
  ],
});
const concentration = (mode: "product"|"brand"|"seller"): Dist[] => {
  const total = metric.value.units || 0;
  if (!total) return [];
  if (mode === "product") {
    return [...snapshotRows.value]
      .sort((a,b)=>(n(b.totalunits)||0)-(n(a.totalunits)||0))
      .map((r,i)=>({
        label:String(i+1),
        name:t(r.title) || t(r.asin) || `#${i+1}`,
        row: r,
        count:1,
        units:n(r.totalunits)||0,
        share:(n(r.totalunits)||0)/total,
        extra:isNew(r)?(n(r.totalunits)||0):0,
      }));
  }
  return group(snapshotRows.value, mode === "brand" ? "brand" : "sellername")
    .map((r, i) => ({
      label: String(i + 1),
      name: r.name,
      count: r.count,
      units: r.units,
      share: r.share,
      extra: r.newUnits,
      amount: r.amount,
      newAmount: r.newAmount,
      newCount: r.newCount,
      entityLabel: mode === "brand" ? "品牌名称" : "卖家名称",
    }));
};
const byField = (field: string, limit = 10): Dist[] => { const total = metric.value.units || 0, map = new Map<string, Dist>(); snapshotRows.value.forEach(r => { const key = t(r[field]) || "未知", cur = map.get(key) || { label:key, count:0, units:0, share:0 }; cur.count += 1; cur.units += n(r.totalunits) || 0; map.set(key, cur); }); return [...map.values()].sort((a,b)=>b.count-a.count).slice(0,limit).map(i => ({ ...i, share: total ? i.units/total : 0 })); };
const sellerNationLabel = (value: unknown) => {
  const code = t(value).trim().toUpperCase();
  if (!code || code === "UNKNOWN") return "未知";
  return SELLER_NATION_ZH[code] || code;
};
const productOption = computed(() => lineBar(concentration("product"), "销量", "销量占比", "新品销量", true, true, true));
const brandOption = computed(() => lineBar(concentration("brand"), "销量", "销量占比", "新品销量", true, false, true, true));
const sellerOption = computed(() => lineBar(concentration("seller"), "销量", "销量占比", "新品销量", true, false, true, true));
const salesTrendOption = computed<EChartsOption>(() => ({
  grid:{ left:50,right:56,top:28,bottom:44,containLabel:true },
  tooltip:{
    trigger:"axis",
    formatter: (params) => {
      const seriesList = Array.isArray(params) ? params : [params];
      const label = String(seriesList[0]?.axisValueLabel || seriesList[0]?.name || "-");
      const units = Number(seriesList.find((item) => item.seriesName === "销量")?.value ?? 0);
      const amount = Number(seriesList.find((item) => item.seriesName === "销售额")?.value ?? 0);
      const avgBsr = Number(seriesList.find((item) => item.seriesName === "平均BSR")?.value ?? 0);
      return [
        `<div>${label}</div>`,
        `<div>${seriesList.find((item) => item.seriesName === "销量")?.marker || ""}销量 ${formatInteger(units)}</div>`,
        `<div>${seriesList.find((item) => item.seriesName === "销售额")?.marker || ""}销售额 ${formatCurrency(amount)}</div>`,
        `<div>${seriesList.find((item) => item.seriesName === "平均BSR")?.marker || ""}平均BSR ${formatInteger(avgBsr)}</div>`,
      ].join("");
    },
  },
  legend:{ top:0, data:["销量","销售额","平均BSR"] },
  xAxis:{ type:"category", data:historyByMonth.value.map(i=>i.label) },
  yAxis:[{ type:"value", name:"销量" }, { type:"value", name:"销售额($)" }, { type:"value", name:"平均BSR", show:false }],
  series:[
    { name:"销量", type:"bar", data:historyByMonth.value.map(i=>i.units), itemStyle:{ color:"#ffad2f" }, barMaxWidth:28 },
    { name:"销售额", type:"line", yAxisIndex:1, smooth:true, data:historyByMonth.value.map(i=>i.amount), itemStyle:{ color:"#62c554" }, lineStyle:{ color:"#62c554", width:2 } },
    { name:"平均BSR", type:"line", yAxisIndex:2, smooth:true, data:historyByMonth.value.map(i=>i.avgBsr), itemStyle:{ color:"#c7cbd4" }, lineStyle:{ color:"#c7cbd4", width:2 } },
  ],
}));
const sellerTypePoints = computed(() => byField("sellertype", 8));
const sellerTypeShareOption = computed<EChartsOption>(() => ({
  grid:{ left:68,right:18,top:24,bottom:24,containLabel:true },
  tooltip:{ trigger:"axis", axisPointer:{ type:"shadow" } },
  legend:{ top:0, data:["ASIN数量占比","月销量占比"] },
  xAxis:{ type:"value", max:100, axisLabel:{ formatter:"{value}%" } },
  yAxis:{ type:"category", data:sellerTypePoints.value.map(i=>i.label) },
  series:[
    { name:"ASIN数量占比", type:"bar", data:sellerTypePoints.value.map(i=>Number(((i.count/Math.max(snapshotRows.value.length,1))*100).toFixed(2))), itemStyle:{ color:"#f59e0b" } },
    { name:"月销量占比", type:"bar", data:sellerTypePoints.value.map(i=>Number((i.share*100).toFixed(2))), itemStyle:{ color:"#84cc16" } },
  ],
}));
const sellerTypeQualityOption = computed<EChartsOption>(() => {
  const points = sellerTypePoints.value.map(i => {
    const rows = snapshotRows.value.filter(r => (t(r.sellertype)||"未知") === i.label);
    return { ...i, count: avg(rows.map(r=>n(r.reviews))) || 0, extra: avg(rows.map(r=>n(r.rating))) || 0 };
  });
  return {
    grid:{ left:64,right:70,top:40,bottom:40,containLabel:true },
    tooltip:{
      trigger:"axis",
      confine: true,
      position: (pos, _params, _dom, _rect, size) => {
        const x = Math.max(12, pos[0] - size.contentSize[0] / 2);
        const y = Math.max(12, pos[1] - size.contentSize[1] - 14);
        return [x, y];
      },
    },
    legend:{ top:0, data:["平均评分数","平均评分值"] },
    xAxis:{ type:"category", data:points.map(i=>i.label) },
    yAxis:[
      {
        type:"value",
        name:"评分数",
        nameLocation:"middle",
        nameGap:48,
        nameRotate:90,
      },
      {
        type:"value",
        name:"评分值",
        max:5,
        nameLocation:"middle",
        nameGap:52,
        nameRotate:270,
      },
    ],
    series:[
      { name:"平均评分数", type:"bar", data:points.map(i=>i.count), itemStyle:{ color:"#f59e0b" } },
      { name:"平均评分值", type:"line", yAxisIndex:1, smooth:true, data:points.map(i=>i.extra || 0), itemStyle:{ color:"#6cc24a" }, lineStyle:{ color:"#6cc24a", width:2 } },
    ],
  };
});
const sellerNationPoints = computed(() =>
  byField("sellernation", 10).map((item) => ({
    ...item,
    key: item.label,
    name: sellerNationLabel(item.label),
    label: sellerNationLabel(item.label),
  })),
);
const sellerNationOption = computed(() => lineBar(sellerNationPoints.value, "产品数量", "销量占比", undefined, false, false, false, false, "count"));
const listingAgePoints = computed(() => bucket(snapshotRows.value, r => n(r.availabledays), [0,30,90,180,365,730,1095,999999], ["1个月内","1-3个月","3-6个月","6-12个月","1-2年","2-3年","3年以上"]));
const listingAgeOption = computed(() => lineBar(listingAgePoints.value, "产品数量", "销量占比", undefined, false, false, false, false, "count"));
const listingTrendPoints = computed(() => { const total = metric.value.units || 0, map = new Map<string, Dist>(); snapshotRows.value.forEach(r => { const label = dateMs(r) ? String(new Date(dateMs(r)!).getFullYear()) : "未知", cur = map.get(label) || { label, count:0, units:0, share:0, rows: [] as Row[] }; cur.count += 1; cur.units += n(r.totalunits) || 0; cur.rows?.push(r); map.set(label, cur); }); return [...map.values()].sort((a,b)=>a.label.localeCompare(b.label)).map(i => ({ ...i, share: total ? i.units/total : 0 })); });
const listingTrendOption = computed(() => lineBar(listingTrendPoints.value, "产品数量", "销量占比", undefined, false, false, false, false, "count"));
const reviewPoints = computed(() => bucket(snapshotRows.value, r => n(r.reviews), [0,1,50,100,200,300,500,999999], ["无评论数","1-50","50-100","100-200","200-300","300-500","500以上"]));
const reviewOption = computed(() => lineBar(reviewPoints.value, "产品数量", "销量占比", undefined, false, false, false, false, "count"));
const ratingPoints = computed(() => bucket(snapshotRows.value, r => n(r.rating), [0,2,3,3.5,4,4.3,4.5,5.1], ["2.0以下","2.0-3.0","3.0-3.5","3.5-4.0","4.0-4.3","4.3-4.5","4.5以上"]));
const ratingOption = computed(() => lineBar(ratingPoints.value, "产品数量", "销量占比", undefined, false, false, false, false, "count"));
const priceBucketGroups = computed(() => {
  const rowsWithPrice = snapshotRows.value
    .map((row) => ({ row, price: n(row.price) }))
    .filter((item): item is { row: Row; price: number } => item.price !== null)
    .sort((a, b) => a.price - b.price);
  const step = Math.max(1, Math.round(n(priceRangeStep.value) || 1));
  const start = Math.max(0, Math.round(n(priceRangeStart.value) || 0));
  const maxPrice = rowsWithPrice.length ? rowsWithPrice[rowsWithPrice.length - 1].price : start;
  const totalUnits = metric.value.units || 0;
  const groups: Array<{ label: string; min: number; max: number; rows: Row[]; units: number; share: number }> = [];

  groups.push({ label: `0-${start}`, min: 0, max: start, rows: [], units: 0, share: 0 });
  for (let lower = start; lower <= maxPrice; lower += step) {
    groups.push({
      label: `${lower}-${lower + step}`,
      min: lower,
      max: lower + step,
      rows: [],
      units: 0,
      share: 0,
    });
  }

  rowsWithPrice.forEach(({ row, price }) => {
    let targetIndex = 0;
    if (price >= start) {
      targetIndex = 1 + Math.floor((price - start) / step);
      targetIndex = Math.min(targetIndex, groups.length - 1);
    }
    groups[targetIndex].rows.push(row);
    groups[targetIndex].units += n(row.totalunits) || 0;
  });

  groups.forEach((group) => {
    group.share = totalUnits ? group.units / totalUnits : 0;
  });

  return groups;
});
const priceOption = computed(() =>
  lineBar(
    priceBucketGroups.value.map((group) => ({
      label: group.label,
      name: group.label,
      count: group.rows.length,
      units: group.rows.length,
      share: group.share,
    })),
    "产品数量",
    "销量占比",
    undefined,
    false,
    false,
    true,
    false,
    "count",
  ),
);
async function loadAllRows() { const items: Row[] = []; let page = 1, total = 0; do { loadingText.value = total ? `正在读取历史数据... ${items.length} / ${total}` : "正在读取竞品数据..."; const data = await fetchPgItems({ table:"seller_sprite_competitor_items", page, pageSize:1000, sortBy:"year_month", sortDir:"asc" }); items.push(...(data.items || [])); total = Number(data.total || 0); page += 1; if (!data.items?.length) break; } while (items.length < total); allRows.value = items; }
function selectCategory(path: string) { activeCategoryPath.value = path; }
function updateReportQuery(nextYear: number, nextMonth: number) {
  if (nextYear === selectedYear.value && nextMonth === selectedMonth.value) return;
  switchingFilters.value = true;
  router.replace({
    query: {
      ...route.query,
      year: nextYear || undefined,
      month: nextMonth || undefined,
    },
  });
  window.setTimeout(() => {
    switchingFilters.value = false;
  }, 320);
}
function handleYearChange(event: Event) {
  const nextYear = Number((event.target as HTMLSelectElement).value || 0) || 0;
  const allowedMonths = [...new Set(
    historyByMonth.value
      .filter((item) => !nextYear || Math.floor(item.ym / 100) === nextYear)
      .map((item) => item.ym % 100),
  )];
  const nextMonth = selectedMonth.value && allowedMonths.includes(selectedMonth.value) ? selectedMonth.value : 0;
  updateReportQuery(nextYear, nextMonth);
}
function handleMonthChange(event: Event) {
  const nextMonth = Number((event.target as HTMLSelectElement).value || 0) || 0;
  updateReportQuery(selectedYear.value, nextMonth);
}
function openPriceBucketModal(params: unknown) {
  const dataIndex = Number((params as { dataIndex?: number })?.dataIndex ?? -1);
  if (dataIndex < 0) return;
  const target = priceBucketGroups.value[dataIndex];
  if (!target) return;
  priceBucketModal.value = {
    open: true,
    title: "商品销量分布",
    label: target.label,
    rows: [...target.rows].sort((a, b) => (n(b.totalunits) || 0) - (n(a.totalunits) || 0)),
  };
}
function openAggregateBucketModal(mode: "brand" | "seller", params: unknown) {
  const dataIndex = Number((params as { dataIndex?: number })?.dataIndex ?? -1);
  if (dataIndex < 0) return;
  const target = concentration(mode)[dataIndex];
  if (!target?.name) return;
  const rows = snapshotRows.value
    .filter((row) => (mode === "brand" ? t(row.brand) : t(row.sellername)) === target.name)
    .sort((a, b) => (n(b.totalunits) || 0) - (n(a.totalunits) || 0));
  if (!rows.length) return;
  priceBucketModal.value = {
    open: true,
    title: mode === "brand" ? "品牌商品销量分布" : "卖家商品销量分布",
    label: target.name,
    rows,
  };
}
function openBrandBucketModal(params: unknown) {
  openAggregateBucketModal("brand", params);
}
function openSellerBucketModal(params: unknown) {
  openAggregateBucketModal("seller", params);
}
function openSellerNationBucketModal(params: unknown) {
  const dataIndex = Number((params as { dataIndex?: number })?.dataIndex ?? -1);
  if (dataIndex < 0) return;
  const target = sellerNationPoints.value[dataIndex];
  if (!target?.key) return;
  const rows = snapshotRows.value
    .filter((row) => (t(row.sellernation) || "未知") === target.key)
    .sort((a, b) => (n(b.totalunits) || 0) - (n(a.totalunits) || 0));
  priceBucketModal.value = {
    open: true,
    title: "属地商品销量分布",
    label: target.label,
    rows,
  };
}
function openRowsBucketModal(title: string, points: Dist[], params: unknown) {
  const dataIndex = Number((params as { dataIndex?: number })?.dataIndex ?? -1);
  if (dataIndex < 0) return;
  const target = points[dataIndex];
  const rows = [...(target?.rows || [])].sort((a, b) => (n(b.totalunits) || 0) - (n(a.totalunits) || 0));
  if (!target || !rows.length) return;
  priceBucketModal.value = {
    open: true,
    title,
    label: target.label,
    rows,
  };
}
function openListingAgeBucketModal(params: unknown) {
  openRowsBucketModal("上架时间商品分布", listingAgePoints.value, params);
}
function openListingTrendBucketModal(params: unknown) {
  openRowsBucketModal("上架趋势商品分布", listingTrendPoints.value, params);
}
function openReviewBucketModal(params: unknown) {
  openRowsBucketModal("评论数商品分布", reviewPoints.value, params);
}
function openRatingBucketModal(params: unknown) {
  openRowsBucketModal("评分值商品分布", ratingPoints.value, params);
}
function closePriceBucketModal() {
  priceBucketModal.value.open = false;
  priceBucketTooltipIndex = -1;
}
function amazonUrl(asin: unknown): string {
  const value = t(asin).trim();
  return value ? `https://www.amazon.com/dp/${encodeURIComponent(value)}` : "https://www.amazon.com";
}
function openPriceBucketAmazon(params: unknown) {
  const dataIndex = Number((params as { dataIndex?: number })?.dataIndex ?? -1);
  if (dataIndex < 0) return;
  const point = priceBucketModalPoints.value[dataIndex];
  if (!point?.asin) return;
  window.open(amazonUrl(point.asin), "_blank", "noopener,noreferrer");
}
function openProductAmazon(params: unknown) {
  const dataIndex = Number((params as { dataIndex?: number })?.dataIndex ?? -1);
  if (dataIndex < 0) return;
  const point = concentration("product")[dataIndex];
  const asin = point?.row ? t(point.row.asin) : "";
  if (!asin) return;
  window.open(amazonUrl(asin), "_blank", "noopener,noreferrer");
}
const exportReport = () => window.print();
const goBack = () => router.push({ name:"competitors", query:{ year:selectedYear.value || undefined, month:selectedMonth.value || undefined } });
onMounted(async () => {
  loading.value = true;
  error.value = "";
  try {
    await loadAllRows();
    if (!allRows.value.length) error.value = "没有可用于生成报告的竞品数据。";
    if (!activeCategoryPath.value && categoryOptions.value.length) activeCategoryPath.value = categoryOptions.value[0].path;
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
  position: relative;
  padding-top: 1rem;
}

.report-switch-mask {
  position: absolute;
  inset: 0;
  z-index: 12;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  gap: 10px;
  padding-top: 72px;
  background: rgba(239, 246, 255, 0.72);
  backdrop-filter: blur(1.5px);
}

.report-hero,
.report-panel,
.report-loading-panel {
  border-radius: 14px;
  padding: 16px 18px;
  background: rgba(255, 255, 255, 0.94);
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

.report-actions,
.report-chip-group {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
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

.table-loading-spinner {
  width: 18px;
  height: 18px;
  border-radius: 50%;
  border: 2px solid rgba(47, 99, 218, 0.18);
  border-top-color: #2f63da;
  animation: table-loading-spin 0.8s linear infinite;
}

.table-loading-text {
  font-size: 13px;
  color: #1f2f4a;
  font-weight: 600;
}

.report-stats-grid,
.report-overview-grid,
.report-grid,
.report-mini-grid {
  display: grid;
  gap: 12px;
}

.report-stats-grid {
  grid-template-columns: repeat(4, minmax(0, 1fr));
  margin-bottom: 16px;
}

.report-overview-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.report-grid-2 {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.report-grid-3 {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.report-grid-tight {
  margin-bottom: 0;
}

.report-price-panel {
  grid-column: 1 / -1;
}

.report-stat-card,
.report-mini-card,
.report-card {
  border-radius: 14px;
  padding: 14px 16px;
  background: linear-gradient(180deg, #fff 0%, #f8fbff 100%);
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

.report-panel {
  margin-bottom: 16px;
}

.report-panel-header {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  margin-bottom: 14px;
}

.report-panel-header h3,
.report-card-title {
  margin: 0;
  font-size: 1.05rem;
  color: #132542;
}

.report-chip {
  border-radius: 999px;
  padding: 4px 10px;
  background: #eff6ff;
  color: #36518b;
  font-size: 12px;
  font-weight: 600;
}

.report-filter-layout,
.report-filter-group {
  display: flex;
  gap: 12px;
}

.report-filter-layout {
  justify-content: space-between;
  align-items: flex-start;
  flex-wrap: wrap;
}

.report-filter-group {
  align-items: flex-start;
  flex-wrap: wrap;
}

.report-filter-stack {
  width: 100%;
}

.report-filter-group-category {
  flex: 1 1 680px;
}

.report-filter-field {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 120px;
}

.report-filter-label {
  color: #5b6b82;
  font-size: 13px;
  font-weight: 600;
}

.report-filter-select {
  min-width: 120px;
  height: 40px;
  padding: 0 12px;
  border: 1px solid #c9ddff;
  border-radius: 10px;
  background: #f8fbff;
  color: #132542;
  font-size: 13px;
}

.report-filter-field-inline {
  min-width: 96px;
}

.report-filter-input {
  width: 100%;
  height: 40px;
  padding: 0 12px;
  border: 1px solid #c9ddff;
  border-radius: 10px;
  background: #f8fbff;
  color: #132542;
  font-size: 13px;
}

.report-price-controls {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}

.report-tab-row {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.report-tab-btn {
  border: 1px solid #c9ddff;
  background: #f8fbff;
  color: #35538d;
  border-radius: 10px;
  padding: 10px 14px;
  font-size: 13px;
  line-height: 1.4;
  cursor: pointer;
}

.report-tab-btn.active {
  background: #2f63da;
  border-color: #2f63da;
  color: #fff;
}

.report-card-title {
  margin-bottom: 10px;
}

.report-card-title-with-tools {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.report-card-tools {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: nowrap;
}

.report-filter-select-sm,
.report-filter-input-sm {
  min-width: 136px;
  height: 34px;
  width: 136px;
  box-sizing: border-box;
}

.report-table,
.report-metric-table {
  width: 100%;
  border-collapse: collapse;
}

.report-table th,
.report-table td,
.report-metric-table th,
.report-metric-table td {
  padding: 10px 8px;
  border-bottom: 1px solid #edf2fb;
  font-size: 13px;
}

.report-table th,
.report-metric-table th {
  text-align: left;
  color: #5b6b82;
  font-weight: 700;
}

.report-table td,
.report-metric-table td {
  text-align: right;
  color: #12203c;
}

.report-table th,
.report-table td {
  text-align: center;
}

.report-table th:first-child,
.report-table td:first-child,
.report-metric-table th:first-child,
.report-metric-table td:first-child {
  text-align: left;
}

.report-matrix-table {
  table-layout: fixed;
}

.report-matrix-table th,
.report-matrix-table td {
  text-align: center;
}

.report-matrix-table .report-col-year {
  width: 90px;
}

.report-matrix-table .report-col-metric {
  width: 140px;
}

.report-matrix-table .report-col-month {
  width: 72px;
}

.report-matrix-table .report-col-total {
  width: 90px;
}

.report-matrix-table th:first-child,
.report-matrix-table td:first-child,
.report-matrix-table td:nth-child(2) {
  text-align: left;
}

.report-matrix-table th:nth-child(2) {
  text-align: left;
}

.report-modal-mask {
  position: fixed;
  inset: 0;
  z-index: 30;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  background: rgba(15, 23, 42, 0.34);
}

.report-modal-card {
  width: min(1100px, 100%);
  max-height: min(80vh, 880px);
  overflow: hidden;
  border-radius: 16px;
  background: #fff;
  border: 1px solid #dbeafe;
  box-shadow: 0 24px 80px rgba(15, 23, 42, 0.18);
}

.report-modal-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  padding: 18px 20px;
  border-bottom: 1px solid #edf2fb;
}

.report-modal-header h3 {
  margin: 0;
  font-size: 1.05rem;
  color: #132542;
}

.report-modal-header p {
  margin: 6px 0 0;
  color: #64748b;
  font-size: 13px;
}

.report-modal-close {
  width: 36px;
  height: 36px;
  border: 1px solid #dbeafe;
  border-radius: 10px;
  background: #f8fbff;
  color: #4f5f7b;
  font-size: 22px;
  line-height: 1;
  cursor: pointer;
}

.report-modal-body {
  max-height: calc(80vh - 88px);
  overflow: auto;
  padding: 12px 20px 20px;
}

.report-modal-table th,
.report-modal-table td {
  vertical-align: top;
}

.report-modal-title-cell {
  min-width: 320px;
  max-width: 520px;
  white-space: normal;
  line-height: 1.5;
}

@media print {
  .report-actions {
    display: none;
  }
}

@media (max-width: 1200px) {
  .report-stats-grid,
  .report-overview-grid,
  .report-grid-3 {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 900px) {
  .report-stats-grid,
  .report-overview-grid,
  .report-grid-2,
  .report-grid-3,
  .report-mini-grid {
    grid-template-columns: 1fr;
  }

  .report-price-controls {
    width: 100%;
  }
}

@media (max-width: 768px) {
  .report-hero {
    display: grid;
  }
}

@keyframes table-loading-spin {
  to {
    transform: rotate(360deg);
  }
}
</style>

