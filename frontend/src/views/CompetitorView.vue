<template>
  <main class="dashboard competitor-page">
    <section class="table-panel competitor-panel">
      <div class="table-filters toolbar-filters competitor-filters">
        <label>
          年份
          <select v-model.number="selectedYear">
            <option :value="0">全部</option>
            <option v-for="item in yearOptions" :key="`year-${item}`" :value="item">
              {{ item }}
            </option>
          </select>
        </label>
        <label>
          月份
          <select v-model.number="selectedMonth">
            <option :value="0">全部</option>
            <option v-for="item in monthOptions" :key="`month-${item}`" :value="item">
              {{ String(item).padStart(2, "0") }}
            </option>
          </select>
        </label>
        <label>
          排序字段
          <select v-model="sortBy">
            <option v-for="item in sortFieldOptions" :key="`sort-${item.value}`" :value="item.value">
              {{ item.label }}
            </option>
          </select>
        </label>
        <label>
          排序
          <select v-model="sortDir">
            <option value="desc">降序</option>
            <option value="asc">升序</option>
          </select>
        </label>
        <button class="secondary-btn" type="button" @click="openReport">查看报告</button>
      </div>

      <div v-if="error" class="table-error">{{ error }}</div>

      <div class="competitor-list-shell" ref="tableWrapRef" @scroll.passive="onTableWrapScroll">
        <div class="competitor-head-card">
          <table class="data-table competitor-table competitor-head-table">
            <thead>
              <tr>
                <th class="serial-col">序号</th>
                <th
                  v-for="col in displayColumns"
                  :key="`head-${col}`"
                  :data-col="col"
                  class="sortable-header"
                  :class="{
                    'drag-source-col': dragGhost.active && draggingCol === col,
                    'filtered-col': isColumnValueFiltered(col),
                  }"
                  :style="columnStyle(col)"
                  @pointerdown="onHeaderPointerDown(col, $event)"
                >
                  <div class="th-inner">
                    <div class="th-stack">
                      <span v-for="(line, idx) in columnLines(col)" :key="`${col}-line-${idx}`">
                        {{ line }}
                      </span>
                    </div>
                    <button
                      v-if="isFilterableColumn(col)"
                      class="filter-btn"
                      :class="{ active: isColumnValueFiltered(col) }"
                      title="筛选"
                      @pointerdown.stop
                      @click.stop="openFilterMenu(col, $event)"
                    >
                      ▼
                    </button>
                  </div>
                </th>
              </tr>
            </thead>
          </table>
        </div>

        <table class="data-table competitor-table competitor-body-table">
          <tbody>
            <tr v-if="!rows.length && !loading" class="empty-row">
              <td :colspan="displayColumns.length + 1">暂无数据</td>
            </tr>

            <template v-for="(row, index) in rows" :key="rowKey(row, index)">
              <tr class="competitor-main-row">
                <td class="serial-col serial-cell">{{ rowSerial(index) }}</td>

                <template v-for="col in displayColumns" :key="`${rowKey(row, index)}-${col}`">
                  <td :class="columnCellClass(col)" :style="columnStyle(col)">
                    <template v-if="col === 'product'">
                      <div class="product-body">
                        <img
                          v-if="cellText(row.imageurl)"
                          class="product-image"
                          :src="cellText(row.imageurl)"
                          :alt="cellText(row.title) || cellText(row.asin) || 'product-image'"
                          loading="lazy"
                        />
                        <div class="product-copy">
                          <button
                            type="button"
                            class="product-title product-title-copy"
                            :title="`${cellText(row.title) || cellText(row.asin) || '-'}（点击复制）`"
                            @click="copyText(cellText(row.title) || cellText(row.asin))"
                          >
                            {{ cellText(row.title) || cellText(row.asin) }}
                          </button>

                          <div class="product-line">
                            <span class="product-meta-label">ASIN:</span>
                            <a
                              v-if="cellText(row.asin)"
                              class="product-meta-link"
                              :href="amazonUrl(row.asin)"
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              {{ cellText(row.asin) }}
                            </a>
                            <span v-else>-</span>
                          </div>
                          <div v-if="cellText(row.parent)" class="product-line">
                            <span class="product-meta-label">父ASIN:</span>
                            <a
                              class="product-meta-link"
                              :href="amazonUrl(row.parent)"
                              target="_blank"
                              rel="noopener noreferrer"
                            >
                              {{ cellText(row.parent) }}
                            </a>
                          </div>
                          <div class="product-line">
                            <span class="product-meta-label">品牌:</span>
                            <span>{{ cellText(row.brand) || "-" }}</span>
                          </div>
                        </div>
                      </div>
                    </template>

                    <template v-else-if="col === 'bsrrank'">
                      <div class="metric-main">{{ formatNumber(row.bsrrank) }}</div>
                      <div class="metric-sub down">{{ formatSigned(row.bsrrankcv) }}</div>
                      <div class="metric-sub down">{{ formatPercent(row.bsrrankcr) }}</div>
                    </template>

                    <template v-else-if="col === 'trend'">
                      <div class="trend-inner">
                        <TableTrendMiniChart
                          :raw-trend="row.trends || row.salestrend"
                          @open="openTrendModalFromRow(row, $event)"
                        />
                      </div>
                    </template>

                    <template v-else-if="col === 'totalunits'">
                      <div class="metric-main">{{ formatNumber(row.totalunits) }}</div>
                      <div class="metric-sub accent">{{ formatPercent(row.totalunitsgrowth) }}</div>
                    </template>

                    <template v-else-if="col === 'totalamount'">
                      <div class="metric-main">{{ formatCurrency(row.totalamount) }}</div>
                    </template>

                    <template v-else-if="col === 'amzunit'">
                      <div class="metric-main">{{ formatCompact(row.amzunit) }}</div>
                      <div class="metric-sub">{{ formatCurrency(row.subtotalamount) }}</div>
                    </template>

                    <template v-else-if="col === 'variations'">
                      <div class="metric-main">{{ formatNumber(row.variations) }}</div>
                    </template>

                    <template v-else-if="col === 'price'">
                      <div class="metric-main">{{ formatCurrency(row.price) }}</div>
                      <div class="metric-sub">{{ formatNumber(row.questions) }}</div>
                    </template>

                    <template v-else-if="col === 'reviews'">
                      <div class="metric-main">{{ formatNumber(row.reviews) }}</div>
                      <div class="metric-sub">{{ formatSigned(row.reviewsincreasement) }}</div>
                    </template>

                    <template v-else-if="col === 'rating'">
                      <div class="metric-main">{{ formatRating(row.rating) }}</div>
                      <div class="metric-sub">{{ formatPercent(row.reviewsrate) }}</div>
                    </template>

                    <template v-else-if="col === 'fba'">
                      <div class="metric-main">{{ formatCurrency(row.fba) }}</div>
                      <div class="metric-sub">{{ formatPercent(row.profit) }}</div>
                    </template>

                    <template v-else-if="col === 'availabledate'">
                      <div class="metric-main">{{ formatDate(row.availabledate) }}</div>
                      <div class="metric-sub">{{ formatDays(row.availabledays) }}</div>
                    </template>

                    <template v-else-if="col === 'delivery'">
                      <div class="metric-main">{{ cellText(row.sellertype) || "-" }}</div>
                      <div class="metric-sub">{{ cellText(row.sellernation) || "-" }}</div>
                    </template>
                  </td>
                </template>
              </tr>

              <tr class="competitor-detail-row">
                <td class="serial-col detail-anchor"></td>
                <td :colspan="displayColumns.length">
                  <div class="detail-grid">
                    <div class="detail-line detail-wide">
                      <span class="detail-label">浏览类目:</span>
                      <span class="detail-value detail-path">{{ cellText(row.nodelabelpath) || "-" }}</span>
                    </div>
                    <div class="detail-line detail-wide">
                      <span class="detail-label">SKU:</span>
                      <span class="detail-value">{{ cellText(row.sku) || "-" }}</span>
                    </div>
                    <div class="detail-line">
                      <span class="detail-label">LQS:</span>
                      <span class="detail-value">{{ formatNumber(row.lqs) }}</span>
                    </div>
                    <div class="detail-line">
                      <span class="detail-label">卖家数:</span>
                      <span class="detail-value">{{ formatNumber(row.sellers) }}</span>
                    </div>
                    <div class="detail-line">
                      <span class="detail-label">卖家:</span>
                      <span class="detail-value">{{ cellText(row.sellername) || "-" }}</span>
                    </div>
                    <div class="detail-line">
                      <span class="detail-label">重量:</span>
                      <span class="detail-value">{{ cellText(row.weight) || "-" }}</span>
                    </div>
                    <div class="detail-line">
                      <span class="detail-label">商品尺寸:</span>
                      <span class="detail-value">{{ cellText(row.dimensions) || "-" }}</span>
                    </div>
                    <div class="detail-line">
                      <span class="detail-label">包装重量:</span>
                      <span class="detail-value">{{ cellText(row.pkgweight) || "-" }}</span>
                    </div>
                    <div class="detail-line">
                      <span class="detail-label">包装尺寸:</span>
                      <span class="detail-value">{{ cellText(row.pkgdimensions) || "-" }}</span>
                    </div>
                  </div>
                </td>
              </tr>
            </template>
          </tbody>
        </table>

        <div
          v-if="dragGhost.active && insertLineLeft !== null"
          class="column-insert-line"
          :style="{ left: `${insertLineLeft}px` }"
        />

        <div v-if="loading" class="table-loading-mask" aria-live="polite" aria-busy="true">
          <span class="table-loading-spinner" />
          <span class="table-loading-text">加载中...</span>
        </div>

        <div v-if="loadingMore" class="competitor-lazy-state">
          <span class="mini-spinner" />
          <span>加载更多中...</span>
        </div>
        <div v-else-if="hasMoreRows" class="competitor-lazy-state">向下滚动到底自动加载</div>
        <div v-else-if="rows.length" class="competitor-lazy-state">已加载全部</div>
      </div>

      <div
        v-if="dragGhost.active"
        class="column-drag-ghost"
        :style="{ left: `${dragGhost.x}px`, top: `${dragGhost.y}px`, width: `${dragGhost.width}px` }"
      >
        <div class="column-drag-ghost-header">{{ columnLabel(draggingCol) }}</div>
        <div v-for="(item, idx) in dragPreviewValues" :key="`preview-${idx}`" class="column-drag-ghost-cell">
          {{ item }}
        </div>
      </div>

      <div
        v-if="filterMenu.open"
        ref="filterMenuRef"
        class="excel-filter-pop"
        :style="{ left: `${filterMenu.x}px`, top: `${filterMenu.y}px` }"
      >
        <div class="excel-filter-title-row">
          <div class="excel-filter-title">{{ columnLabel(filterMenu.columnKey) }}</div>
          <div class="excel-filter-count">{{ workingFilterValues.length }} / {{ filterOptions.length }}</div>
        </div>
        <input
          v-model="filterMenu.keyword"
          class="excel-filter-search"
          placeholder="搜索筛选值"
          @keydown.stop
        />
        <div class="excel-filter-advanced">
          <select v-model="filterTextRule.op" class="excel-filter-op">
            <option v-for="op in textFilterOpOptions" :key="op.value" :value="op.value">
              {{ op.label }}
            </option>
          </select>
          <input
            v-if="filterRuleNeedsValue"
            v-model="filterTextRule.value"
            class="excel-filter-rule-value"
            placeholder="条件值"
            @keydown.stop
          />
          <button class="secondary-btn" @click="applyTextRuleFilter">应用条件</button>
        </div>
        <div class="excel-filter-actions">
          <button class="secondary-btn" @click="selectAllFilterValues">全选</button>
          <button class="secondary-btn" @click="invertFilterSelection">反选</button>
          <button class="secondary-btn" @click="clearFilterSelection">清空选择</button>
          <button class="secondary-btn" @click="clearColumnFilter">清空筛选</button>
        </div>
        <div class="excel-filter-list">
          <div v-if="filterOptionsLoading" class="excel-filter-loading">
            <span class="mini-spinner" />
            <span>加载选项中...</span>
          </div>
          <label v-else v-for="option in visibleFilterOptions" :key="option.value" class="excel-filter-option">
            <input
              type="checkbox"
              :checked="workingFilterSet.has(option.value)"
              @change="toggleWorkingFilterValue(option.value, $event)"
            />
            <span class="option-label">{{ option.label }}</span>
            <span class="option-count">{{ option.count }}</span>
          </label>
          <div v-if="!filterOptionsLoading && !visibleFilterOptions.length" class="excel-filter-empty">无可选值</div>
        </div>
        <div class="excel-filter-footer">
          <button class="secondary-btn" @click="closeFilterMenu">取消</button>
          <button class="primary-btn small" @click="applyFilterMenu">应用</button>
        </div>
      </div>

      <div class="competitor-footer-controls">
        <label class="rows-control">
          显示行数
          <input v-model.number.lazy="pageSize" type="number" min="1" step="1" />
        </label>
        <span class="lazy-hint">已加载 {{ rows.length }} / {{ total }}</span>
        <span v-if="loadingMore" class="lazy-loading-inline">
          <span class="mini-spinner" />
          <span>加载更多中...</span>
        </span>
        <span v-else-if="hasMoreRows" class="lazy-hint">向下滚动到底自动加载</span>
        <span v-else-if="rows.length" class="lazy-hint">已加载全部</span>
      </div>
    </section>

    <TrendDataModal
      :open="trendModalOpen"
      :title="trendModalTitle"
      :points="trendModalPoints"
      @close="trendModalOpen = false"
    />

    <div v-if="copyToast" class="copy-toast">{{ copyToast }}</div>
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";

import { fetchPgFilterOptions, fetchPgItems, fetchPgYearMonthsByTable } from "@/api/data";
import TableTrendMiniChart from "@/components/TableTrendMiniChart.vue";
import TrendDataModal from "@/components/TrendDataModal.vue";
import type { PgFilterOption, PgTrendPoint } from "@/types/data";

const LONG_PRESS_MS = 220;
const TARGET_TABLE = "seller_sprite_competitor_items";
const router = useRouter();

type TextFilterOp =
  | "contains"
  | "contains_word"
  | "not_contains"
  | "starts_with"
  | "ends_with"
  | "equals"
  | "not_equals"
  | "greater_than"
  | "less_than"
  | "is_blank"
  | "is_not_blank";

type TextFilterRule = { op: TextFilterOp; value?: string };
type TextFilterValue = string | TextFilterRule;

type ColumnDef = {
  key: string;
  label: string | string[];
  width: string;
  filterField?: string;
};

const textFilterOpOptions: Array<{ value: TextFilterOp; label: string }> = [
  { value: "contains", label: "包含" },
  { value: "contains_word", label: "包含完整词" },
  { value: "not_contains", label: "不包含" },
  { value: "starts_with", label: "开头是" },
  { value: "ends_with", label: "结尾是" },
  { value: "equals", label: "等于" },
  { value: "not_equals", label: "不等于" },
  { value: "greater_than", label: "大于" },
  { value: "less_than", label: "小于" },
  { value: "is_blank", label: "为空" },
  { value: "is_not_blank", label: "非空" },
];

const COLUMN_DEFS: ColumnDef[] = [
  { key: "product", label: "产品信息", width: "23%", filterField: "title" },
  { key: "bsrrank", label: "大类BSR", width: "5%", filterField: "bsrrank" },
  { key: "trend", label: "销量趋势", width: "12%" },
  { key: "totalunits", label: ["销量(父)", "增长率"], width: "6%", filterField: "totalunits" },
  { key: "totalamount", label: "销售额", width: "7%", filterField: "totalamount" },
  { key: "amzunit", label: ["子体销量", "子体销售额"], width: "7%", filterField: "amzunit" },
  { key: "variations", label: "变体数", width: "4.5%", filterField: "variations" },
  { key: "price", label: ["价格", "Q&A"], width: "5.5%", filterField: "price" },
  { key: "reviews", label: ["评论数", "月新增"], width: "5.5%", filterField: "reviews" },
  { key: "rating", label: ["评分", "留评率"], width: "5%", filterField: "rating" },
  { key: "fba", label: ["FBA", "毛利率"], width: "5%", filterField: "fba" },
  { key: "availabledate", label: "上架时间", width: "7%", filterField: "availabledate" },
  { key: "delivery", label: ["配送", "卖家国家"], width: "7.5%", filterField: "sellernation" },
];

const COLUMN_MAP = new Map(COLUMN_DEFS.map((item) => [item.key, item]));

const sortFieldOptions = [
  { value: "totalunits", label: "销量" },
  { value: "totalamount", label: "销售额" },
  { value: "bsrrank", label: "BSR" },
  { value: "price", label: "价格" },
  { value: "reviews", label: "评论数" },
  { value: "rating", label: "评分" },
  { value: "availabledate", label: "上架时间" },
];

const rows = ref<Record<string, unknown>[]>([]);
const total = ref(0);
const page = ref(0);
const pageSize = ref(20);
const sortBy = ref("totalunits");
const sortDir = ref<"asc" | "desc">("desc");
const loading = ref(false);
const loadingMore = ref(false);
const error = ref("");
const yearMonthOptions = ref<number[]>([]);
const selectedYear = ref(0);
const selectedMonth = ref(0);

const orderedColumns = ref<string[]>(COLUMN_DEFS.map((item) => item.key));
const textFilters = ref<Record<string, TextFilterValue>>({});
const valueFilters = ref<Record<string, string[]>>({});

const tableWrapRef = ref<HTMLElement | null>(null);
const filterMenuRef = ref<HTMLElement | null>(null);
const draggingCol = ref("");
const dragGhost = ref({
  active: false,
  x: 0,
  y: 0,
  width: 240,
});
const insertLineLeft = ref<number | null>(null);
const dropIndex = ref<number | null>(null);

const filterMenu = ref({
  open: false,
  columnKey: "",
  field: "",
  x: 0,
  y: 0,
  keyword: "",
});
const filterTextRule = ref<{ op: TextFilterOp; value: string }>({
  op: "contains",
  value: "",
});
const filterOptions = ref<PgFilterOption[]>([]);
const filterOptionsLoading = ref(false);
const workingFilterValues = ref<string[]>([]);
const trendModalOpen = ref(false);
const trendModalTitle = ref("");
const trendModalPoints = ref<PgTrendPoint[]>([]);
const copyToast = ref("");

let longPressTimer: ReturnType<typeof setTimeout> | null = null;
let filterTimer: ReturnType<typeof setTimeout> | null = null;
let activeFilterRequestSeq = 0;
let scrollCheckRaf = 0;
let copyToastTimer: ReturnType<typeof setTimeout> | null = null;

const displayColumns = computed(() => {
  const base = COLUMN_DEFS.map((item) => item.key);
  const baseSet = new Set(base);
  const ordered = orderedColumns.value.filter((key) => baseSet.has(key));
  const missing = base.filter((key) => !ordered.includes(key));
  return [...ordered, ...missing];
});
const hasMoreRows = computed(() => rows.value.length < total.value);
const workingFilterSet = computed(() => new Set(workingFilterValues.value));
const visibleFilterOptions = computed(() => filterOptions.value);
const filterRuleNeedsValue = computed(
  () => filterTextRule.value.op !== "is_blank" && filterTextRule.value.op !== "is_not_blank"
);
const dragPreviewValues = computed(() =>
  rows.value.slice(0, 8).map((row) => previewText(row, draggingCol.value)).filter(Boolean)
);
const yearOptions = computed(() => {
  const years = new Set<number>();
  for (const ym of yearMonthOptions.value) years.add(Math.floor(ym / 100));
  return Array.from(years).sort((a, b) => b - a);
});
const monthOptions = computed(() => {
  const months = new Set<number>();
  for (const ym of yearMonthOptions.value) {
    const year = Math.floor(ym / 100);
    const month = ym % 100;
    if (selectedYear.value === 0 || selectedYear.value === year) {
      months.add(month);
    }
  }
  return Array.from(months).sort((a, b) => a - b);
});

function getColumnDef(key: string): ColumnDef | undefined {
  return COLUMN_MAP.get(key);
}

function columnLines(key: string): string[] {
  const label = getColumnDef(key)?.label ?? key;
  return Array.isArray(label) ? label : [label];
}

function columnLabel(key: string): string {
  return columnLines(key).join("");
}

function columnStyle(key: string): Record<string, string> {
  const def = getColumnDef(key);
  return def ? { width: def.width } : {};
}

function columnCellClass(key: string): string {
  if (key === "product") return "product-cell";
  if (key === "trend") return "trend-cell";
  return "metric-cell";
}

function filterFieldFor(key: string): string {
  return getColumnDef(key)?.filterField || "";
}

function isFilterableColumn(key: string): boolean {
  return Boolean(filterFieldFor(key));
}

function cellText(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function toNumber(value: unknown): number | null {
  if (value == null || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function rowKey(row: Record<string, unknown>, index: number): string {
  return `${cellText(row.id_2 || row.asin || row.title || index)}-${index}`;
}

function rowSerial(index: number): number {
  return index + 1;
}

function formatNumber(value: unknown): string {
  const num = toNumber(value);
  return num === null ? "-" : num.toLocaleString();
}

function formatCompact(value: unknown): string {
  const num = toNumber(value);
  if (num === null) return "-";
  if (Math.abs(num) >= 1000) {
    return `${(num / 1000).toFixed(num >= 10000 ? 0 : 1).replace(/\.0$/, "")}K+`;
  }
  return num.toLocaleString();
}

function formatCurrency(value: unknown): string {
  const num = toNumber(value);
  if (num === null) return "-";
  return `$${num.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function formatPercent(value: unknown): string {
  const num = toNumber(value);
  if (num === null) return "-";
  return `${num.toLocaleString(undefined, { maximumFractionDigits: 2 })}%`;
}

function formatSigned(value: unknown): string {
  const num = toNumber(value);
  if (num === null) return "-";
  return `${num > 0 ? "+" : ""}${num.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function formatRating(value: unknown): string {
  const num = toNumber(value);
  return num === null ? "-" : num.toFixed(1);
}

function formatDate(value: unknown): string {
  const num = toNumber(value);
  if (num === null) return "-";
  const date = new Date(num);
  if (Number.isNaN(date.getTime())) return "-";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDays(value: unknown): string {
  const num = toNumber(value);
  if (num === null) return "-";
  if (num >= 365) {
    return `${Math.floor(num / 365)}年${Math.floor((num % 365) / 30)}个月`;
  }
  if (num >= 30) {
    return `${Math.floor(num / 30)}个月`;
  }
  return `${num}天`;
}

function amazonUrl(asin: unknown): string {
  const value = cellText(asin).trim();
  return value ? `https://www.amazon.com/dp/${encodeURIComponent(value)}` : "https://www.amazon.com";
}

async function copyText(value: string): Promise<void> {
  const text = String(value || "").trim();
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
    showCopyToast("标题已复制");
  } catch {
    try {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.style.position = "fixed";
      textarea.style.left = "-9999px";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
      showCopyToast("标题已复制");
    } catch {
      showCopyToast("复制失败");
    }
  }
}

function showCopyToast(message: string): void {
  copyToast.value = message;
  if (copyToastTimer) clearTimeout(copyToastTimer);
  copyToastTimer = setTimeout(() => {
    copyToast.value = "";
    copyToastTimer = null;
  }, 1400);
}

function previewText(row: Record<string, unknown>, col: string): string {
  switch (col) {
    case "product":
      return cellText(row.title || row.asin) || "-";
    case "bsrrank":
      return formatNumber(row.bsrrank);
    case "trend":
      return "销量趋势";
    case "totalunits":
      return formatNumber(row.totalunits);
    case "totalamount":
      return formatCurrency(row.totalamount);
    case "amzunit":
      return formatCompact(row.amzunit);
    case "variations":
      return formatNumber(row.variations);
    case "price":
      return formatCurrency(row.price);
    case "reviews":
      return formatNumber(row.reviews);
    case "rating":
      return formatRating(row.rating);
    case "fba":
      return formatCurrency(row.fba);
    case "availabledate":
      return formatDate(row.availabledate);
    case "delivery":
      return cellText(row.sellernation || row.sellertype) || "-";
    default:
      return "-";
  }
}

function openTrendModalFromRow(row: Record<string, unknown>, points: PgTrendPoint[]): void {
  if (!points.length) return;
  const title = String(row.title ?? row.asin ?? "").trim();
  trendModalTitle.value = title ? `${title} 趋势数据` : "趋势数据";
  trendModalPoints.value = points;
  trendModalOpen.value = true;
}

function openReport(): void {
  void router.push({
    name: "competitor-report",
    query: {
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
    },
  });
}

function normalizedValueFilters(): Record<string, string[]> {
  const output: Record<string, string[]> = {};
  for (const [key, values] of Object.entries(valueFilters.value)) {
    if (Array.isArray(values) && values.length > 0) output[key] = values;
  }
  return output;
}

function normalizedTextFilters(): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(textFilters.value)) {
    if (typeof value === "string") {
      const normalized = value.trim();
      if (normalized) output[key] = normalized;
      continue;
    }
    if (!value || typeof value !== "object") continue;
    const op = String(value.op || "").trim() as TextFilterOp;
    if (!op) continue;
    if (op === "is_blank" || op === "is_not_blank") {
      output[key] = { op };
      continue;
    }
    const normalized = String(value.value ?? "").trim();
    if (normalized) output[key] = { op, value: normalized };
  }
  return output;
}

function isColumnValueFiltered(columnKey: string): boolean {
  const field = filterFieldFor(columnKey);
  if (!field) return false;
  if (Array.isArray(valueFilters.value[field]) && valueFilters.value[field].length > 0) return true;
  const textFilter = textFilters.value[field];
  if (!textFilter) return false;
  if (typeof textFilter === "string") return textFilter.trim().length > 0;
  return Boolean(textFilter.op);
}

function moveColumnToInsertIndex(col: string, insertIndex: number): void {
  const current = [...displayColumns.value];
  const from = current.indexOf(col);
  if (from < 0) return;
  current.splice(from, 1);

  let target = Math.max(0, Math.min(insertIndex, current.length));
  if (target > from) target -= 1;
  current.splice(target, 0, col);
  orderedColumns.value = current;
}

function getHeaderMetrics(): Array<{ left: number; right: number; center: number }> {
  const wrap = tableWrapRef.value;
  if (!wrap) return [];
  const cells = Array.from(wrap.querySelectorAll(".competitor-head-table th[data-col]")) as HTMLElement[];
  return cells.map((cell) => {
    const rect = cell.getBoundingClientRect();
    return {
      left: rect.left,
      right: rect.right,
      center: rect.left + rect.width / 2,
    };
  });
}

function updateInsertPreview(clientX: number): void {
  const wrap = tableWrapRef.value;
  const metrics = getHeaderMetrics();
  if (!wrap || !metrics.length) {
    insertLineLeft.value = null;
    dropIndex.value = null;
    return;
  }

  let index = metrics.length;
  for (let i = 0; i < metrics.length; i += 1) {
    if (clientX < metrics[i].center) {
      index = i;
      break;
    }
  }
  dropIndex.value = index;

  const wrapRect = wrap.getBoundingClientRect();
  const targetX = index === metrics.length ? metrics[metrics.length - 1].right : metrics[index].left;
  insertLineLeft.value = targetX - wrapRect.left + wrap.scrollLeft;
}

function updateGhostPosition(clientX: number, clientY: number): void {
  dragGhost.value.x = clientX + 14;
  dragGhost.value.y = clientY + 14;
}

function cleanupDragHandlers(): void {
  window.removeEventListener("pointermove", onGlobalPointerMove);
  window.removeEventListener("pointerup", onGlobalPointerUp);
  window.removeEventListener("pointercancel", onGlobalPointerUp);
  document.body.classList.remove("col-dragging-mode");
}

function clearDragState(): void {
  dragGhost.value.active = false;
  draggingCol.value = "";
  insertLineLeft.value = null;
  dropIndex.value = null;
}

function onHeaderPointerDown(col: string, event: PointerEvent): void {
  if (event.button !== 0 && event.pointerType !== "touch") return;
  if (longPressTimer) clearTimeout(longPressTimer);

  const target = event.currentTarget as HTMLElement | null;
  const width = target?.getBoundingClientRect().width || 240;
  const startX = event.clientX;
  const startY = event.clientY;

  longPressTimer = setTimeout(() => {
    draggingCol.value = col;
    dragGhost.value.active = true;
    dragGhost.value.width = Math.max(180, Math.min(380, width));
    updateGhostPosition(startX, startY);
    updateInsertPreview(startX);
    document.body.classList.add("col-dragging-mode");
  }, LONG_PRESS_MS);

  window.addEventListener("pointermove", onGlobalPointerMove);
  window.addEventListener("pointerup", onGlobalPointerUp);
  window.addEventListener("pointercancel", onGlobalPointerUp);
}

function onGlobalPointerMove(event: PointerEvent): void {
  if (!dragGhost.value.active) return;
  event.preventDefault();
  updateGhostPosition(event.clientX, event.clientY);
  updateInsertPreview(event.clientX);
}

function onGlobalPointerUp(): void {
  if (longPressTimer) {
    clearTimeout(longPressTimer);
    longPressTimer = null;
  }

  if (dragGhost.value.active && draggingCol.value && dropIndex.value !== null) {
    moveColumnToInsertIndex(draggingCol.value, dropIndex.value);
  }

  clearDragState();
  cleanupDragHandlers();
}

async function loadYearMonths(): Promise<void> {
  yearMonthOptions.value = await fetchPgYearMonthsByTable(TARGET_TABLE);
}

async function loadTable(options: { append?: boolean } = {}): Promise<void> {
  const append = options.append === true;
  if (append) {
    if (!hasMoreRows.value || loading.value || loadingMore.value) return;
  }

  const requestPage = append ? page.value + 1 : 1;
  if (append) loadingMore.value = true;
  else loading.value = true;
  error.value = "";

  try {
    const data = await fetchPgItems({
      table: TARGET_TABLE,
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      page: requestPage,
      pageSize: pageSize.value,
      sortBy: sortBy.value,
      sortDir: sortDir.value,
      textFilters: normalizedTextFilters(),
      valueFilters: normalizedValueFilters(),
    });
    const incomingRows = data.items || [];
    rows.value = append ? [...rows.value, ...incomingRows] : incomingRows;
    total.value = Number(data.total || 0);
    page.value = requestPage;
  } catch (err) {
    console.error("reload competitor table failed:", err);
    error.value = "竞品表加载失败，请稍后重试。";
    if (!append) {
      rows.value = [];
      total.value = 0;
      page.value = 0;
    }
  } finally {
    if (append) loadingMore.value = false;
    else loading.value = false;
    scheduleLazyLoadCheck();
  }
}

async function reloadTable(): Promise<void> {
  page.value = 0;
  rows.value = [];
  total.value = 0;
  if (tableWrapRef.value) {
    tableWrapRef.value.scrollTop = 0;
  }
  await loadTable({ append: false });
}

function isSameValueSet(values: string[], options: PgFilterOption[]): boolean {
  if (values.length !== options.length) return false;
  const set = new Set(values);
  for (const option of options) {
    if (!set.has(option.value)) return false;
  }
  return true;
}

async function openFilterMenu(columnKey: string, event: MouseEvent): Promise<void> {
  const field = filterFieldFor(columnKey);
  if (!field) return;

  const target = event.currentTarget as HTMLElement | null;
  const rect = target?.getBoundingClientRect();
  if (!rect) return;

  filterMenu.value.open = true;
  filterMenu.value.columnKey = columnKey;
  filterMenu.value.field = field;

  const existingTextFilter = textFilters.value[field];
  if (typeof existingTextFilter === "string") {
    filterMenu.value.keyword = existingTextFilter;
    filterTextRule.value = { op: "contains", value: existingTextFilter };
  } else if (existingTextFilter && typeof existingTextFilter === "object") {
    const op = (existingTextFilter.op as TextFilterOp) || "contains";
    const value = String(existingTextFilter.value ?? "");
    filterTextRule.value = { op, value };
    filterMenu.value.keyword = op === "contains" ? value : "";
  } else {
    filterMenu.value.keyword = "";
    filterTextRule.value = { op: "contains", value: "" };
  }

  const menuWidth = 360;
  const menuHeight = 560;
  const viewportPadding = 12;
  const maxX = Math.max(viewportPadding, window.innerWidth - menuWidth - viewportPadding);
  const preferredX = rect.left - 8;
  filterMenu.value.x = Math.min(Math.max(viewportPadding, preferredX), maxX);

  const preferredY = rect.bottom + 8;
  const maxY = window.innerHeight - viewportPadding;
  filterMenu.value.y =
    preferredY + menuHeight <= maxY ? preferredY : Math.max(viewportPadding, rect.top - menuHeight - 8);

  workingFilterValues.value = [];
  await loadFilterOptionsForMenu(filterMenu.value.keyword.trim(), true);
}

async function loadFilterOptionsForMenu(keyword: string, initialize = false): Promise<void> {
  const field = filterMenu.value.field;
  if (!field) return;

  const requestSeq = ++activeFilterRequestSeq;
  filterOptionsLoading.value = true;
  const existing = valueFilters.value[field];
  const shouldAutoSelectNext =
    !existing?.length && isSameValueSet(workingFilterValues.value, filterOptions.value);

  try {
    const options = await fetchPgFilterOptions({
      column: field,
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      keyword: keyword || undefined,
      textFilters: normalizedTextFilters(),
      valueFilters: normalizedValueFilters(),
      limit: keyword ? 20000 : 500,
      table: TARGET_TABLE,
    });

    if (!filterMenu.value.open || filterMenu.value.field !== field) return;
    if (requestSeq !== activeFilterRequestSeq) return;

    filterOptions.value = options;
    if (existing && existing.length > 0) {
      if (initialize && !workingFilterValues.value.length) {
        workingFilterValues.value = [...existing];
      }
      return;
    }
    if (initialize || shouldAutoSelectNext) {
      workingFilterValues.value = options.map((item) => item.value);
    }
  } finally {
    if (requestSeq === activeFilterRequestSeq) {
      filterOptionsLoading.value = false;
    }
  }
}

function closeFilterMenu(): void {
  activeFilterRequestSeq += 1;
  filterOptionsLoading.value = false;
  filterMenu.value.open = false;
  filterMenu.value.columnKey = "";
  filterMenu.value.field = "";
  filterMenu.value.keyword = "";
  filterTextRule.value = { op: "contains", value: "" };
  filterOptions.value = [];
  workingFilterValues.value = [];
}

function selectAllFilterValues(): void {
  workingFilterValues.value = filterOptions.value.map((item) => item.value);
}

function invertFilterSelection(): void {
  const selected = new Set(workingFilterValues.value);
  workingFilterValues.value = filterOptions.value
    .map((option) => option.value)
    .filter((value) => !selected.has(value));
}

function clearFilterSelection(): void {
  workingFilterValues.value = [];
}

function toggleWorkingFilterValue(value: string, event: Event): void {
  const target = event.target as HTMLInputElement;
  if (target.checked) {
    if (!workingFilterValues.value.includes(value)) {
      workingFilterValues.value = [...workingFilterValues.value, value];
    }
  } else {
    workingFilterValues.value = workingFilterValues.value.filter((item) => item !== value);
  }
}

function clearColumnFilter(): void {
  const field = filterMenu.value.field;
  if (!field) return;
  delete textFilters.value[field];
  delete valueFilters.value[field];
  closeFilterMenu();
  page.value = 1;
  void reloadTable();
}

function applyTextRuleFilter(): void {
  const field = filterMenu.value.field;
  if (!field) return;
  const op = filterTextRule.value.op;
  const value = filterTextRule.value.value.trim();

  if (filterRuleNeedsValue.value && !value) {
    alert("请输入条件值");
    return;
  }

  if (op === "contains") {
    textFilters.value[field] = value;
  } else if (op === "is_blank" || op === "is_not_blank") {
    textFilters.value[field] = { op };
  } else {
    textFilters.value[field] = { op, value };
  }
  delete valueFilters.value[field];
  closeFilterMenu();
  page.value = 1;
  void reloadTable();
}

function applyFilterMenu(): void {
  const field = filterMenu.value.field;
  if (!field) return;

  const allValues = filterOptions.value.map((item) => item.value);
  const selected = [...new Set(workingFilterValues.value)];
  const keyword = filterMenu.value.keyword.trim();
  const keywordActive = keyword.length > 0;

  if (!selected.length) {
    delete textFilters.value[field];
    delete valueFilters.value[field];
  } else if (keywordActive && selected.length === allValues.length) {
    textFilters.value[field] = keyword;
    delete valueFilters.value[field];
  } else if (!keywordActive && selected.length === allValues.length) {
    delete textFilters.value[field];
    delete valueFilters.value[field];
  } else {
    delete textFilters.value[field];
    valueFilters.value[field] = selected;
  }

  closeFilterMenu();
  page.value = 1;
  void reloadTable();
}

function handleDocumentPointerDown(event: PointerEvent): void {
  if (!filterMenu.value.open) return;
  const target = event.target as HTMLElement | null;
  if (!target) return;
  if (filterMenuRef.value?.contains(target)) return;
  if (target.closest(".filter-btn")) return;
  closeFilterMenu();
}

function isNearBottom(): boolean {
  const el = tableWrapRef.value;
  if (!el) return false;
  const threshold = 220;
  return el.scrollTop + el.clientHeight >= el.scrollHeight - threshold;
}

async function maybeLoadMoreRows(): Promise<void> {
  if (!hasMoreRows.value) return;
  if (loading.value || loadingMore.value) return;
  if (!isNearBottom()) return;
  await loadTable({ append: true });
}

function scheduleLazyLoadCheck(): void {
  if (scrollCheckRaf) return;
  scrollCheckRaf = window.requestAnimationFrame(() => {
    scrollCheckRaf = 0;
    void maybeLoadMoreRows();
  });
}

function onTableWrapScroll(): void {
  scheduleLazyLoadCheck();
}

watch(selectedYear, () => {
  if (selectedMonth.value !== 0 && !monthOptions.value.includes(selectedMonth.value)) {
    selectedMonth.value = 0;
    return;
  }
  void reloadTable();
});

watch(selectedMonth, () => {
  void reloadTable();
});

watch([sortBy, sortDir, pageSize], () => {
  void reloadTable();
});

watch(
  () => filterMenu.value.keyword,
  (keyword) => {
    if (!filterMenu.value.open || !filterMenu.value.field) return;
    if (filterTimer) clearTimeout(filterTimer);
    filterTimer = setTimeout(() => {
      filterTimer = null;
      void loadFilterOptionsForMenu(keyword.trim());
    }, 220);
  }
);

onMounted(async () => {
  document.addEventListener("pointerdown", handleDocumentPointerDown);
  await loadYearMonths();
  await reloadTable();
});

onBeforeUnmount(() => {
  document.removeEventListener("pointerdown", handleDocumentPointerDown);
  if (longPressTimer) clearTimeout(longPressTimer);
  if (filterTimer) clearTimeout(filterTimer);
  if (scrollCheckRaf) window.cancelAnimationFrame(scrollCheckRaf);
  if (copyToastTimer) clearTimeout(copyToastTimer);
  cleanupDragHandlers();
  closeFilterMenu();
});
</script>

<style scoped>
.competitor-page {
  padding-top: 1rem;
}

.toolbar-filters {
  display: flex;
  gap: 0.75rem;
  align-items: end;
  flex-wrap: wrap;
  margin-left: auto;
  justify-content: flex-end;
}

.competitor-filters {
  margin-bottom: 0.8rem;
}

.toolbar-filters label {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
  font-size: 0.85rem;
  color: #374151;
}

.toolbar-filters select {
  min-width: 100px;
}

.competitor-panel {
  margin-top: 0;
}

.competitor-list-shell {
  position: relative;
  margin-top: 0.8rem;
  padding: 12px;
  background: #fff;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  max-height: 72vh;
  overflow-y: auto;
  overflow-x: hidden;
}

.competitor-head-card {
  position: sticky;
  top: -12px;
  z-index: 6;
  margin-bottom: 10px;
  margin-left: -12px;
  margin-right: -12px;
  padding: 12px 12px 2px;
  background: #fff;
}

.competitor-table {
  width: 100%;
  min-width: 0;
  table-layout: fixed;
  border-collapse: separate;
  border-spacing: 0;
}

.competitor-body-table {
  border-spacing: 0 14px;
}

.competitor-table thead th {
  background: #ffffff;
  color: #42597f;
  font-size: 13px;
  font-weight: 800;
  line-height: 1.28;
  text-align: center;
  vertical-align: middle;
  border-top: 1px solid #e6edf7;
  border-bottom: 1px solid #e6edf7;
  padding: 12px 6px;
}

.competitor-head-table {
  background: #fff;
  border-radius: 10px;
}

.competitor-table thead th:first-child {
  border-left: 1px solid #e6edf7;
  border-top-left-radius: 16px;
  border-bottom-left-radius: 16px;
}

.competitor-table thead th:last-child {
  border-right: 1px solid #e6edf7;
  border-top-right-radius: 16px;
  border-bottom-right-radius: 16px;
  padding-right: 20px;
}

.competitor-table th.serial-col,
.competitor-table td.serial-col {
  width: 56px;
  min-width: 56px;
}

.th-inner {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
}

.th-stack {
  display: inline-flex;
  flex-direction: column;
  align-items: center;
  gap: 0;
}

.sortable-header {
  cursor: grab;
  user-select: none;
}

.sortable-header:active {
  cursor: grabbing;
}

.filtered-col {
  background: #fbfdff !important;
}

.filter-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 18px;
  height: 18px;
  border: none;
  background: transparent;
  color: #9aa8bf;
  font-size: 10px;
  cursor: pointer;
  padding: 0;
  transition: color 0.18s ease;
}

.filter-btn:hover,
.filter-btn.active {
  color: #f97316;
}

.drag-source-col {
  background: #e8f0ff !important;
}

.competitor-main-row td {
  padding: 8px 6px;
  vertical-align: middle;
  background: #ffffff;
  border-top: 1px solid #e6edf7;
}

.competitor-detail-row td {
  padding: 5px 6px 6px;
  background: #ffffff;
  border-bottom: 1px solid #e6edf7;
}

.competitor-detail-row {
  position: relative;
  top: -14px;
}

.competitor-main-row td:first-child,
.competitor-detail-row td:first-child {
  border-left: 1px solid #e6edf7;
}

.competitor-main-row td:last-child,
.competitor-detail-row td:last-child {
  border-right: 1px solid #e6edf7;
}

.competitor-main-row td:first-child {
  border-top-left-radius: 16px;
}

.competitor-main-row td:last-child {
  border-top-right-radius: 16px;
}

.competitor-detail-row td:first-child {
  border-bottom-left-radius: 16px;
}

.competitor-detail-row td:last-child {
  border-bottom-right-radius: 16px;
}

.serial-cell {
  font-size: 14px;
  color: #8b97ab;
  text-align: center;
}

.empty-row td {
  padding: 20px 12px;
  text-align: center;
  color: #64748b;
  background: #fff;
  border: 1px solid #e6edf7;
  border-radius: 16px;
}

.product-cell {
  padding-top: 5px !important;
  padding-bottom: 6px !important;
}

.product-body {
  display: flex;
  align-items: flex-start;
  gap: 8px;
}

.product-image {
  width: 72px;
  height: 72px;
  border-radius: 8px;
  border: 1px solid #e5ebf5;
  object-fit: cover;
  background: #fff;
  flex: 0 0 auto;
}

.product-copy {
  min-width: 0;
}

.product-title {
  appearance: none;
  border: 0;
  padding: 0;
  background: transparent;
  text-align: left;
  width: 100%;
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
  overflow: hidden;
  color: #253858;
  text-decoration: none;
  font-weight: 700;
  font-size: 17px;
  line-height: 1.3;
}

.product-title:hover {
  color: #f97316;
}

.product-title-copy {
  cursor: pointer;
  font: inherit;
}

.product-title-copy:focus-visible {
  outline: 2px solid rgba(58, 104, 216, 0.28);
  outline-offset: 2px;
}

.product-line {
  margin-top: 2px;
  display: flex;
  align-items: baseline;
  gap: 4px;
  color: #667085;
  font-size: 12.5px;
  line-height: 1.25;
}

.product-meta-label {
  color: #8a94a6;
  white-space: nowrap;
}

.product-meta-link {
  color: #3a68d8;
  text-decoration: none;
}

.product-meta-link:hover {
  color: #f97316;
  text-decoration: underline;
}

.metric-cell {
  min-width: 0;
  text-align: center;
}

.metric-main {
  color: #2b3445;
  font-size: 14px;
  font-weight: 500;
  line-height: 1.2;
  white-space: nowrap;
}

.metric-sub {
  margin-top: 1px;
  color: #8a94a6;
  font-size: 14px;
  font-weight: 500;
  line-height: 1.2;
  white-space: nowrap;
}

.metric-sub.accent {
  color: #f97316;
}

.metric-sub.down {
  color: #ef4444;
}

.trend-cell {
  text-align: center;
}

.trend-inner {
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
}

.detail-anchor {
  background: #fffdf9 !important;
}

.detail-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 12px;
  align-items: center;
}

.detail-line {
  display: inline-flex;
  align-items: baseline;
  gap: 4px;
  color: #667085;
  font-size: 13.5px;
  line-height: 1.5;
}

.detail-line.detail-wide {
  width: 100%;
}

.detail-label {
  color: #8a94a6;
  font-size: 13.5px;
  white-space: nowrap;
}

.detail-value {
  color: #344054;
  font-size: 13.5px;
}

.detail-value.detail-path {
  color: #f97316;
}

.column-insert-line {
  position: absolute;
  top: 0;
  bottom: 0;
  width: 2px;
  background: #315fcd;
  z-index: 9;
  pointer-events: none;
}

.column-drag-ghost {
  position: fixed;
  z-index: 1000;
  pointer-events: none;
  border: 1px solid #bcd0f7;
  border-radius: 10px;
  box-shadow: 0 16px 30px rgba(18, 38, 75, 0.2);
  background: rgba(255, 255, 255, 0.95);
  overflow: hidden;
}

.column-drag-ghost-header {
  padding: 8px 10px;
  font-size: 12px;
  font-weight: 700;
  color: #1f3f77;
  background: #eef4ff;
  border-bottom: 1px solid #d7e5ff;
}

.column-drag-ghost-cell {
  padding: 6px 10px;
  font-size: 12px;
  color: #2a3d60;
  border-top: 1px solid #f1f5ff;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

:global(body.col-dragging-mode) {
  cursor: grabbing;
}

.excel-filter-pop {
  position: fixed;
  z-index: 1002;
  width: 360px;
  background: #fff;
  border: 1px solid #c8d6f0;
  border-radius: 12px;
  box-shadow: 0 24px 46px rgba(23, 41, 78, 0.22);
}

.excel-filter-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 10px 12px 8px;
  border-bottom: 1px solid #e6edf9;
}

.excel-filter-title {
  color: #1f3f77;
  font-size: 14px;
  font-weight: 700;
}

.excel-filter-count {
  font-size: 12px;
  color: #35528d;
  background: #edf3ff;
  border: 1px solid #cfddf8;
  border-radius: 999px;
  padding: 2px 8px;
  font-variant-numeric: tabular-nums;
}

.excel-filter-search {
  margin: 10px 10px 0;
  width: calc(100% - 20px);
  border-radius: 8px;
  border: 1px solid #c8d6f0;
  height: 36px;
  padding: 0 10px;
}

.excel-filter-search:focus {
  outline: none;
  border-color: #3f6fda;
  box-shadow: 0 0 0 3px rgba(63, 111, 218, 0.16);
}

.excel-filter-advanced {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 8px 10px 0;
  padding: 8px;
  border-radius: 8px;
  background: #f6f9ff;
  border: 1px solid #e0e9fb;
}

.excel-filter-op {
  min-width: 116px;
  height: 34px;
  border: 1px solid #c8d7f3;
  border-radius: 8px;
  padding: 0 8px;
  background: #fff;
}

.excel-filter-rule-value {
  flex: 1;
  min-width: 100px;
  height: 34px;
  border: 1px solid #c8d7f3;
  border-radius: 8px;
  padding: 0 10px;
}

.excel-filter-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin: 8px 10px 0;
}

.excel-filter-actions .secondary-btn {
  padding: 0.38rem 0.6rem;
  border-radius: 7px;
}

.excel-filter-list {
  margin: 8px 10px 0;
  max-height: 280px;
  overflow: auto;
  border: 1px solid #e3ebfb;
  border-radius: 9px;
  background: #fff;
}

.excel-filter-option {
  display: grid;
  grid-template-columns: auto 1fr auto;
  gap: 8px;
  align-items: center;
  padding: 0.46rem 0.5rem;
  transition: background 0.15s ease;
}

.excel-filter-option:hover {
  background: #f6f9ff;
}

.excel-filter-option input {
  accent-color: #315fcd;
}

.option-label {
  min-width: 0;
  color: #2a3d60;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.option-count {
  color: #8a94a6;
  font-size: 12px;
}

.excel-filter-loading,
.excel-filter-empty {
  height: 140px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  color: #375181;
  font-size: 13px;
}

.excel-filter-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  padding: 10px;
}

.table-loading-mask {
  position: absolute;
  inset: 0;
  z-index: 8;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  background: rgba(240, 246, 255, 0.45);
  backdrop-filter: blur(1px);
}

.table-loading-spinner,
.mini-spinner {
  width: 18px;
  height: 18px;
  border-radius: 50%;
  border: 2px solid rgba(50, 85, 190, 0.28);
  border-top-color: #3255be;
  animation: table-loading-spin 0.8s linear infinite;
}

.mini-spinner {
  width: 12px;
  height: 12px;
}

.table-loading-text {
  font-size: 13px;
  color: #1f2f4a;
  font-weight: 600;
}

.table-error {
  margin: 10px 0;
  padding: 10px 12px;
  border-radius: 10px;
  border: 1px solid #fecaca;
  background: #fff1f2;
  color: #b91c1c;
  font-size: 0.9rem;
}

.competitor-lazy-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 12px 8px 2px;
  color: #5c6b84;
  font-size: 13px;
}

.competitor-footer-controls {
  margin-top: 0.8rem;
  display: flex;
  align-items: center;
  gap: 1rem;
  flex-wrap: wrap;
  font-size: 0.85rem;
  color: #374151;
}

.copy-toast {
  position: fixed;
  left: 50%;
  top: 18%;
  transform: translateX(-50%);
  z-index: 1300;
  padding: 10px 14px;
  border-radius: 10px;
  background: rgba(31, 47, 74, 0.92);
  color: #fff;
  font-size: 13px;
  line-height: 1;
  box-shadow: 0 10px 24px rgba(15, 23, 42, 0.22);
}

.competitor-footer-controls :deep(.rows-control) {
  font-size: 0.85rem;
}

.competitor-footer-controls :deep(.rows-control input) {
  height: 36px;
  padding: 0.35rem 0.5rem;
}

.competitor-footer-controls :deep(.lazy-hint),
.competitor-footer-controls :deep(.lazy-loading-inline) {
  font-size: 13px;
  color: #5c6b84;
}

:deep(.competitor-page .trend-mini-btn) {
  width: 142px;
  min-width: 142px;
  height: 74px;
  padding: 4px 5px;
  gap: 2px;
  margin: 0 auto;
}

:deep(.competitor-page .trend-mini-svg) {
  height: 52px;
}

:deep(.competitor-page .trend-mini-range) {
  font-size: 11px;
}

@media (max-width: 1080px) {
  .toolbar-filters {
    width: 100%;
  }

  .competitor-list-shell {
    overflow-y: auto;
    overflow-x: hidden;
  }
}

@media (max-width: 900px) {
  .excel-filter-pop {
    width: min(92vw, 360px);
  }
}

@keyframes table-loading-spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
