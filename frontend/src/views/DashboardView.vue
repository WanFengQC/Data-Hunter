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

    <section class="table-panel">
      <div class="table-panel-header">
        <h2>PostgreSQL 数据表</h2>
        <div class="table-filters">
          <label>
            年度
            <select v-model.number="selectedYear">
              <option :value="0">全部</option>
              <option v-for="y in yearOptions" :key="y" :value="y">{{ y }}</option>
            </select>
          </label>
          <label>
            月份
            <select v-model.number="selectedMonth">
              <option :value="0">全部</option>
              <option v-for="m in monthOptions" :key="m" :value="m">
                {{ String(m).padStart(2, "0") }}
              </option>
            </select>
          </label>
          <label>
            排序字段
            <select v-model="sortBy">
              <option v-for="col in sortFieldOptions" :key="`sort-${col}`" :value="col">
                {{ columnLabel(col) }}
              </option>
            </select>
          </label>
          <label>
            排序方向
            <select v-model="sortDir">
              <option value="asc">升序</option>
              <option value="desc">降序</option>
            </select>
          </label>
          <button class="secondary-btn" @click="exportFullCsv" :disabled="!tableTotal || exportLoading">
            {{ exportLoading ? "导出中..." : "导出完整 CSV" }}
          </button>
        </div>
      </div>

      <details class="column-manager">
        <summary>列管理（支持记忆）</summary>
        <div class="column-actions">
          <button class="secondary-btn" @click="selectAllColumns">全选</button>
          <button class="secondary-btn" @click="resetColumns">重置</button>
        </div>
        <div class="column-list">
          <div class="column-item" v-for="col in manageableColumns" :key="col">
            <label>
              <input type="checkbox" :checked="isColumnVisible(col)" @change="toggleColumn(col, $event)" />
              <span :title="col">{{ columnLabel(col) }}</span>
            </label>
          </div>
        </div>
      </details>

      <div class="table-meta">
        <span>总数: {{ tableTotal }}</span>
        <span>页码: {{ tablePage }}</span>
        <span v-if="sortBy">排序: {{ columnLabel(sortBy) }} {{ sortDir === "asc" ? "升序" : "降序" }}</span>
        <span>表头长按后可左右拖动换列</span>
      </div>

      <div class="table-wrap" ref="tableWrapRef">
        <table class="data-table">
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
                @pointerdown="onHeaderPointerDown(col, $event)"
              >
                <div class="th-inner">
                  <span :title="col">{{ columnLabel(col) }}</span>
                  <button
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
          <tbody>
            <tr v-if="!tableRows.length">
              <td :colspan="Math.max(displayColumns.length + 1, 1)">暂无数据</td>
            </tr>
            <tr v-for="(row, idx) in tableRows" :key="idx">
              <td class="serial-col">{{ getRowSerial(idx) }}</td>
              <td
                v-for="col in displayColumns"
                :key="`${idx}-${col}`"
                :title="cellToText(getCell(row, col))"
                :class="{ 'drag-source-col': dragGhost.active && draggingCol === col }"
              >
                {{ formatCell(getCell(row, col)) }}
              </td>
            </tr>
          </tbody>
        </table>

        <div
          v-if="dragGhost.active && insertLineLeft !== null"
          class="column-insert-line"
          :style="{ left: `${insertLineLeft}px` }"
        />
      </div>

      <div
        v-if="dragGhost.active"
        class="column-drag-ghost"
        :style="{ left: `${dragGhost.x}px`, top: `${dragGhost.y}px`, width: `${dragGhost.width}px` }"
      >
        <div class="column-drag-ghost-header">{{ columnLabel(draggingCol) }}</div>
        <div v-for="(item, idx) in dragPreviewValues" :key="idx" class="column-drag-ghost-cell">
          {{ item }}
        </div>
      </div>

      <div
        v-if="filterMenu.open"
        ref="filterMenuRef"
        class="excel-filter-pop"
        :style="{ left: `${filterMenu.x}px`, top: `${filterMenu.y}px` }"
      >
        <div class="excel-filter-title">{{ columnLabel(filterMenu.column) }}</div>
        <input
          v-model="filterMenu.keyword"
          class="excel-filter-search"
          placeholder="搜索筛选值"
          @keydown.stop
        />
        <div class="excel-filter-actions">
          <button class="secondary-btn" @click="selectAllFilterValues">全选</button>
          <button class="secondary-btn" @click="clearFilterSelection">清空选择</button>
          <button class="secondary-btn" @click="clearColumnFilter">清空筛选</button>
        </div>
        <div class="excel-filter-list">
          <label v-for="option in visibleFilterOptions" :key="option.value" class="excel-filter-option">
            <input
              type="checkbox"
              :checked="workingFilterSet.has(option.value)"
              @change="toggleWorkingFilterValue(option.value, $event)"
            />
            <span class="option-label">{{ option.label }}</span>
            <span class="option-count">{{ option.count }}</span>
          </label>
          <div v-if="!visibleFilterOptions.length" class="excel-filter-empty">无可选值</div>
        </div>
        <div class="excel-filter-footer">
          <button class="secondary-btn" @click="closeFilterMenu">取消</button>
          <button class="primary-btn small" :disabled="!workingFilterValues.length" @click="applyFilterMenu">
            应用
          </button>
        </div>
      </div>

      <div class="pagination">
        <label class="rows-control">
          显示行数
          <input v-model.number="tablePageSize" type="number" min="1" step="1" />
        </label>
        <button class="secondary-btn" @click="prevPage" :disabled="tablePage <= 1 || tableLoading">
          上一页
        </button>
        <button
          class="secondary-btn"
          @click="nextPage"
          :disabled="tablePage * tablePageSize >= tableTotal || tableLoading"
        >
          下一页
        </button>
      </div>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";

import {
  exportPgItemsCsv,
  fetchPgFilterOptions,
  fetchPgItems,
  fetchPgYearMonths,
  fetchProcessedSummary,
  triggerCrawlPipeline,
} from "@/api/data";
import StatCard from "@/components/StatCard.vue";
import TrendChart from "@/components/TrendChart.vue";
import type { PgFilterOption, ProcessedSummary } from "@/types/data";

const COLUMN_PREFS_KEY = "data_hunter.pg.column_prefs.v1";
const LONG_PRESS_MS = 220;

const EXCLUDED_COLUMNS = new Set([
  "id",
  "source_folder",
  "source_file",
  "page_no",
  "year_month",
  "imported_at",
  "station",
  "keywordjp",
]);

const FIELD_LABELS: Record<string, string> = {
  id: "编号",
  keyword: "关键词",
  searches: "搜索量",
  clicks: "点击量",
  impressions: "曝光量",
  purchaserate: "购买率",
  purchases: "购买量",
  keywordcn: "中文关键词",
  adproducts1: "广告商品数(1天)",
  adproducts7: "广告商品数(7天)",
  adproducts30: "广告商品数(30天)",
  products: "商品数量",
  departments: "类目",
  searchrank: "搜索排名",
  searchrankgrowthvalue: "搜索排名变化值",
  searchrankgrowthrate: "搜索排名变化率",
  cvssharerate: "转化份额",
  clicksharerate: "点击份额",
  gkdatas: "关键词商品明细",
  titledensityexact: "标题密度(精确)",
  cprexact: "CPR(精确)",
  w1searchrank: "1周搜索排名",
  w1rankgrowthvalue: "1周排名变化值",
  w1rankgrowthrate: "1周排名变化率",
  w4searchrank: "4周搜索排名",
  w4rankgrowthvalue: "4周排名变化值",
  w4rankgrowthrate: "4周排名变化率",
  w12searchrank: "12周搜索排名",
  w12rankgrowthvalue: "12周排名变化值",
  w12rankgrowthrate: "12周排名变化率",
  top3brands: "前3品牌",
  bid: "建议竞价",
  bidmax: "最高竞价",
  bidmin: "最低竞价",
  minphraseppc: "词组最小PPC",
  maxphraseppc: "词组最大PPC",
  phraseppc: "词组PPC",
  minbroadppc: "广泛最小PPC",
  maxbroadppc: "广泛最大PPC",
  broadppc: "广泛PPC",
  minexactppc: "精确最小PPC",
  maxexactppc: "精确最大PPC",
  exactppc: "精确PPC",
  top3asindtolist: "前3ASIN明细",
  trends: "趋势数据",
};

const loading = ref(false);
const exportLoading = ref(false);
const summary = ref<ProcessedSummary>({ generated_at: null, categories: [] });

const tableLoading = ref(false);
const tableColumns = ref<string[]>([]);
const tableRows = ref<Record<string, unknown>[]>([]);
const tableTotal = ref(0);
const tablePage = ref(1);
const tablePageSize = ref(20);
const autoReloadReady = ref(false);

const yearMonths = ref<number[]>([]);
const selectedYear = ref(0);
const selectedMonth = ref(0);

const sortBy = ref<string>("id");
const sortDir = ref<"asc" | "desc">("asc");
const valueFilters = ref<Record<string, string[]>>({});
const visibleColumns = ref<string[]>([]);

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
  column: "",
  x: 0,
  y: 0,
  keyword: "",
});
const filterOptions = ref<PgFilterOption[]>([]);
const workingFilterValues = ref<string[]>([]);

let longPressTimer: ReturnType<typeof setTimeout> | null = null;
let filterTimer: ReturnType<typeof setTimeout> | null = null;

const totalCount = computed(() => summary.value.categories.reduce((sum, row) => sum + row.count, 0));

const generatedAt = computed(() => {
  if (!summary.value.generated_at) return "暂无";
  return new Date(summary.value.generated_at).toLocaleString();
});

const yearOptions = computed(() => {
  const years = new Set<number>();
  for (const ym of yearMonths.value) years.add(Math.floor(ym / 100));
  return Array.from(years).sort((a, b) => b - a);
});

const monthOptions = computed(() => {
  const months = new Set<number>();
  for (const ym of yearMonths.value) {
    const year = Math.floor(ym / 100);
    const month = ym % 100;
    if (selectedYear.value === 0 || selectedYear.value === year) months.add(month);
  }
  return Array.from(months).sort((a, b) => a - b);
});

const availableColumns = computed(() => tableColumns.value.filter((c) => !EXCLUDED_COLUMNS.has(c)));

const sortFieldOptions = computed(() => {
  const cols = [...availableColumns.value];
  if (!cols.includes("id")) cols.unshift("id");
  return cols;
});

const displayColumns = computed(() => {
  if (!visibleColumns.value.length) return availableColumns.value;
  const availableSet = new Set(availableColumns.value);
  return visibleColumns.value.filter((c) => availableSet.has(c));
});

const manageableColumns = computed(() => {
  const available = availableColumns.value;
  const selectedSet = new Set(visibleColumns.value);
  const selected = visibleColumns.value.filter((c) => selectedSet.has(c) && available.includes(c));
  const hidden = available.filter((c) => !selectedSet.has(c));
  return [...selected, ...hidden];
});

const dragPreviewValues = computed(() => {
  if (!draggingCol.value) return [];
  return tableRows.value.slice(0, 8).map((row) => formatCell(getCell(row, draggingCol.value)));
});

const workingFilterSet = computed(() => new Set(workingFilterValues.value));

const visibleFilterOptions = computed(() => {
  const keyword = filterMenu.value.keyword.trim().toLowerCase();
  if (!keyword) return filterOptions.value;
  return filterOptions.value.filter((item) => item.label.toLowerCase().includes(keyword));
});

function columnLabel(col: string): string {
  return FIELD_LABELS[col] || col;
}

function loadColumnPrefs(): void {
  try {
    const raw = localStorage.getItem(COLUMN_PREFS_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return;
    visibleColumns.value = parsed.filter((v): v is string => typeof v === "string");
  } catch {
    // ignore broken cache
  }
}

function saveColumnPrefs(): void {
  localStorage.setItem(COLUMN_PREFS_KEY, JSON.stringify(visibleColumns.value));
}

function syncColumnsState(): void {
  const available = availableColumns.value;
  if (!visibleColumns.value.length) {
    visibleColumns.value = [...available];
    return;
  }
  const availableSet = new Set(available);
  const next = visibleColumns.value.filter((c) => availableSet.has(c));
  visibleColumns.value = next.length ? next : [...available];
}

function isColumnValueFiltered(col: string): boolean {
  return Array.isArray(valueFilters.value[col]) && valueFilters.value[col].length > 0;
}

function moveColumnToInsertIndex(col: string, insertIndex: number): void {
  const current = [...displayColumns.value];
  const from = current.indexOf(col);
  if (from < 0) return;
  current.splice(from, 1);

  let target = Math.max(0, Math.min(insertIndex, current.length));
  if (target > from) target -= 1;
  current.splice(target, 0, col);

  visibleColumns.value = [...current];
  saveColumnPrefs();
}

function getHeaderMetrics(): Array<{ left: number; right: number; center: number }> {
  const wrap = tableWrapRef.value;
  if (!wrap) return [];
  const cells = Array.from(wrap.querySelectorAll("th[data-col]")) as HTMLElement[];
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

async function initYearMonthFilters(): Promise<void> {
  yearMonths.value = await fetchPgYearMonths();
  if (!yearMonths.value.length) return;
  const latest = yearMonths.value[0];
  selectedYear.value = Math.floor(latest / 100);
  selectedMonth.value = latest % 100;
}

function getCell(row: Record<string, unknown>, key: string): unknown {
  return row[key];
}

function cellToText(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatCell(value: unknown): string {
  const text = cellToText(value);
  return text.length > 80 ? `${text.slice(0, 80)}...` : text;
}

function getRowSerial(idx: number): number {
  return (tablePage.value - 1) * tablePageSize.value + idx + 1;
}

function normalizedValueFilters(): Record<string, string[]> {
  const output: Record<string, string[]> = {};
  for (const [key, values] of Object.entries(valueFilters.value)) {
    if (Array.isArray(values) && values.length > 0) output[key] = values;
  }
  return output;
}

async function loadTable(): Promise<void> {
  tableLoading.value = true;
  try {
    const data = await fetchPgItems({
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      page: tablePage.value,
      pageSize: tablePageSize.value,
      sortBy: sortBy.value || undefined,
      sortDir: sortDir.value,
      valueFilters: normalizedValueFilters(),
    });
    tableColumns.value = data.columns;
    tableRows.value = data.items;
    tableTotal.value = data.total;
    syncColumnsState();
  } finally {
    tableLoading.value = false;
  }
}

async function reloadTable(): Promise<void> {
  tablePage.value = 1;
  await loadTable();
}

function triggerRealtimeReload(): void {
  if (!autoReloadReady.value) return;
  void reloadTable();
}

function isColumnVisible(col: string): boolean {
  return visibleColumns.value.includes(col);
}

function toggleColumn(col: string, event: Event): void {
  const target = event.target as HTMLInputElement;
  if (target.checked) {
    if (!visibleColumns.value.includes(col)) visibleColumns.value = [...visibleColumns.value, col];
  } else {
    if (visibleColumns.value.length <= 1) return;
    visibleColumns.value = visibleColumns.value.filter((c) => c !== col);
  }
  saveColumnPrefs();
}

function selectAllColumns(): void {
  visibleColumns.value = [...availableColumns.value];
  saveColumnPrefs();
}

function resetColumns(): void {
  visibleColumns.value = [...availableColumns.value];
  sortBy.value = "id";
  sortDir.value = "asc";
  valueFilters.value = {};
  saveColumnPrefs();
  void reloadTable();
}

async function openFilterMenu(col: string, event: MouseEvent): Promise<void> {
  const target = event.currentTarget as HTMLElement | null;
  const rect = target?.getBoundingClientRect();
  if (!rect) return;

  filterMenu.value.open = true;
  filterMenu.value.column = col;
  filterMenu.value.keyword = "";
  filterMenu.value.x = Math.max(12, rect.left - 8);
  filterMenu.value.y = rect.bottom + 8;

  const options = await fetchPgFilterOptions({
    column: col,
    year: selectedYear.value || undefined,
    month: selectedMonth.value || undefined,
    valueFilters: normalizedValueFilters(),
    limit: 500,
  });
  filterOptions.value = options;

  const exists = valueFilters.value[col];
  if (exists && exists.length > 0) {
    workingFilterValues.value = [...exists];
  } else {
    workingFilterValues.value = options.map((x) => x.value);
  }
}

function closeFilterMenu(): void {
  filterMenu.value.open = false;
  filterMenu.value.column = "";
  filterMenu.value.keyword = "";
  filterOptions.value = [];
  workingFilterValues.value = [];
}

function selectAllFilterValues(): void {
  workingFilterValues.value = filterOptions.value.map((x) => x.value);
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
    workingFilterValues.value = workingFilterValues.value.filter((x) => x !== value);
  }
}

function clearColumnFilter(): void {
  if (!filterMenu.value.column) return;
  delete valueFilters.value[filterMenu.value.column];
  closeFilterMenu();
  void reloadTable();
}

function applyFilterMenu(): void {
  const col = filterMenu.value.column;
  if (!col) return;
  const allValues = filterOptions.value.map((x) => x.value);
  const selected = [...new Set(workingFilterValues.value)];

  if (!selected.length || selected.length === allValues.length) {
    delete valueFilters.value[col];
  } else {
    valueFilters.value[col] = selected;
  }
  closeFilterMenu();
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

async function exportFullCsv(): Promise<void> {
  if (!tableTotal.value) return;
  exportLoading.value = true;
  try {
    const blob = await exportPgItemsCsv({
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      sortBy: sortBy.value || undefined,
      sortDir: sortDir.value,
      valueFilters: normalizedValueFilters(),
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pg_items_full_${selectedYear.value || "all"}_${selectedMonth.value || "all"}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } finally {
    exportLoading.value = false;
  }
}

async function prevPage(): Promise<void> {
  if (tablePage.value <= 1) return;
  tablePage.value -= 1;
  await loadTable();
}

async function nextPage(): Promise<void> {
  if (tablePage.value * tablePageSize.value >= tableTotal.value) return;
  tablePage.value += 1;
  await loadTable();
}

watch(selectedYear, () => {
  if (selectedMonth.value !== 0 && !monthOptions.value.includes(selectedMonth.value)) {
    selectedMonth.value = 0;
    return;
  }
  triggerRealtimeReload();
});

watch([selectedMonth, sortBy, sortDir], () => {
  triggerRealtimeReload();
});

watch(tablePageSize, (next) => {
  const normalized = Math.max(1, Math.trunc(Number(next) || 1));
  if (normalized !== next) {
    tablePageSize.value = normalized;
    return;
  }
  triggerRealtimeReload();
});

watch(sortFieldOptions, (options) => {
  if (!options.length) return;
  if (!options.includes(sortBy.value)) {
    sortBy.value = options.includes("id") ? "id" : options[0];
  }
});

onMounted(async () => {
  loadColumnPrefs();
  document.addEventListener("pointerdown", handleDocumentPointerDown);
  await loadSummary();
  await initYearMonthFilters();
  await loadTable();
  autoReloadReady.value = true;
});

onBeforeUnmount(() => {
  if (longPressTimer) clearTimeout(longPressTimer);
  if (filterTimer) clearTimeout(filterTimer);
  clearDragState();
  cleanupDragHandlers();
  document.removeEventListener("pointerdown", handleDocumentPointerDown);
});
</script>
