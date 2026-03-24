<template>
  <main class="dashboard">
    <section class="dashboard-header">
      <h1>Data Hunter Dashboard</h1>
      <p>采集、处理和分析结果总览</p>
    </section>

    <section class="word-trend-panel">
      <div class="word-trend-header">
        <h2>搜索量趋势搜索</h2>
        <div class="word-trend-controls">
          <input
            v-model.trim="wordSearchInput"
            class="word-trend-input"
            type="text"
            placeholder="输入单词，如 car"
            @keydown.enter.prevent="searchWordTrend"
          />
          <button
            class="primary-btn small trend-search-btn"
            :disabled="wordTrendLoading || !wordSearchInput"
            @click="searchWordTrend"
          >
            {{ wordTrendLoading ? "查询中..." : "查询" }}
          </button>
          <input
            v-model.trim="compareWordInput"
            class="word-trend-input compare-word-input"
            type="text"
            placeholder="对比词（支持多个，逗号/空格分隔）"
            @keydown.enter.prevent="searchCompareTrend"
          />
          <button
            class="secondary-btn trend-compare-btn"
            :disabled="wordTrendLoading || compareTrendLoading || !wordTrendResult || !compareWordInput"
            @click="searchCompareTrend"
          >
            {{ compareTrendLoading ? "添加中..." : "添加对比" }}
          </button>
          <button v-if="compareTrendResults.length" class="secondary-btn trend-clear-btn" @click="clearCompareTrend">
            清空对比
          </button>
        </div>
      </div>

      <div v-if="wordTrendResult" class="word-trend-meta">
        <span>单词: {{ wordTrendResult.word }}</span>
        <span>中文: {{ wordTrendResult.info.translation_zh || "-" }}</span>
        <span>标签: {{ wordTrendResult.info.tag_label || wordTrendResult.info.object_category || "-" }}</span>
        <span>原因: {{ wordTrendResult.info.reason || "-" }}</span>
      </div>
      <div v-if="compareTrendResults.length" class="compare-word-meta-inline">
        <span v-for="item in compareTrendResults" :key="`cmp-meta-${item.word}`" class="compare-word-meta-pill">
          单词: {{ item.word || "-" }} | 中文: {{ item.info?.translation_zh || "-" }} | 标签:
          {{ item.info?.tag_label || item.info?.object_category || "-" }} | 原因: {{ item.info?.reason || "-" }}
        </span>
      </div>
      <div v-if="compareTrendResults.length" class="compare-summary-row compare-summary-row-bottom">
        <div class="word-trend-meta compare-meta">
          <span>已添加对比词: {{ compareTrendResults.length }} 个</span>
        </div>
        <div class="compare-word-chips compare-word-chips-bottom">
          <button
            v-for="item in compareTrendResults"
            :key="`cmp-${item.word}`"
            class="compare-word-chip"
            @click="removeCompareTrend(item.word)"
            title="点击移除该对比词"
          >
            {{ item.word }} ×
          </button>
        </div>
      </div>
      <div v-if="wordTrendError" class="word-trend-error">{{ wordTrendError }}</div>
      <div v-if="compareTrendError" class="word-trend-error compare-error">{{ compareTrendError }}</div>

      <div v-if="hasKeywordSearchTriggered" class="word-trend-chart-wrap">
        <div v-if="wordTrendLoading" class="word-trend-loading">加载中...</div>
        <WordFrequencyTrendChart
          v-else-if="wordTrendResult && wordTrendResult.points.length"
          :points="wordTrendResult.points"
          :primary-word="wordTrendResult.word"
          :compare-series="compareSeriesForChart"
        />
        <div v-else class="word-trend-empty">
          {{ wordTrendResult ? "未找到该词的词频记录" : "请输入单词进行搜索" }}
        </div>
      </div>
    </section>

    <section v-if="hasKeywordSearchTriggered" class="keyword-top10-panel">
      <div class="keyword-top10-header">
        <h2>关键词 Top10</h2>
        <div v-if="top10WordTabs.length" class="keyword-top10-tabs">
          <button
            v-for="tab in top10WordTabs"
            :key="`top10-tab-${tab.word}`"
            class="keyword-top10-tab"
            :class="{ active: activeTop10Word === tab.word }"
            @click="activeTop10Word = tab.word"
          >
            {{ tab.label }}
          </button>
        </div>
      </div>

      <div v-if="!wordTrendResult" class="keyword-top10-empty">查询关键词后显示关键词数据</div>
      <div v-else-if="activeTop10Data?.loading" class="keyword-top10-loading">Top10 加载中...</div>
      <div v-else-if="activeTop10Data?.error" class="keyword-top10-error">{{ activeTop10Data.error }}</div>
      <div
        v-else-if="activeTop10Data && activeTop10Data.items.length"
        class="table-wrap keyword-top10-wrap"
        ref="top10WrapRef"
        @scroll.passive="onTop10WrapScroll"
      >
        <table class="data-table keyword-top10-table">
          <thead>
            <tr>
              <th class="serial-col">序号</th>
              <th v-for="col in activeTop10Columns" :key="`top10-head-${col}`">
                <span :title="col">{{ columnLabel(col) }}</span>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, idx) in activeTop10Data.items" :key="`top10-row-${activeTop10Word}-${idx}-${cellToText(getCell(row, 'id'))}`">
              <td class="serial-col">{{ idx + 1 }}</td>
              <td
                v-for="col in activeTop10Columns"
                :key="`top10-${idx}-${col}`"
                :title="
                  col === 'top3asindtolist' || col === 'top3brands' || col === 'trends'
                    ? ''
                    : cellToText(getCell(row, col))
                "
                :class="{
                  'top3-cell': col === 'top3asindtolist' || col === 'top3brands',
                  'trend-cell': col === 'trends',
                }"
              >
                <template v-if="col === 'top3asindtolist'">
                  <div class="top3-asin-cell">
                    <template v-if="parseTop3Asins(getCell(row, col)).length">
                      <div
                        v-for="(asinItem, asinIdx) in parseTop3Asins(getCell(row, col))"
                        :key="`top10-${idx}-${col}-asin-${asinIdx}`"
                        class="top3-asin-item"
                      >
                        <img
                          v-if="asinItem.imageUrl"
                          class="top3-asin-image"
                          :src="asinItem.imageUrl"
                          :alt="asinItem.asin || `asin-${asinIdx + 1}`"
                          loading="lazy"
                        />
                        <div v-else class="top3-asin-image empty">-</div>
                        <a
                          v-if="asinItem.asin"
                          class="top3-asin-link"
                          :href="buildAsinUrl(asinItem.asin)"
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          {{ asinItem.asin }}
                        </a>
                        <span v-else class="top3-asin-link empty">-</span>
                        <div class="top3-asin-metric">
                          <span>点击</span>
                          <strong>{{ formatPercent(asinItem.clickRate) }}</strong>
                        </div>
                        <div class="top3-asin-metric">
                          <span>转化</span>
                          <strong>{{ formatPercent(asinItem.conversionRate) }}</strong>
                        </div>
                      </div>
                    </template>
                    <span v-else>-</span>
                  </div>
                </template>
                <template v-else-if="col === 'top3brands'">
                  <div class="top3-brand-cell">
                    <template v-if="parseTop3Brands(getCell(row, col)).length">
                      <div
                        v-for="(brand, brandIdx) in parseTop3Brands(getCell(row, col))"
                        :key="`top10-${idx}-${col}-brand-${brandIdx}`"
                        class="top3-brand-item"
                      >
                        {{ brand }}
                      </div>
                    </template>
                    <span v-else>-</span>
                  </div>
                </template>
                <template v-else-if="col === 'trends'">
                  <TableTrendMiniChart :raw-trend="getCell(row, col)" @open="openTrendModalFromRow(row, $event)" />
                </template>
                <template v-else>
                  {{ formatCell(getCell(row, col), col) }}
                </template>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="activeTop10Data.loadingMore" class="keyword-top10-loading more">加载更多中...</div>
      </div>
      <div v-else class="keyword-top10-empty">该词未找到关键词数据</div>
    </section>

    <section class="keyword-top10-panel growth-top10-panel">
      <div class="keyword-top10-header growth-top10-header">
        <div class="keyword-top10-tabs">
          <button
            class="keyword-top10-tab"
            :class="{ active: growthTop10Mode === 'monthly' }"
            @click="switchGrowthTop10Mode('monthly')"
          >
            月度增长率TOP10
          </button>
          <button
            class="keyword-top10-tab"
            :class="{ active: growthTop10Mode === 'quarterly' }"
            @click="switchGrowthTop10Mode('quarterly')"
          >
            季度增长率TOP10
          </button>
          <button
            class="keyword-top10-tab"
            :class="{ active: growthTop10Mode === 'searches' }"
            @click="switchGrowthTop10Mode('searches')"
          >
            总搜索量TOP10
          </button>
        </div>
        <div class="table-filters growth-top10-filters">
          <label>
            年份
            <select v-model.number="growthTop10SelectedYear">
              <option :value="0">全部</option>
              <option v-for="y in growthTop10YearOptions" :key="`growth-year-${y}`" :value="y">{{ y }}</option>
            </select>
          </label>
          <label>
            月份
            <select v-model.number="growthTop10SelectedMonth">
              <option :value="0">全部</option>
              <option v-for="m in growthTop10MonthOptions" :key="`growth-month-${m}`" :value="m">
                {{ String(m).padStart(2, "0") }}
              </option>
            </select>
          </label>
          <label>
            搜索量最小值
            <input
              v-model="growthTop10SearchMin"
              type="number"
              min="0"
              step="1"
              placeholder="min"
            />
          </label>
          <label>
            搜索量最大值
            <input
              v-model="growthTop10SearchMax"
              type="number"
              min="0"
              step="1"
              placeholder="max"
            />
          </label>
        </div>
      </div>

      <div v-if="growthTop10Loading" class="keyword-top10-loading">增长率TOP10 加载中...</div>
      <div v-else-if="growthTop10Error" class="keyword-top10-error">{{ growthTop10Error }}</div>
      <div v-else-if="growthTop10Rows.length" class="table-wrap growth-top10-table-wrap">
        <table class="data-table growth-top10-table">
          <thead>
            <tr>
              <th class="serial-col">序号</th>
              <th v-for="col in growthTop10Columns" :key="`growth-top10-head-${col}`">
                <span :title="col">{{ columnLabel(col) }}</span>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="(row, idx) in growthTop10Rows"
              :key="`growth-top10-row-${growthTop10Mode}-${idx}-${cellToText(getCell(row, 'id'))}`"
            >
              <td class="serial-col">{{ idx + 1 }}</td>
              <td
                v-for="col in growthTop10Columns"
                :key="`growth-top10-${idx}-${col}`"
                :title="cellToText(getCell(row, col))"
              >
                {{ formatCell(getCell(row, col), col) }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div v-else class="keyword-top10-empty">暂无增长率Top10数据</div>
    </section>

    <section class="table-panel" :class="{ 'panel-fullscreen': panelFullscreen }" ref="tablePanelRef">
      <div class="table-panel-header">
        <div class="keyword-top10-tabs table-type-switch">
          <button
            type="button"
            class="keyword-top10-tab"
            :class="{ active: currentTableMode === 'aba' }"
            @click="switchTable('aba')"
          >
            ABA表
          </button>
          <button
            type="button"
            class="keyword-top10-tab"
            :class="{ active: currentTableMode === 'word' }"
            @click="switchTable('word')"
          >
            词频表
          </button>
        </div>
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
          <button class="secondary-btn" @click="toggleTableFullscreen">
            {{ panelFullscreen ? "退出全屏" : "全屏" }}
          </button>
        </div>
      </div>

      <div class="table-meta">
        <span>总数: {{ tableTotal }}</span>
        <span>已加载页: {{ tablePage }}</span>
        <span v-if="sortBy">排序: {{ columnLabel(sortBy) }} {{ sortDir === "asc" ? "升序" : "降序" }}</span>
        <span>可按列头筛选</span>
      </div>

      <div
        class="table-wrap"
        :class="{ 'is-loading': tableLoading }"
        ref="tableWrapRef"
        @scroll.passive="onTableWrapScroll"
      >
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
                :title="
                  col === 'top3asindtolist' || col === 'top3brands' || col === 'trends'
                    ? ''
                    : cellToText(getCell(row, col))
                "
                :class="{
                  'drag-source-col': dragGhost.active && draggingCol === col,
                  'top3-cell': col === 'top3asindtolist' || col === 'top3brands',
                  'trend-cell': col === 'trends',
                }"
              >
                <template v-if="col === 'top3asindtolist'">
                  <div class="top3-asin-cell">
                    <template v-if="parseTop3Asins(getCell(row, col)).length">
                      <div
                        v-for="(asinItem, asinIdx) in parseTop3Asins(getCell(row, col))"
                        :key="`${idx}-${col}-asin-${asinIdx}`"
                        class="top3-asin-item"
                      >
                      <img
                        v-if="asinItem.imageUrl"
                        class="top3-asin-image"
                        :src="asinItem.imageUrl"
                        :alt="asinItem.asin || `asin-${asinIdx + 1}`"
                        loading="lazy"
                        />
                        <div v-else class="top3-asin-image empty">-</div>
                        <a
                          v-if="asinItem.asin"
                          class="top3-asin-link"
                          :href="buildAsinUrl(asinItem.asin)"
                          target="_blank"
                          rel="noopener noreferrer"
                          @click.stop
                        >
                          {{ asinItem.asin }}
                        </a>
                        <span v-else class="top3-asin-link empty">-</span>
                        <div class="top3-asin-metric">
                          <span>点击</span>
                          <strong>{{ formatPercent(asinItem.clickRate) }}</strong>
                        </div>
                        <div class="top3-asin-metric">
                          <span>转化</span>
                          <strong>{{ formatPercent(asinItem.conversionRate) }}</strong>
                        </div>
                      </div>
                    </template>
                    <span v-else>-</span>
                  </div>
                </template>
                <template v-else-if="col === 'top3brands'">
                  <div class="top3-brand-cell">
                    <template v-if="parseTop3Brands(getCell(row, col)).length">
                      <div
                        v-for="(brand, brandIdx) in parseTop3Brands(getCell(row, col))"
                        :key="`${idx}-${col}-brand-${brandIdx}`"
                        class="top3-brand-item"
                      >
                        {{ brand }}
                      </div>
                    </template>
                    <span v-else>-</span>
                  </div>
                </template>
                <template v-else-if="col === 'trends'">
                  <TableTrendMiniChart :raw-trend="getCell(row, col)" @open="openTrendModalFromRow(row, $event)" />
                </template>
                <template v-else>
                  {{ formatCell(getCell(row, col), col) }}
                </template>
              </td>
            </tr>
          </tbody>
        </table>

        <div
          v-if="dragGhost.active && insertLineLeft !== null"
          class="column-insert-line"
          :style="{ left: `${insertLineLeft}px` }"
        />

        <div v-if="tableLoading" class="table-loading-mask" aria-live="polite" aria-busy="true">
          <span class="table-loading-spinner" />
          <span class="table-loading-text">加载中...</span>
        </div>
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
        <div class="excel-filter-title-row">
          <div class="excel-filter-title">{{ columnLabel(filterMenu.column) }}</div>
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
          <button class="primary-btn small" :disabled="!workingFilterValues.length" @click="applyFilterMenu">
            应用
          </button>
        </div>
      </div>

      <div class="pagination">
        <label class="rows-control">
          显示行数
          <input v-model.number.lazy="tablePageSize" type="number" min="1" step="1" />
        </label>
        <span class="lazy-hint">已加载 {{ tableRows.length }} / {{ tableTotal }}</span>
        <span v-if="tableLoadingMore" class="lazy-loading-inline">
          <span class="mini-spinner" />
          加载更多...
        </span>
        <span v-else-if="hasMoreRows" class="lazy-hint">向下滚动到底自动加载</span>
        <span v-else-if="tableRows.length" class="lazy-hint">已加载全部</span>
      </div>
    </section>

    <TrendDataModal
      :open="trendModalOpen"
      :title="trendModalTitle"
      :points="trendModalPoints"
      @close="trendModalOpen = false"
    />
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";

import {
  createPgExportJob,
  downloadPgExportJob,
  fetchPgExportJob,
  fetchPgFilterOptions,
  fetchPgGrowthTop10,
  fetchPgItems,
  fetchPgYearMonthsByTable,
  fetchWordFrequencyTrend,
} from "@/api/data";
import TableTrendMiniChart from "@/components/TableTrendMiniChart.vue";
import TrendDataModal from "@/components/TrendDataModal.vue";
import WordFrequencyTrendChart from "@/components/WordFrequencyTrendChart.vue";
import type { PgFilterOption, PgTrendPoint, WordFrequencyTrendResponse } from "@/types/data";

const LONG_PRESS_MS = 220;
type DataTableMode = "aba" | "word";
const DATA_TABLES: Record<DataTableMode, { title: string; tableName: string; keywordCol: string; defaultSortField: string }> =
  {
    aba: { title: "ABA表", tableName: "seller_sprite_items", keywordCol: "keyword", defaultSortField: "searches" },
    word: {
      title: "词频表",
      tableName: "seller_sprite_word_frequency",
      keywordCol: "word",
      defaultSortField: "total_searches",
    },
  };
const ABA_FIXED_COLUMNS = [
  "keyword",
  "keywordcn",
  "searchrank",
  "searches",
  "w4rankgrowthrate",
  "top3brands",
  "top3asindtolist",
  "trends",
];
const TOP10_VISIBLE_COLUMNS = [...ABA_FIXED_COLUMNS];
const TOP10_PAGE_SIZE = 20;
const GROWTH_TOP10_VISIBLE_COLUMNS = [
  "word",
  "word_zh",
  "freq",
  "total_searches",
  "total_searches_growth_rate",
  "total_searches_quarter_avg_growth_rate",
];
type GrowthTop10Mode = "monthly" | "quarterly" | "searches";
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

const BASE_EXCLUDED_COLUMNS = new Set([
  "id",
  "source_folder",
  "source_file",
  "page_no",
  "year_month",
  "imported_at",
  "station",
  "keywordjp",
]);
const WORD_TABLE_FORCE_HIDDEN_COLUMNS = new Set([
  "year",
  "month",
  "updated_at",
  "freq_ratio",
  "coverage",
  "coverage_percent",
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
  w4rankgrowthrate: "4月排名变化率",
  w12searchrank: "12周搜索排名",
  w12rankgrowthvalue: "12周排名变化值",
  w12rankgrowthrate: "12周排名变化率",
  top3brands: "点击前三品牌",
  bid: "建议竞价",
  bidmax: "最高竞价",
  bidmin: "最低竞价",
  minphraseppc: "词组最低PPC",
  maxphraseppc: "词组最高PPC",
  phraseppc: "词组PPC",
  minbroadppc: "广泛最低PPC",
  maxbroadppc: "广泛最高PPC",
  broadppc: "广泛PPC",
  minexactppc: "精确最低PPC",
  maxexactppc: "精确最高PPC",
  exactppc: "精确PPC",
  top3asindtolist: "点击前三ASIN",
  trends: "趋势数据",
  word: "单词",
  word_zh: "中文释义",
  pos: "词性",
  category: "类别",
  plushable: "可毛绒",
  标签: "标签",
  原因: "原因",
  freq: "词频",
  freq_ratio: "词频占比",
  freq_ratio_percent: "词频占比(%)",
  coverage: "覆盖数",
  coverage_percent: "覆盖率(%)",
  total_searches: "总搜索量",
  total_searches_growth_rate: "月度增长率",
  total_searches_quarter_avg_growth_rate: "季度增长率",
  year: "年份",
  month: "月份",
  updated_at: "更新时间",
};

const exportLoading = ref(false);

const tableLoading = ref(false);
const tableLoadingMore = ref(false);
const currentTableMode = ref<DataTableMode>("aba");
const tableColumns = ref<string[]>([]);
const tableRows = ref<Record<string, unknown>[]>([]);
const tableTotal = ref(0);
const tablePage = ref(1);
const tablePageSize = ref(100);
const autoReloadReady = ref(false);
const suspendAutoReload = ref(false);
const panelFullscreen = ref(false);
const wordSearchInput = ref("");
const wordTrendLoading = ref(false);
const wordTrendError = ref("");
const wordTrendResult = ref<WordFrequencyTrendResponse | null>(null);
const hasKeywordSearchTriggered = ref(false);
const compareWordInput = ref("");
const compareTrendLoading = ref(false);
const compareTrendError = ref("");
const compareTrendResults = ref<WordFrequencyTrendResponse[]>([]);
const trendModalOpen = ref(false);
const trendModalTitle = ref("");
const trendModalPoints = ref<PgTrendPoint[]>([]);
type KeywordTop10State = {
  keyword: string;
  yearMonth: number | null;
  loading: boolean;
  loadingMore: boolean;
  error: string;
  columns: string[];
  items: Record<string, unknown>[];
  page: number;
  pageSize: number;
  total: number;
};
const keywordTop10Map = ref<Record<string, KeywordTop10State>>({});
const activeTop10Word = ref("");
const growthTop10Mode = ref<GrowthTop10Mode>("monthly");
const growthTop10Loading = ref(false);
const growthTop10Error = ref("");
const growthTop10ColumnsRaw = ref<string[]>([]);
const growthTop10Rows = ref<Record<string, unknown>[]>([]);
const growthTop10YearMonths = ref<number[]>([]);
const growthTop10SelectedYear = ref(0);
const growthTop10SelectedMonth = ref(0);
const growthTop10SearchMin = ref<string | number>("");
const growthTop10SearchMax = ref<string | number>("");

const yearMonths = ref<number[]>([]);
const selectedYear = ref(0);
const selectedMonth = ref(0);

const sortBy = ref<string>("id");
const sortDir = ref<"asc" | "desc">("asc");
const textFilters = ref<Record<string, TextFilterValue>>({});
const valueFilters = ref<Record<string, string[]>>({});
const orderedColumns = ref<string[]>([]);

const tablePanelRef = ref<HTMLElement | null>(null);
const tableWrapRef = ref<HTMLElement | null>(null);
const top10WrapRef = ref<HTMLElement | null>(null);
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
const filterTextRule = ref<{ op: TextFilterOp; value: string }>({
  op: "contains",
  value: "",
});
const filterOptions = ref<PgFilterOption[]>([]);
const filterOptionsLoading = ref(false);
const workingFilterValues = ref<string[]>([]);

let longPressTimer: ReturnType<typeof setTimeout> | null = null;
let filterTimer: ReturnType<typeof setTimeout> | null = null;
let growthTop10FilterTimer: ReturnType<typeof setTimeout> | null = null;
let activeTableRequestSeq = 0;
let scrollCheckRaf = 0;
let top10ScrollCheckRaf = 0;
let activeFilterRequestSeq = 0;
let activeGrowthTop10RequestSeq = 0;

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

const growthTop10YearOptions = computed(() => {
  const years = new Set<number>();
  for (const ym of growthTop10YearMonths.value) years.add(Math.floor(ym / 100));
  return Array.from(years).sort((a, b) => b - a);
});

const growthTop10MonthOptions = computed(() => {
  const months = new Set<number>();
  for (const ym of growthTop10YearMonths.value) {
    const year = Math.floor(ym / 100);
    const month = ym % 100;
    if (growthTop10SelectedYear.value === 0 || growthTop10SelectedYear.value === year) months.add(month);
  }
  return Array.from(months).sort((a, b) => a - b);
});

const currentTableMeta = computed(() => DATA_TABLES[currentTableMode.value]);
const currentTableName = computed(() => currentTableMeta.value.tableName);
const currentTableTitle = computed(() => currentTableMeta.value.title);

const excludedColumns = computed(() => {
  const base = new Set(BASE_EXCLUDED_COLUMNS);
  if (currentTableMode.value === "word") {
    for (const col of WORD_TABLE_FORCE_HIDDEN_COLUMNS) base.add(col);
  }
  return base;
});

const availableColumns = computed(() => tableColumns.value.filter((c) => !excludedColumns.value.has(c)));

const sortFieldOptions = computed(() => {
  const cols = [...availableColumns.value];
  if (!cols.includes("id")) cols.unshift("id");
  return cols;
});

const baseDisplayColumns = computed(() => {
  if (currentTableMode.value === "aba") {
    const availableSet = new Set(availableColumns.value);
    return ABA_FIXED_COLUMNS.filter((c) => availableSet.has(c));
  }
  return availableColumns.value;
});

const displayColumns = computed(() => {
  const base = baseDisplayColumns.value;
  if (!orderedColumns.value.length) return base;
  const baseSet = new Set(base);
  const ordered = orderedColumns.value.filter((c) => baseSet.has(c));
  const missing = base.filter((c) => !ordered.includes(c));
  return [...ordered, ...missing];
});

const workingFilterSet = computed(() => new Set(workingFilterValues.value));
const dragPreviewValues = computed(() => {
  if (!draggingCol.value) return [];
  return tableRows.value.slice(0, 8).map((row) => formatCell(getCell(row, draggingCol.value), draggingCol.value));
});

const visibleFilterOptions = computed(() => filterOptions.value);
const hasMoreRows = computed(() => tableRows.value.length < tableTotal.value);
const compareSeriesForChart = computed(() =>
  compareTrendResults.value.map((item) => ({
    word: item.word,
    points: item.points || [],
  }))
);
const top10WordTabs = computed(() => {
  const words: string[] = [];
  const mainWord = String(wordTrendResult.value?.word || "").trim().toLowerCase();
  if (mainWord) words.push(mainWord);
  for (const item of compareTrendResults.value) {
    const w = String(item.word || "").trim().toLowerCase();
    if (w && !words.includes(w)) words.push(w);
  }
  return words.map((word) => ({
    word,
    label: word === mainWord ? `${word}（主词）` : word,
  }));
});
const activeTop10Data = computed(() => {
  const key = String(activeTop10Word.value || "").trim().toLowerCase();
  if (!key) return null;
  return keywordTop10Map.value[key] || null;
});
const activeTop10HasMore = computed(() => {
  const data = activeTop10Data.value;
  if (!data) return false;
  return data.total > 0 && data.items.length < data.total;
});
const activeTop10Columns = computed(() => {
  const data = activeTop10Data.value;
  if (!data) return [] as string[];
  const available = Array.isArray(data.columns) && data.columns.length
    ? data.columns
    : data.items.length
      ? Object.keys(data.items[0])
      : [];
  const availableSet = new Set(available);
  return TOP10_VISIBLE_COLUMNS.filter((col) => availableSet.has(col));
});
const growthTop10SortField = computed(() =>
  growthTop10Mode.value === "monthly"
    ? "total_searches_growth_rate"
    : growthTop10Mode.value === "quarterly"
      ? "total_searches_quarter_avg_growth_rate"
      : "total_searches"
);
const growthTop10Columns = computed(() => {
  const availableSet = new Set(growthTop10ColumnsRaw.value);
  return GROWTH_TOP10_VISIBLE_COLUMNS.filter((col) => availableSet.has(col));
});
const filterRuleNeedsValue = computed(
  () => filterTextRule.value.op !== "is_blank" && filterTextRule.value.op !== "is_not_blank"
);

function columnLabel(col: string): string {
  return FIELD_LABELS[col] || col;
}

function syncOrderedColumnsState(): void {
  const base = baseDisplayColumns.value;
  if (!orderedColumns.value.length) {
    orderedColumns.value = [...base];
    return;
  }
  const baseSet = new Set(base);
  const ordered = orderedColumns.value.filter((c) => baseSet.has(c));
  const missing = base.filter((c) => !ordered.includes(c));
  orderedColumns.value = [...ordered, ...missing];
}

function isColumnValueFiltered(col: string): boolean {
  if (Array.isArray(valueFilters.value[col]) && valueFilters.value[col].length > 0) return true;
  const textFilter = textFilters.value[col];
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

async function initYearMonthFilters(): Promise<void> {
  yearMonths.value = await fetchPgYearMonthsByTable(currentTableName.value);
  if (!yearMonths.value.length) {
    selectedYear.value = 0;
    selectedMonth.value = 0;
    return;
  }
  const latest = yearMonths.value[0];
  selectedYear.value = Math.floor(latest / 100);
  selectedMonth.value = latest % 100;
}

async function initGrowthTop10Filters(): Promise<void> {
  growthTop10YearMonths.value = await fetchPgYearMonthsByTable(DATA_TABLES.word.tableName);
  if (!growthTop10YearMonths.value.length) {
    growthTop10SelectedYear.value = 0;
    growthTop10SelectedMonth.value = 0;
    return;
  }
  const latest = growthTop10YearMonths.value[0];
  growthTop10SelectedYear.value = Math.floor(latest / 100);
  growthTop10SelectedMonth.value = latest % 100;
}

function resetGrowthTop10FilterValues(): void {
  growthTop10SearchMin.value = "";
  growthTop10SearchMax.value = "";
  const latest = growthTop10YearMonths.value[0];
  if (!latest) {
    growthTop10SelectedYear.value = 0;
    growthTop10SelectedMonth.value = 0;
    return;
  }
  growthTop10SelectedYear.value = Math.floor(latest / 100);
  growthTop10SelectedMonth.value = latest % 100;
}

type Top3AsinItem = {
  asin: string;
  imageUrl: string;
  clickRate: number | null;
  conversionRate: number | null;
};

function toObjectArray(value: unknown): Record<string, unknown>[] {
  const pickArrayFromObject = (obj: Record<string, unknown>): Record<string, unknown>[] => {
    const keys = ["items", "data", "list", "gkdatas", "gkDatas", "top3asindtolist", "top3AsinDtoList", "top3"];
    for (const key of keys) {
      const raw = obj[key];
      if (Array.isArray(raw)) {
        return raw.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"));
      }
    }
    // Object itself might already be a single row item.
    if (Object.keys(obj).length) {
      return [obj];
    }
    return [];
  };

  if (Array.isArray(value)) {
    return value.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"));
  }
  if (value && typeof value === "object") {
    return pickArrayFromObject(value as Record<string, unknown>);
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        return parsed.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"));
      }
      if (parsed && typeof parsed === "object") {
        return pickArrayFromObject(parsed as Record<string, unknown>);
      }
    } catch {
      return [];
    }
  }
  return [];
}

function pickNumber(obj: Record<string, unknown>, keys: string[]): number | null {
  for (const key of keys) {
    const raw = obj[key];
    if (raw === null || raw === undefined || raw === "") continue;
    const num = Number(raw);
    if (Number.isFinite(num)) return num;
  }
  return null;
}

function pickString(obj: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const raw = obj[key];
    if (raw === null || raw === undefined) continue;
    const text = String(raw).trim();
    if (text) return text;
  }
  return "";
}

function parseTop3Asins(value: unknown): Top3AsinItem[] {
  const rows = toObjectArray(value);
  return rows.slice(0, 3).map((item) => ({
    asin: pickString(item, ["asin", "ASIN", "id"]),
    imageUrl: pickString(item, ["imageUrl", "image", "img", "image_url"]),
    clickRate: pickNumber(item, ["clickRate", "click_rate", "clickShareRate", "click_share_rate"]),
    conversionRate: pickNumber(item, ["conversionRate", "conversion_rate", "cvr", "cvsShareRate"]),
  }));
}

function normalizeWordKey(word: string): string {
  return String(word || "").trim().toLowerCase();
}

function parseYearMonth(ym: number | null | undefined): { year?: number; month?: number } {
  const n = Number(ym);
  if (!Number.isFinite(n) || n <= 0) return {};
  const year = Math.floor(n / 100);
  const month = n % 100;
  if (year < 2000 || month < 1 || month > 12) return {};
  return { year, month };
}

async function loadGrowthTop10(): Promise<void> {
  const requestSeq = ++activeGrowthTop10RequestSeq;
  growthTop10Loading.value = true;
  growthTop10Error.value = "";
  growthTop10Rows.value = [];
  growthTop10ColumnsRaw.value = [];
  const minText = String(growthTop10SearchMin.value ?? "").trim();
  const maxText = String(growthTop10SearchMax.value ?? "").trim();
  const hasMin = minText.length > 0;
  const hasMax = maxText.length > 0;
  let minValue: number | null = null;
  let maxValue: number | null = null;

  if (hasMin) {
    minValue = Number(minText);
    if (!Number.isFinite(minValue)) {
      growthTop10Error.value = "搜索量最小值必须是数字";
      growthTop10Loading.value = false;
      return;
    }
  }
  if (hasMax) {
    maxValue = Number(maxText);
    if (!Number.isFinite(maxValue)) {
      growthTop10Error.value = "搜索量最大值必须是数字";
      growthTop10Loading.value = false;
      return;
    }
  }
  if (minValue !== null && maxValue !== null && minValue > maxValue) {
    growthTop10Error.value = "搜索量最小值不能大于最大值";
    growthTop10Loading.value = false;
    return;
  }

  try {
    const data = await fetchPgGrowthTop10({
      mode: growthTop10Mode.value,
      table: DATA_TABLES.word.tableName,
      limit: 10,
      year: growthTop10SelectedYear.value || undefined,
      month: growthTop10SelectedMonth.value || undefined,
      searchMin: minValue ?? undefined,
      searchMax: maxValue ?? undefined,
    });
    if (requestSeq !== activeGrowthTop10RequestSeq) return;
    growthTop10ColumnsRaw.value = data.columns || [];
    growthTop10Rows.value = data.items || [];
  } catch (error) {
    if (requestSeq !== activeGrowthTop10RequestSeq) return;
    console.error("loadGrowthTop10 failed:", error);
    growthTop10Error.value = "增长率TOP10查询失败，请稍后重试";
    growthTop10ColumnsRaw.value = [];
    growthTop10Rows.value = [];
  } finally {
    if (requestSeq === activeGrowthTop10RequestSeq) {
      growthTop10Loading.value = false;
    }
  }
}

function scheduleGrowthTop10Reload(delay = 180): void {
  if (!autoReloadReady.value) return;
  if (growthTop10FilterTimer) clearTimeout(growthTop10FilterTimer);
  growthTop10FilterTimer = setTimeout(() => {
    growthTop10FilterTimer = null;
    void loadGrowthTop10();
  }, delay);
}

async function switchGrowthTop10Mode(mode: GrowthTop10Mode): Promise<void> {
  if (growthTop10Mode.value === mode) return;
  growthTop10Mode.value = mode;
  await loadGrowthTop10();
}

async function fetchKeywordTop10(
  word: string,
  latestYearMonth?: number | null,
  options: { append?: boolean } = {}
): Promise<void> {
  const key = normalizeWordKey(word);
  if (!key) return;

  const prev = keywordTop10Map.value[key];
  const append = options.append === true;
  if (append) {
    if (!prev || prev.loading || prev.loadingMore) return;
    if (prev.total > 0 && prev.items.length >= prev.total) return;
  }

  const targetYearMonth = latestYearMonth ?? prev?.yearMonth ?? null;
  const nextPage = append ? Math.max(1, (prev?.page || 1) + 1) : 1;
  const pageSize = prev?.pageSize || TOP10_PAGE_SIZE;
  keywordTop10Map.value = {
    ...keywordTop10Map.value,
    [key]: {
      keyword: key,
      yearMonth: targetYearMonth,
      loading: !append,
      loadingMore: append,
      error: "",
      columns: prev?.columns ?? [],
      items: prev?.items ?? [],
      page: append ? prev?.page || 1 : 0,
      pageSize,
      total: prev?.total ?? 0,
    },
  };

  try {
    const { year, month } = parseYearMonth(targetYearMonth);
    const data = await fetchPgItems({
      table: DATA_TABLES.aba.tableName,
      page: nextPage,
      pageSize,
      year,
      month,
      sortBy: "searches",
      sortDir: "desc",
      textFilters: {
        [DATA_TABLES.aba.keywordCol]: {
          op: "contains_word",
          value: key,
        },
      },
      valueFilters: {},
    });
    const incomingItems: Record<string, unknown>[] = data.items || [];
    const items = append ? [...(prev?.items || []), ...incomingItems] : incomingItems;
    keywordTop10Map.value = {
      ...keywordTop10Map.value,
      [key]: {
        keyword: key,
        yearMonth: targetYearMonth,
        loading: false,
        loadingMore: false,
        error: "",
        columns: data.columns || [],
        items,
        page: Number(data.page || nextPage),
        pageSize: Number(data.page_size || pageSize),
        total: Number(data.total || 0),
      },
    };
  } catch (error) {
    console.error("fetchKeywordTop10 failed:", error);
    keywordTop10Map.value = {
      ...keywordTop10Map.value,
      [key]: {
        keyword: key,
        yearMonth: targetYearMonth,
        loading: false,
        loadingMore: false,
        error: "关键词数据查询失败，请稍后重试",
        columns: prev?.columns || [],
        items: prev?.items || [],
        page: prev?.page || 0,
        pageSize,
        total: prev?.total || 0,
      },
    };
  }
}

function isTop10NearBottom(): boolean {
  const el = top10WrapRef.value;
  if (!el) return false;
  const threshold = 220;
  return el.scrollTop + el.clientHeight >= el.scrollHeight - threshold;
}

async function maybeLoadMoreTop10Rows(): Promise<void> {
  const data = activeTop10Data.value;
  if (!data || !activeTop10HasMore.value) return;
  if (data.loading || data.loadingMore) return;
  if (!isTop10NearBottom()) return;
  await fetchKeywordTop10(data.keyword, data.yearMonth, { append: true });
}

function scheduleTop10LazyLoadCheck(): void {
  if (top10ScrollCheckRaf) return;
  top10ScrollCheckRaf = window.requestAnimationFrame(() => {
    top10ScrollCheckRaf = 0;
    void maybeLoadMoreTop10Rows();
  });
}

function onTop10WrapScroll(): void {
  scheduleTop10LazyLoadCheck();
}

function buildAsinUrl(asinRaw: string): string {
  const asin = String(asinRaw || "").trim();
  if (!asin) return "https://www.amazon.com";
  return `https://www.amazon.com/dp/${encodeURIComponent(asin)}`;
}

function parseTop3Brands(value: unknown): string[] {
  const normalizeBrand = (raw: string): string => {
    const text = raw.trim();
    if (!text) return "-";
    const lowered = text.toLowerCase();
    if (lowered === "none" || lowered === "null" || lowered === "nan") return "-";
    return text;
  };

  const normalizeTop3 = (items: string[]): string[] => {
    if (!items.length) return [];
    const fixed = items.map((item) => normalizeBrand(item)).slice(0, 3);
    while (fixed.length < 3) fixed.push("-");
    return fixed;
  };

  if (Array.isArray(value)) {
    return normalizeTop3(
      value
      .map((item) => {
        if (typeof item === "string") return item;
        if (item && typeof item === "object") {
          return pickString(item as Record<string, unknown>, ["brand", "name", "label"]);
        }
        return "";
      })
      .slice(0, 3)
    );
  }
  if (typeof value === "string") {
    const text = value.trim();
    if (!text) return [];
    try {
      const parsed = JSON.parse(text);
      return parseTop3Brands(parsed);
    } catch {
      return normalizeTop3(text
        .split(/[,\n|]+/)
        .slice(0, 3));
    }
  }
  return [];
}

function formatPercent(value: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const normalized = Math.abs(value) <= 1 ? value * 100 : value;
  return `${normalized.toFixed(2)}%`;
}

function formatInteger(value: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return Math.round(value).toLocaleString("en-US");
}

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function getCell(row: Record<string, unknown>, key: string): unknown {
  return row[key];
}

function cellToText(value: unknown): string {
  if (value === null || value === undefined) return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatCell(value: unknown, col?: string): string {
  if (
    col === "w4rankgrowthrate" ||
    col === "total_searches_growth_rate" ||
    col === "total_searches_quarter_avg_growth_rate"
  ) {
    return formatPercent(toNumber(value));
  }
  const text = cellToText(value);
  return text.length > 80 ? `${text.slice(0, 80)}...` : text;
}

function getRowSerial(idx: number): number {
  return idx + 1;
}

function openTrendModalFromRow(row: Record<string, unknown>, points: PgTrendPoint[]): void {
  if (!points.length) return;
  const keyword = String(row.keyword ?? row.word ?? "").trim();
  trendModalTitle.value = keyword ? `${keyword} 趋势数据` : "趋势数据";
  trendModalPoints.value = points;
  trendModalOpen.value = true;
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

async function loadTable(options: { append?: boolean } = {}): Promise<void> {
  const append = options.append === true;
  if (append) {
    if (!hasMoreRows.value || tableLoading.value || tableLoadingMore.value) return;
  }

  const requestPage = append ? tablePage.value + 1 : 1;
  const requestSeq = ++activeTableRequestSeq;
  if (append) tableLoadingMore.value = true;
  else tableLoading.value = true;

  try {
    const data = await fetchPgItems({
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      page: requestPage,
      pageSize: tablePageSize.value,
      sortBy: sortBy.value || undefined,
      sortDir: sortDir.value,
      textFilters: normalizedTextFilters(),
      valueFilters: normalizedValueFilters(),
      table: currentTableName.value,
    });
    if (requestSeq !== activeTableRequestSeq) return;

    tableColumns.value = data.columns;
    tableTotal.value = data.total;
    tablePage.value = requestPage;
    if (append) {
      if (!data.items.length) {
        tableTotal.value = tableRows.value.length;
      } else {
        tableRows.value = [...tableRows.value, ...data.items];
      }
    } else {
      tableRows.value = data.items;
    }
    syncOrderedColumnsState();
  } finally {
    if (append) tableLoadingMore.value = false;
    else tableLoading.value = false;
    if (requestSeq === activeTableRequestSeq) scheduleLazyLoadCheck();
  }
}

async function reloadTable(): Promise<void> {
  tablePage.value = 0;
  if (tableWrapRef.value) {
    tableWrapRef.value.scrollTop = 0;
  }
  await loadTable({ append: false });
}

function triggerRealtimeReload(): void {
  if (!autoReloadReady.value) return;
  if (suspendAutoReload.value) return;
  void reloadTable();
}

async function switchTable(mode: DataTableMode): Promise<void> {
  if (currentTableMode.value === mode) return;
  suspendAutoReload.value = true;
  currentTableMode.value = mode;
  closeFilterMenu();
  textFilters.value = {};
  valueFilters.value = {};
  tableColumns.value = [];
  tableRows.value = [];
  tableTotal.value = 0;
  tablePage.value = 1;
  orderedColumns.value = [];
  sortBy.value = "id";
  sortDir.value = "asc";
  try {
    await initYearMonthFilters();
    await loadTable();
  } finally {
    suspendAutoReload.value = false;
  }
}

async function openFilterMenu(col: string, event: MouseEvent): Promise<void> {
  const target = event.currentTarget as HTMLElement | null;
  const rect = target?.getBoundingClientRect();
  if (!rect) return;

  filterMenu.value.open = true;
  filterMenu.value.column = col;
  const existingTextFilter = textFilters.value[col];
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
  if (preferredY + menuHeight <= maxY) {
    filterMenu.value.y = preferredY;
  } else {
    filterMenu.value.y = Math.max(viewportPadding, rect.top - menuHeight - 8);
  }
  workingFilterValues.value = [];
  await loadFilterOptionsForMenu(filterMenu.value.keyword.trim(), true);
}

function isSameValueSet(values: string[], options: PgFilterOption[]): boolean {
  if (values.length !== options.length) return false;
  const set = new Set(values);
  for (const option of options) {
    if (!set.has(option.value)) return false;
  }
  return true;
}

async function loadFilterOptionsForMenu(keyword: string, initialize = false): Promise<void> {
  const col = filterMenu.value.column;
  if (!col) return;

  const requestSeq = ++activeFilterRequestSeq;
  filterOptionsLoading.value = true;
  const existing = valueFilters.value[col];
  const shouldAutoSelectNext =
    !existing?.length && isSameValueSet(workingFilterValues.value, filterOptions.value);

  try {
    const options = await fetchPgFilterOptions({
      column: col,
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      keyword: keyword || undefined,
      textFilters: normalizedTextFilters(),
      valueFilters: normalizedValueFilters(),
      limit: keyword ? 20000 : 500,
      table: currentTableName.value,
    });

    if (!filterMenu.value.open || filterMenu.value.column !== col) return;
    if (requestSeq !== activeFilterRequestSeq) return;
    filterOptions.value = options;

    if (existing && existing.length > 0) {
      if (initialize && !workingFilterValues.value.length) {
        workingFilterValues.value = [...existing];
      }
      return;
    }
    if (initialize || shouldAutoSelectNext) {
      workingFilterValues.value = options.map((x) => x.value);
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
  filterMenu.value.column = "";
  filterMenu.value.keyword = "";
  filterTextRule.value = { op: "contains", value: "" };
  filterOptions.value = [];
  workingFilterValues.value = [];
}

function selectAllFilterValues(): void {
  workingFilterValues.value = filterOptions.value.map((x) => x.value);
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
    workingFilterValues.value = workingFilterValues.value.filter((x) => x !== value);
  }
}

function clearColumnFilter(): void {
  if (!filterMenu.value.column) return;
  delete textFilters.value[filterMenu.value.column];
  delete valueFilters.value[filterMenu.value.column];
  closeFilterMenu();
  void reloadTable();
}

function applyTextRuleFilter(): void {
  const col = filterMenu.value.column;
  if (!col) return;
  const op = filterTextRule.value.op;
  const value = filterTextRule.value.value.trim();
  if (filterRuleNeedsValue.value && !value) {
    alert("请输入条件值");
    return;
  }

  if (op === "contains") {
    textFilters.value[col] = value;
  } else if (op === "is_blank" || op === "is_not_blank") {
    textFilters.value[col] = { op };
  } else {
    textFilters.value[col] = { op, value };
  }
  delete valueFilters.value[col];
  closeFilterMenu();
  void reloadTable();
}

function applyFilterMenu(): void {
  const col = filterMenu.value.column;
  if (!col) return;
  const allValues = filterOptions.value.map((x) => x.value);
  const selected = [...new Set(workingFilterValues.value)];
  const keyword = filterMenu.value.keyword.trim();
  const keywordActive = keyword.length > 0;

  if (!selected.length) {
    delete textFilters.value[col];
    delete valueFilters.value[col];
  } else if (keywordActive && selected.length === allValues.length) {
    textFilters.value[col] = keyword;
    delete valueFilters.value[col];
  } else if (!keywordActive && selected.length === allValues.length) {
    delete textFilters.value[col];
    delete valueFilters.value[col];
  } else {
    delete textFilters.value[col];
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
    const job = await createPgExportJob({
      year: selectedYear.value || undefined,
      month: selectedMonth.value || undefined,
      sortBy: sortBy.value || undefined,
      sortDir: sortDir.value,
      textFilters: normalizedTextFilters(),
      valueFilters: normalizedValueFilters(),
      table: currentTableName.value,
    });

    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
    let fileName = `pg_${currentTableMode.value}_full_${selectedYear.value || "all"}_${selectedMonth.value || "all"}.csv`;
    let completed = false;

    for (let i = 0; i < 1800; i += 1) {
      const state = await fetchPgExportJob(job.job_id);
      if (state.status === "completed") {
        if (state.file_name) fileName = state.file_name;
        completed = true;
        break;
      }
      if (state.status === "failed") {
        throw new Error(state.error || "导出任务失败");
      }
      await sleep(1000);
    }

    if (!completed) {
      throw new Error("导出任务超时，请重试");
    }

    const blob = await downloadPgExportJob(job.job_id);
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch (error) {
    console.error("exportFullCsv failed:", error);
    alert("导出失败，请稍后重试。");
  } finally {
    exportLoading.value = false;
  }
}

function isNearBottom(): boolean {
  const el = tableWrapRef.value;
  if (!el) return false;
  const threshold = 220;
  return el.scrollTop + el.clientHeight >= el.scrollHeight - threshold;
}

async function maybeLoadMoreRows(): Promise<void> {
  if (!autoReloadReady.value) return;
  if (!hasMoreRows.value) return;
  if (tableLoading.value || tableLoadingMore.value) return;
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

function onFullscreenChange(): void {
  panelFullscreen.value = document.fullscreenElement === tablePanelRef.value;
  scheduleLazyLoadCheck();
}

async function searchWordTrend(): Promise<void> {
  const word = wordSearchInput.value.trim().toLowerCase();
  if (!word) return;
  hasKeywordSearchTriggered.value = true;
  wordTrendLoading.value = true;
  wordTrendError.value = "";
  compareTrendError.value = "";
  compareTrendResults.value = [];
  compareWordInput.value = "";
  keywordTop10Map.value = {};
  activeTop10Word.value = "";
  try {
    const data = await fetchWordFrequencyTrend(word);
    wordTrendResult.value = data;
    const mainWord = normalizeWordKey(data.word || word);
    if (mainWord) {
      activeTop10Word.value = mainWord;
      await fetchKeywordTop10(mainWord, data.latest_year_month ?? null);
    }
  } catch (error) {
    console.error("searchWordTrend failed:", error);
    wordTrendError.value = "查询失败，请稍后重试";
    wordTrendResult.value = null;
  } finally {
    wordTrendLoading.value = false;
  }
}

async function searchCompareTrend(): Promise<void> {
  const baseWord = wordTrendResult.value?.word?.trim().toLowerCase();
  if (!baseWord) {
    compareTrendError.value = "请先查询主词";
    return;
  }

  const raw = compareWordInput.value.trim().toLowerCase();
  if (!raw) {
    compareTrendError.value = "请输入对比词（可多个）";
    return;
  }

  const inputWords = Array.from(
    new Set(
      raw
        .split(/[,\s，]+/)
        .map((part) => part.trim().toLowerCase())
        .filter(Boolean)
    )
  );
  if (!inputWords.length) {
    compareTrendError.value = "请输入有效对比词";
    return;
  }
  const existing = new Set(compareTrendResults.value.map((item) => item.word.toLowerCase()));
  const words = inputWords.filter((item) => item !== baseWord && !existing.has(item));
  if (!words.length) {
    compareTrendError.value = "输入词已存在或与主词相同";
    return;
  }

  compareTrendLoading.value = true;
  compareTrendError.value = "";
  try {
    const settled = await Promise.allSettled(words.map((item) => fetchWordFrequencyTrend(item)));
    const appended: WordFrequencyTrendResponse[] = [];
    const failed: string[] = [];
    for (let i = 0; i < settled.length; i += 1) {
      const result = settled[i];
      const word = words[i];
      if (result.status === "fulfilled") {
        appended.push(result.value);
      } else {
        failed.push(word);
      }
    }
    if (appended.length) {
      compareTrendResults.value = [...compareTrendResults.value, ...appended];
      compareWordInput.value = "";
      await Promise.allSettled(
        appended.map((item) => fetchKeywordTop10(item.word, item.latest_year_month ?? null))
      );
    }
    if (failed.length) {
      compareTrendError.value = `以下词查询失败: ${failed.join(", ")}`;
    }
  } catch (error) {
    console.error("searchCompareTrend failed:", error);
    compareTrendError.value = "对比查询失败，请稍后重试";
  } finally {
    compareTrendLoading.value = false;
  }
}

function clearCompareTrend(): void {
  compareTrendResults.value = [];
  compareTrendError.value = "";
  compareWordInput.value = "";
  const mainWord = normalizeWordKey(wordTrendResult.value?.word || "");
  if (mainWord && keywordTop10Map.value[mainWord]) {
    keywordTop10Map.value = { [mainWord]: keywordTop10Map.value[mainWord] };
    activeTop10Word.value = mainWord;
  } else {
    keywordTop10Map.value = {};
    activeTop10Word.value = "";
  }
}

function removeCompareTrend(word: string): void {
  compareTrendResults.value = compareTrendResults.value.filter((item) => item.word !== word);
  const key = normalizeWordKey(word);
  const mainWord = normalizeWordKey(wordTrendResult.value?.word || "");
  if (key && key !== mainWord) {
    const next = { ...keywordTop10Map.value };
    delete next[key];
    keywordTop10Map.value = next;
    if (activeTop10Word.value === key) {
      activeTop10Word.value = mainWord || top10WordTabs.value[0]?.word || "";
    }
  }
}

async function toggleTableFullscreen(): Promise<void> {
  const panel = tablePanelRef.value;
  if (!panel) return;
  try {
    if (document.fullscreenElement === panel) {
      await document.exitFullscreen();
      return;
    }
    if (document.fullscreenElement) {
      await document.exitFullscreen();
    }
    await panel.requestFullscreen();
  } catch (error) {
    console.error("toggleTableFullscreen failed:", error);
  }
}

watch(selectedYear, () => {
  if (selectedMonth.value !== 0 && !monthOptions.value.includes(selectedMonth.value)) {
    selectedMonth.value = 0;
    return;
  }
  triggerRealtimeReload();
});

watch(growthTop10SelectedYear, () => {
  if (growthTop10SelectedMonth.value !== 0 && !growthTop10MonthOptions.value.includes(growthTop10SelectedMonth.value)) {
    growthTop10SelectedMonth.value = 0;
    return;
  }
  scheduleGrowthTop10Reload(0);
});

watch([growthTop10SelectedMonth, growthTop10SearchMin, growthTop10SearchMax], () => {
  scheduleGrowthTop10Reload();
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

watch(
  top10WordTabs,
  (tabs) => {
    if (!tabs.length) {
      activeTop10Word.value = "";
      return;
    }
    if (!tabs.some((tab) => tab.word === activeTop10Word.value)) {
      activeTop10Word.value = tabs[0].word;
    }
  },
  { deep: true }
);

watch(
  () => filterMenu.value.keyword,
  (keyword) => {
    if (!filterMenu.value.open || !filterMenu.value.column) return;
    if (filterTimer) clearTimeout(filterTimer);
    filterTimer = setTimeout(() => {
      void loadFilterOptionsForMenu(keyword.trim(), false);
    }, 220);
  }
);

onMounted(async () => {
  document.addEventListener("pointerdown", handleDocumentPointerDown);
  document.addEventListener("fullscreenchange", onFullscreenChange);
  try {
    await initYearMonthFilters();
  } catch (error) {
    console.error("initYearMonthFilters failed:", error);
  }
  try {
    await initGrowthTop10Filters();
  } catch (error) {
    console.error("initGrowthTop10Filters failed:", error);
  }
  try {
    await loadTable();
  } catch (error) {
    console.error("loadTable failed:", error);
  }
  try {
    await loadGrowthTop10();
  } catch (error) {
    console.error("loadGrowthTop10 failed:", error);
  }
  autoReloadReady.value = true;
  scheduleLazyLoadCheck();
});

onBeforeUnmount(() => {
  if (longPressTimer) clearTimeout(longPressTimer);
  if (filterTimer) clearTimeout(filterTimer);
  if (growthTop10FilterTimer) clearTimeout(growthTop10FilterTimer);
  clearDragState();
  cleanupDragHandlers();
  document.removeEventListener("pointerdown", handleDocumentPointerDown);
  document.removeEventListener("fullscreenchange", onFullscreenChange);
  if (scrollCheckRaf) {
    window.cancelAnimationFrame(scrollCheckRaf);
    scrollCheckRaf = 0;
  }
  if (top10ScrollCheckRaf) {
    window.cancelAnimationFrame(top10ScrollCheckRaf);
    top10ScrollCheckRaf = 0;
  }
});
</script>

<style scoped>
.word-trend-panel {
  margin-bottom: 16px;
  padding: 16px 18px;
  background: linear-gradient(180deg, #eef4ff 0%, #f5f9ff 100%);
  border-radius: 14px;
  border: 1px solid #d4e2fb;
  box-shadow: 0 8px 24px rgba(31, 64, 131, 0.06);
}

.word-trend-header {
  display: flex;
  align-items: end;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.word-trend-controls {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin-left: auto;
  flex-wrap: wrap;
  padding: 8px;
  border: 1px solid #d6e3ff;
  border-radius: 10px;
  background: rgba(255, 255, 255, 0.78);
  max-width: 100%;
}

.word-trend-input {
  width: min(290px, 45vw);
  min-width: 180px;
  height: 36px;
  border: 1px solid #b8c8ea;
  border-radius: 8px;
  padding: 0 12px;
  background: #fff;
  outline: none;
  transition: border-color 0.16s ease, box-shadow 0.16s ease;
}

.word-trend-input:focus {
  border-color: #3f6fda;
  box-shadow: 0 0 0 3px rgba(63, 111, 218, 0.16);
}

.trend-search-btn {
  margin-top: 0;
  height: 36px;
  padding: 0 14px;
  border-radius: 8px;
  box-shadow: 0 6px 14px rgba(33, 89, 219, 0.22);
}

.compare-word-input {
  min-width: 240px;
}

.trend-compare-btn,
.trend-clear-btn {
  height: 36px;
  margin-top: 0;
}

.compare-word-chips {
  margin-top: 8px;
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.compare-word-chips-bottom {
  margin-top: 0;
  align-items: center;
}

.compare-word-chip {
  display: inline-flex;
  align-items: center;
  border: 1px solid #d7e7fb;
  background: #f4f8ff;
  color: #264782;
  border-radius: 999px;
  font-size: 13px;
  padding: 4px 10px;
  line-height: 1.2;
  cursor: pointer;
}

.compare-word-chip:hover {
  background: #e9f1ff;
  border-color: #b8cff3;
}

.compare-summary-row {
  margin-top: 8px;
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.compare-word-meta-inline {
  margin-top: 8px;
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}

.compare-word-meta-pill {
  display: inline-flex;
  align-items: center;
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.82);
  border: 1px solid #d8e5fe;
  color: #243650;
  font-size: 13px;
}

.compare-summary-row .word-trend-meta {
  margin-top: 0;
}

.word-trend-meta {
  margin-top: 10px;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  color: #243650;
  font-size: 13px;
}

.word-trend-meta span {
  display: inline-flex;
  align-items: center;
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.75);
  border: 1px solid #d8e5fe;
}

.compare-meta span {
  border-color: #cdebd6;
  background: rgba(237, 252, 242, 0.8);
}

.word-trend-error {
  margin-top: 8px;
  color: #be2c2c;
  font-size: 13px;
  background: #fff1f1;
  border: 1px solid #ffcfd0;
  border-radius: 8px;
  padding: 6px 10px;
}

.compare-error {
  background: #f2fbff;
  border-color: #c9e6ff;
  color: #215f8c;
}

.word-trend-chart-wrap {
  margin-top: 10px;
  min-height: 320px;
  background: #fff;
  border: 1px solid #d6e0f1;
  border-radius: 8px;
  padding: 10px;
}

.word-trend-loading,
.word-trend-empty {
  min-height: 300px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #5c6b84;
  font-size: 14px;
}

.keyword-top10-panel {
  margin-bottom: 16px;
  padding: 14px 16px;
  border-radius: 12px;
  border: 1px solid #dbe7fb;
  background: #f9fbff;
}

.keyword-top10-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.growth-top10-header {
  align-items: center;
}

.growth-top10-filters {
  margin-top: 0;
  margin-left: auto;
  justify-content: flex-end;
}

.growth-top10-filters select,
.growth-top10-filters input {
  width: 100px;
  min-width: 100px;
  height: 35px;
  box-sizing: border-box;
}

.keyword-top10-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.growth-top10-header .keyword-top10-tabs {
  flex: 1 1 auto;
}

.keyword-top10-tab {
  border: 1px solid #cad8f7;
  background: #ffffff;
  color: #27467e;
  border-radius: 999px;
  padding: 4px 10px;
  font-size: 12px;
  cursor: pointer;
}

.keyword-top10-tab.active {
  border-color: #3f6fda;
  background: #edf4ff;
  color: #1f3f7a;
  font-weight: 600;
}

.keyword-top10-wrap {
  margin-top: 10px;
  overflow: auto;
  overscroll-behavior: contain;
}

.growth-top10-table-wrap {
  margin-top: 10px;
}

.keyword-top10-table {
  width: 100%;
  border-collapse: collapse;
  min-width: 760px;
}

.keyword-top10-table th,
.keyword-top10-table td {
  padding: 8px 10px;
  border-bottom: 1px solid #e4ecfb;
  text-align: left;
  vertical-align: middle;
  font-size: 13px;
}

.keyword-top10-table thead th {
  background: #eff5ff;
  color: #1f3f7a;
  position: sticky;
  top: 0;
  z-index: 1;
}

.keyword-top10-empty,
.keyword-top10-loading {
  margin-top: 10px;
  color: #556480;
  font-size: 13px;
}

.keyword-top10-loading.more {
  padding: 10px 0 2px;
  text-align: center;
}

.keyword-top10-error {
  margin-top: 10px;
  color: #b4232c;
  font-size: 13px;
}


.data-table td:not(.top3-cell) {
  vertical-align: middle !important;
}

.top3-cell {
  white-space: normal !important;
  min-width: 280px;
  vertical-align: middle;
}

.trend-cell {
  min-width: 204px;
  max-width: 204px;
  width: 204px;
  vertical-align: middle;
}

.top3-asin-cell {
  display: flex;
  align-items: flex-start;
  gap: 14px;
  min-height: 74px;
}

.top3-asin-item {
  width: 74px;
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
}

.top3-asin-image {
  width: 52px;
  height: 52px;
  border-radius: 6px;
  object-fit: cover;
  border: 1px solid #d8e4fb;
  background: #fff;
}

.top3-asin-image.empty {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  color: #7a8aa8;
  font-size: 12px;
}

.top3-asin-link {
  max-width: 100%;
  font-size: 11px;
  color: #1d4ed8;
  text-decoration: underline;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.top3-asin-link.empty {
  color: #7a8aa8;
  text-decoration: none;
}

.top3-asin-metric {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  color: #253758;
  font-size: 11px;
  line-height: 1.2;
}

.top3-asin-metric strong {
  font-size: 11px;
  font-weight: 700;
  color: #0f172a;
  font-variant-numeric: tabular-nums;
}

.top3-brand-cell {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 74px;
  justify-content: center;
}

.top3-brand-item {
  font-size: 13px;
  font-weight: 600;
  color: #1f3b73;
  line-height: 1.2;
}

.sortable-header {
  cursor: grab;
  user-select: none;
}

.sortable-header:active {
  cursor: grabbing;
}

.drag-source-col {
  background: #e8f0ff !important;
}

.table-wrap {
  position: relative;
  overflow: auto;
  max-height: 62vh;
  transition: filter 0.18s ease, opacity 0.18s ease;
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

.table-wrap.is-loading .data-table {
  filter: blur(1.2px);
  opacity: 0.55;
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

.table-loading-spinner {
  width: 18px;
  height: 18px;
  border-radius: 50%;
  border: 2px solid rgba(50, 85, 190, 0.28);
  border-top-color: #3255be;
  animation: table-loading-spin 0.8s linear infinite;
}

.table-loading-text {
  font-size: 13px;
  color: #1f2f4a;
  font-weight: 600;
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

.excel-filter-pop {
  width: 360px;
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
  padding: 0;
  border: 0;
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
  border-radius: 8px;
  border: 1px solid #c8d6f0;
  height: 36px;
}

.excel-filter-search:focus {
  outline: none;
  border-color: #3f6fda;
  box-shadow: 0 0 0 3px rgba(63, 111, 218, 0.16);
}

.excel-filter-actions {
  margin: 8px 10px 0;
  gap: 6px;
}

.excel-filter-actions .secondary-btn {
  padding: 0.38rem 0.6rem;
  border-radius: 7px;
}

.excel-filter-list {
  margin: 8px 10px 0;
  border-radius: 9px;
}

.excel-filter-option {
  padding: 0.46rem 0.5rem;
  transition: background 0.15s ease;
}

.excel-filter-option:hover {
  background: #f6f9ff;
}

.excel-filter-option input {
  accent-color: #315fcd;
}

.excel-filter-loading {
  height: 140px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  color: #375181;
  font-size: 13px;
}

.table-panel:fullscreen,
.table-panel.panel-fullscreen {
  width: 100vw;
  height: 100vh;
  max-width: none;
  margin: 0;
  padding: 18px;
  border-radius: 0;
  background: #eef4ff;
  overflow: hidden;
}

.table-panel:fullscreen .table-wrap,
.table-panel.panel-fullscreen .table-wrap {
  max-height: calc(100vh - 235px);
  overflow: auto;
}

.table-panel:fullscreen .data-table thead th,
.table-panel.panel-fullscreen .data-table thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #fff;
}

.lazy-hint {
  color: #5c6b84;
  font-size: 13px;
}

.lazy-loading-inline {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: #234796;
  font-size: 13px;
}

.mini-spinner {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  border: 2px solid rgba(35, 71, 150, 0.25);
  border-top-color: #234796;
  animation: table-loading-spin 0.8s linear infinite;
}

@media (max-width: 900px) {
  .word-trend-controls {
    width: 100%;
    margin-left: 0;
    justify-content: flex-start;
  }

  .word-trend-input {
    flex: 1 1 260px;
    min-width: 0;
    width: auto;
  }

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
