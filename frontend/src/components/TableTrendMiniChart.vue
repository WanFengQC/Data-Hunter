<template>
  <button
    class="trend-mini-btn"
    :class="{ clickable: hasTrendData }"
    type="button"
    :disabled="!hasTrendData"
    :title="hasTrendData ? `点击放大查看 ${rangeText}` : '暂无趋势数据'"
    @click="openModal"
  >
    <svg v-if="hasTrendData" class="trend-mini-svg" :viewBox="`0 0 ${CHART_W} ${CHART_H}`" preserveAspectRatio="none">
      <defs>
        <linearGradient :id="gradientId" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="#8eb6ff" stop-opacity="0.35" />
          <stop offset="100%" stop-color="#8eb6ff" stop-opacity="0.04" />
        </linearGradient>
      </defs>
      <path :d="areaPath" :fill="`url(#${gradientId})`" />
      <polyline :points="linePointsText" fill="none" stroke="#3a68d8" stroke-width="2.2" stroke-linecap="round" />
      <circle
        v-if="lastPoint"
        :cx="lastPoint.x"
        :cy="lastPoint.y"
        r="3.2"
        fill="#3a68d8"
        stroke="#fff"
        stroke-width="1.2"
      />
    </svg>
    <span v-else class="trend-mini-empty">-</span>
    <span v-if="hasTrendData" class="trend-mini-range">{{ rangeText }}</span>
  </button>
</template>

<script setup lang="ts">
import { computed } from "vue";

import type { PgTrendPoint } from "@/types/data";
import { formatTrendLabel, parsePgTrendPoints } from "@/utils/trend";

const CHART_W = 170;
const CHART_H = 62;
const PAD_X = 8;
const PAD_TOP = 6;
const PAD_BOTTOM = 8;

const props = defineProps<{
  rawTrend: unknown;
}>();

const emit = defineEmits<{
  (event: "open", points: PgTrendPoint[]): void;
}>();

const gradientId = `trend-mini-fill-${Math.random().toString(36).slice(2, 10)}`;

const trendPoints = computed(() => parsePgTrendPoints(props.rawTrend));
const trendValues = computed(() =>
  trendPoints.value
    .map((item, idx) => ({
      idx,
      value: typeof item.searches === "number" && Number.isFinite(item.searches) ? item.searches : null,
    }))
    .filter((item): item is { idx: number; value: number } => item.value !== null)
);

const hasTrendData = computed(() => trendValues.value.length >= 2);

const rangeText = computed(() => {
  if (!trendPoints.value.length) return "";
  const first = trendPoints.value[0];
  const last = trendPoints.value[trendPoints.value.length - 1];
  return `${formatTrendLabel(first.label)} ~ ${formatTrendLabel(last.label)}`;
});

const normalizedPoints = computed(() => {
  if (!hasTrendData.value) return [];
  const min = Math.min(...trendValues.value.map((item) => item.value));
  const max = Math.max(...trendValues.value.map((item) => item.value));
  const span = max - min || 1;
  const xStep = trendPoints.value.length > 1 ? (CHART_W - PAD_X * 2) / (trendPoints.value.length - 1) : 0;
  const yRange = CHART_H - PAD_TOP - PAD_BOTTOM;
  return trendValues.value.map((item) => {
    const x = PAD_X + xStep * item.idx;
    const y = PAD_TOP + ((max - item.value) / span) * yRange;
    return { x, y };
  });
});

const linePointsText = computed(() => normalizedPoints.value.map((item) => `${item.x},${item.y}`).join(" "));
const lastPoint = computed(() =>
  normalizedPoints.value.length ? normalizedPoints.value[normalizedPoints.value.length - 1] : null
);

const areaPath = computed(() => {
  if (!normalizedPoints.value.length) return "";
  const first = normalizedPoints.value[0];
  const last = normalizedPoints.value[normalizedPoints.value.length - 1];
  const line = normalizedPoints.value.map((item) => `${item.x},${item.y}`).join(" ");
  return `M ${first.x} ${CHART_H - PAD_BOTTOM} L ${line} L ${last.x} ${CHART_H - PAD_BOTTOM} Z`;
});

function openModal(): void {
  if (!hasTrendData.value) return;
  emit("open", trendPoints.value);
}
</script>

<style scoped>
.trend-mini-btn {
  width: 186px;
  min-width: 186px;
  height: 84px;
  border: 1px solid #d7e4fb;
  background: linear-gradient(180deg, #ffffff 0%, #f7faff 100%);
  border-radius: 10px;
  padding: 6px 8px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 4px;
  transition: border-color 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.trend-mini-btn.clickable {
  cursor: pointer;
}

.trend-mini-btn.clickable:hover {
  border-color: #9cb6ef;
  box-shadow: 0 6px 16px rgba(58, 104, 216, 0.18);
  transform: translateY(-1px);
}

.trend-mini-svg {
  width: 100%;
  height: 62px;
}

.trend-mini-range {
  font-size: 11px;
  color: #45608d;
  font-variant-numeric: tabular-nums;
  text-align: center;
  line-height: 1.1;
}

.trend-mini-empty {
  color: #7d8ba7;
  font-size: 12px;
  text-align: center;
}
</style>
