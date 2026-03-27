<template>
  <Teleport to="body">
    <div v-if="open" class="trend-modal-mask" @click.self="emitClose">
      <div class="trend-modal-card">
        <div class="trend-modal-header">
          <div class="trend-modal-title">{{ title || "趋势数据" }}</div>
          <button class="trend-modal-close" type="button" @click="emitClose">关闭</button>
        </div>
        <div ref="chartRef" class="trend-modal-chart" />
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import * as echarts from "echarts";
import { nextTick, onBeforeUnmount, ref, watch } from "vue";

import type { PgTrendPoint } from "@/types/data";
import { formatTrendLabel } from "@/utils/trend";

const props = defineProps<{
  open: boolean;
  title: string;
  points: PgTrendPoint[];
}>();

const emit = defineEmits<{
  (event: "close"): void;
}>();

const chartRef = ref<HTMLElement | null>(null);
let chart: echarts.ECharts | null = null;

function emitClose(): void {
  emit("close");
}

function formatNumber(value: number | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return value.toLocaleString("en-US");
}

function renderChart(): void {
  if (!chart) return;
  if (!props.points.length) {
    chart.clear();
    return;
  }

  const labels = props.points.map((item) => formatTrendLabel(item.label));
  const searches = props.points.map((item) => item.searches);

  chart.setOption(
    {
      grid: { left: 50, right: 30, top: 52, bottom: 42, containLabel: true },
      tooltip: {
        trigger: "axis",
        formatter: (params: Array<{ dataIndex: number; marker: string; value: number }>) => {
          const index = params?.[0]?.dataIndex ?? 0;
          const point = props.points[index];
          return [
            `<div>${labels[index] || "-"}</div>`,
            `${params?.[0]?.marker || ""} 搜索量: ${formatNumber(typeof params?.[0]?.value === "number" ? params[0].value : null)}`,
            `排名: ${formatNumber(point?.rank ?? null)}`,
          ].join("<br/>");
        },
      },
      xAxis: {
        type: "category",
        data: labels,
        boundaryGap: false,
        axisLabel: { rotate: 35 },
      },
      yAxis: {
        type: "value",
        name: "搜索量",
        axisLabel: {
          formatter: (value: number) => value.toLocaleString("en-US"),
        },
      },
      series: [
        {
          name: "搜索量",
          type: "line",
          smooth: true,
          data: searches,
          symbol: "circle",
          symbolSize: 6,
          lineStyle: { width: 2.4, color: "#2f63da" },
          itemStyle: { color: "#2f63da" },
          areaStyle: {
            color: "rgba(47, 99, 218, 0.12)",
          },
        },
      ],
    },
    { notMerge: true }
  );
}

function ensureChart(): void {
  if (!chartRef.value) return;
  if (!chart) {
    chart = echarts.init(chartRef.value);
  }
  renderChart();
}

const onResize = (): void => {
  chart?.resize();
};

watch(
  () => props.open,
  async (isOpen) => {
    if (isOpen) {
      await nextTick();
      ensureChart();
      window.addEventListener("resize", onResize);
    } else {
      window.removeEventListener("resize", onResize);
      chart?.dispose();
      chart = null;
    }
  }
);

watch(
  () => props.points,
  async () => {
    if (!props.open) return;
    await nextTick();
    ensureChart();
  },
  { deep: true }
);

onBeforeUnmount(() => {
  window.removeEventListener("resize", onResize);
  chart?.dispose();
  chart = null;
});
</script>

<style scoped>
.trend-modal-mask {
  position: fixed;
  inset: 0;
  z-index: 1200;
  background: rgba(14, 23, 41, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
}

.trend-modal-card {
  width: min(1100px, 94vw);
  height: min(680px, 90vh);
  background: #fff;
  border-radius: 14px;
  border: 1px solid #cfdbf3;
  box-shadow: 0 22px 56px rgba(18, 32, 62, 0.4);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.trend-modal-header {
  min-height: 52px;
  border-bottom: 1px solid #e2ebfb;
  padding: 10px 14px;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.trend-modal-title {
  flex: 1;
  min-width: 0;
  font-size: 16px;
  font-weight: 700;
  color: #1f3358;
  line-height: 1.35;
}

.trend-modal-close {
  flex: 0 0 auto;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid #bfd0ef;
  background: #f6f9ff;
  color: #234688;
  border-radius: 8px;
  height: 34px;
  min-width: 64px;
  padding: 0 14px;
  white-space: nowrap;
  line-height: 1;
  cursor: pointer;
}

.trend-modal-chart {
  flex: 1;
  min-height: 360px;
}
</style>
