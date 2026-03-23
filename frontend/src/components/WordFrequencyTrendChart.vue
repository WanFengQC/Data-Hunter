<template>
  <div ref="chartRef" class="chart"></div>
</template>

<script setup lang="ts">
import * as echarts from "echarts";
import { onBeforeUnmount, onMounted, ref, watch } from "vue";

import type { WordFrequencyTrendPoint } from "@/types/data";

type CompareSeries = {
  word: string;
  points: WordFrequencyTrendPoint[];
};

const props = defineProps<{
  points: WordFrequencyTrendPoint[];
  primaryWord?: string;
  compareSeries?: CompareSeries[];
}>();

const chartRef = ref<HTMLElement | null>(null);
let chart: echarts.ECharts | null = null;

const onResize = (): void => chart?.resize();

function render(): void {
  if (!chart) return;
  const primaryPoints = props.points || [];
  const compareSeries = props.compareSeries || [];
  if (!primaryPoints.length && !compareSeries.length) {
    chart.clear();
    return;
  }

  const datasets: CompareSeries[] = [
    {
      word: props.primaryWord?.trim() || "主词",
      points: primaryPoints,
    },
    ...compareSeries
      .filter((item) => item && typeof item.word === "string" && Array.isArray(item.points))
      .map((item) => ({
        word: item.word.trim() || "对比词",
        points: item.points || [],
      })),
  ];

  const ymSet = new Set<number>();
  const collectYm = (point: WordFrequencyTrendPoint): void => {
    const ym = point.year_month;
    if (typeof ym === "number" && Number.isFinite(ym)) {
      ymSet.add(ym);
      return;
    }
    if (typeof point.year === "number" && typeof point.month === "number") {
      ymSet.add(point.year * 100 + point.month);
    }
  };
  for (const series of datasets) {
    series.points.forEach(collectYm);
  }

  const yms = Array.from(ymSet).sort((a, b) => a - b);
  if (!yms.length) {
    chart.clear();
    return;
  }

  const xData = yms.map((ym) => `${Math.floor(ym / 100)}-${String(ym % 100).padStart(2, "0")}`);
  const toMap = (items: WordFrequencyTrendPoint[]): Map<number, WordFrequencyTrendPoint> => {
    const m = new Map<number, WordFrequencyTrendPoint>();
    for (const point of items) {
      const ym =
        typeof point.year_month === "number"
          ? point.year_month
          : typeof point.year === "number" && typeof point.month === "number"
            ? point.year * 100 + point.month
            : null;
      if (ym !== null) m.set(ym, point);
    }
    return m;
  };
  const palette = ["#3468d8", "#f08a24", "#1a9e6f", "#e2568a", "#8a4df0", "#0ea5e9", "#ef4444", "#22c55e"];
  const series: echarts.SeriesOption[] = [];
  for (let index = 0; index < datasets.length; index += 1) {
    const dataset = datasets[index];
    const pointMap = toMap(dataset.points || []);
    const lineColor = palette[index % palette.length];
    series.push(
      {
        name: `${dataset.word} 搜索量`,
        type: "line",
        smooth: true,
        data: yms.map((ym) => pointMap.get(ym)?.total_searches ?? null),
        symbol: "circle",
        symbolSize: 5,
        lineStyle: { width: 2.2, type: "solid", color: lineColor },
        itemStyle: { color: lineColor },
        yAxisIndex: 0,
      },
      {
        name: `${dataset.word} 词频`,
        type: "line",
        smooth: true,
        data: yms.map((ym) => pointMap.get(ym)?.freq ?? null),
        symbol: "circle",
        symbolSize: 4,
        lineStyle: { width: 1.9, type: "dashed", color: lineColor },
        itemStyle: { color: lineColor },
        yAxisIndex: 1,
      }
    );
  }

  chart.setOption(
    {
      grid: { left: 96, right: 92, top: 82, bottom: 52, containLabel: true },
      legend: {
        type: "scroll",
        top: 8,
        left: 12,
        right: 12,
        itemWidth: 12,
        itemHeight: 8,
        pageIconSize: 10,
        pageButtonGap: 4,
        pageTextStyle: { color: "#64748b" },
      },
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: xData,
        axisLabel: { interval: 0, rotate: 30 },
      },
      yAxis: [
        {
          type: "value",
          name: "搜索量",
          axisLabel: {
            formatter: (value: number) => value.toLocaleString("en-US"),
          },
        },
        {
          type: "value",
          name: "词频",
          position: "right",
          axisLabel: {
            formatter: (value: number) => value.toLocaleString("en-US"),
          },
        },
      ],
      series,
    },
    { notMerge: true }
  );
}

onMounted(() => {
  if (!chartRef.value) return;
  chart = echarts.init(chartRef.value);
  render();
  window.addEventListener("resize", onResize);
});

watch(
  () => [props.points, props.compareSeries, props.primaryWord],
  () => render(),
  { deep: true },
);

onBeforeUnmount(() => {
  window.removeEventListener("resize", onResize);
  chart?.dispose();
  chart = null;
});
</script>

<style scoped>
.chart {
  width: 100%;
  height: 320px;
}
</style>
