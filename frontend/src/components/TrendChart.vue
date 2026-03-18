<template>
  <div ref="chartRef" class="chart"></div>
</template>

<script setup lang="ts">
import * as echarts from "echarts";
import { onBeforeUnmount, onMounted, ref, watch } from "vue";

import type { CategorySummary } from "@/types/data";

const props = defineProps<{
  rows: CategorySummary[];
}>();

const chartRef = ref<HTMLElement | null>(null);
let chart: echarts.ECharts | null = null;
const onResize = (): void => chart?.resize();

function renderChart(): void {
  if (!chart || !props.rows.length) {
    chart?.clear();
    return;
  }

  chart.setOption({
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "category",
      data: props.rows.map((item) => item.category),
    },
    yAxis: [
      { type: "value", name: "Count" },
      { type: "value", name: "Avg Length" },
    ],
    series: [
      {
        name: "Count",
        type: "bar",
        data: props.rows.map((item) => item.count),
        yAxisIndex: 0,
      },
      {
        name: "Avg Length",
        type: "line",
        data: props.rows.map((item) => item.avg_content_length),
        yAxisIndex: 1,
        smooth: true,
      },
    ],
  });
}

onMounted(() => {
  if (!chartRef.value) return;
  chart = echarts.init(chartRef.value);
  renderChart();
  window.addEventListener("resize", onResize);
});

watch(
  () => props.rows,
  () => {
    renderChart();
  },
  { deep: true },
);

onBeforeUnmount(() => {
  window.removeEventListener("resize", onResize);
  chart?.dispose();
  chart = null;
});
</script>
