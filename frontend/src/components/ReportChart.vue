<template>
  <div ref="chartRef" class="report-chart" :style="{ height }" />
</template>

<script setup lang="ts">
import * as echarts from "echarts";
import { nextTick, onBeforeUnmount, ref, watch } from "vue";
import type { EChartsOption } from "echarts";

const props = withDefaults(
  defineProps<{
    option: EChartsOption;
    height?: string;
  }>(),
  {
    height: "320px",
  }
);
const emit = defineEmits<{
  (e: "chart-click", params: unknown): void;
}>();

const chartRef = ref<HTMLElement | null>(null);
let chart: echarts.ECharts | null = null;

function renderChart(): void {
  if (!chartRef.value) return;
  if (!chart) {
    chart = echarts.init(chartRef.value);
    chart.on("click", (params) => {
      emit("chart-click", params);
    });
  }
  chart.setOption(props.option, { notMerge: true });
}

const onResize = (): void => {
  chart?.resize();
};

watch(
  () => props.option,
  async () => {
    await nextTick();
    renderChart();
  },
  { deep: true, immediate: true }
);

watch(
  () => props.height,
  async () => {
    await nextTick();
    chart?.resize();
  }
);

onBeforeUnmount(() => {
  window.removeEventListener("resize", onResize);
  chart?.dispose();
  chart = null;
});

watch(
  chartRef,
  (el) => {
    if (!el) return;
    window.addEventListener("resize", onResize);
  },
  { immediate: true }
);
</script>

<style scoped>
.report-chart {
  width: 100%;
}
</style>
