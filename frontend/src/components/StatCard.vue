<template>
  <div class="stat-card">
    <div class="stat-title">{{ title }}</div>
    <div class="stat-value">{{ value }}</div>
    <div class="stat-subtitle">{{ subtitle }}</div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  title: string;
  value: string | number;
  subtitle?: string;
}>();
</script>
