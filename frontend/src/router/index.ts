import { createRouter, createWebHistory } from "vue-router";

import DashboardView from "@/views/DashboardView.vue";
import CompetitorView from "@/views/CompetitorView.vue";
import CompetitorReportView from "@/views/CompetitorReportView.vue";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: "/",
      redirect: "/aba",
    },
    {
      path: "/aba",
      name: "dashboard",
      component: DashboardView,
    },
    {
      path: "/competitors",
      name: "competitors",
      component: CompetitorView,
    },
    {
      path: "/competitors/report",
      name: "competitor-report",
      component: CompetitorReportView,
    },
  ],
});

export default router;
