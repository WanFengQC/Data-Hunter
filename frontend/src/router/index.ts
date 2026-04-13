import { createRouter, createWebHistory } from "vue-router";

import DashboardView from "@/views/DashboardView.vue";
import CompetitorView from "@/views/CompetitorView.vue";
import CompetitorReportView from "@/views/CompetitorReportView.vue";
import WeightedBlanketsPoundsView from "@/views/WeightedBlanketsPoundsView.vue";

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
    {
      path: "/weighted-blankets/pounds",
      name: "weighted-blankets-pounds",
      component: WeightedBlanketsPoundsView,
    },
  ],
});

export default router;
