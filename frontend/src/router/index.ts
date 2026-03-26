import { createRouter, createWebHistory } from "vue-router";

import DashboardView from "@/views/DashboardView.vue";
import CompetitorView from "@/views/CompetitorView.vue";

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
  ],
});

export default router;
