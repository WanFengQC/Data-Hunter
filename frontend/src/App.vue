<template>
  <div class="app-shell">
    <header class="topbar">
      <div class="topbar-inner">
        <div class="brand-block">
          <h1>Data Hunter</h1>
          <p>SellerSprite data workspace</p>
        </div>
        <nav class="topnav">
          <RouterLink to="/aba" class="nav-link">ABA数据选品</RouterLink>
          <RouterLink to="/competitors" class="nav-link">查竞品</RouterLink>
        </nav>
      </div>
    </header>
    <router-view />
  </div>
</template>

<style scoped>
.topbar {
  position: sticky;
  top: 0;
  z-index: 30;
  backdrop-filter: blur(14px);
  background: rgba(255, 255, 255, 0.88);
  border-bottom: 1px solid rgba(148, 163, 184, 0.18);
  box-shadow: 0 10px 26px rgba(15, 23, 42, 0.06);
}

.topbar-inner {
  width: min(1760px, 96vw);
  margin: 0 auto;
  padding: 12px 8px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}

.brand-block h1 {
  margin: 0;
  font-size: 1.22rem;
  line-height: 1.1;
  color: #0f172a;
}

.brand-block p {
  margin: 4px 0 0;
  font-size: 0.82rem;
  color: #64748b;
}

.topnav {
  display: flex;
  align-items: center;
  gap: 20px;
  flex-wrap: wrap;
  margin-left: 8px;
}

.nav-link {
  text-decoration: none;
  color: #475569;
  border-bottom: 2px solid transparent;
  padding: 6px 0;
  font-size: 0.9rem;
  font-weight: 600;
  transition:
    color 0.16s ease,
    border-color 0.16s ease;
}

.nav-link:hover {
  color: #0f172a;
}

.nav-link.router-link-active {
  color: #1d4ed8;
  border-bottom-color: #1d4ed8;
}

@media (max-width: 768px) {
  .topbar-inner {
    align-items: flex-start;
  }

  .topnav {
    width: 100%;
  }

  .nav-link {
    flex: 0 0 auto;
  }
}
</style>
