# 架构设计（FastAPI + Vue3 + PostgreSQL）

## 1. 分层架构
- `script layer`：负责 SellerSprite/词频相关的采集、清洗、导入脚本。
- `storage layer`：负责将 ABA 明细、词频结果和缓存数据写入 PostgreSQL。
- `api layer`：通过 FastAPI 提供 PostgreSQL 查询、导出、趋势分析等接口。
- `presentation layer`：Vue3 前端读取 API，进行表格、筛选、趋势图和报表展示。

## 2. 数据流
1. 通过脚本抓取 SellerSprite/相关来源数据。
2. 将原始导出结果解析为结构化 CSV 或 JSON。
3. 通过导入脚本写入 PostgreSQL 业务表与缓存表。
4. 在 PostgreSQL 上计算增长率、趋势和筛选结果。
5. 前端通过 API 直接查询 PostgreSQL 数据并展示。

## 3. PostgreSQL 表建议
- `seller_sprite_items`: ABA 商品明细与衍生字段
- `seller_sprite_word_frequency`: 词频分析结果与增长率字段
- `seller_sprite_word_cache`: 翻译、标签、Google Trends 等缓存数据

## 4. 可扩展建议
- 采集规模扩大后，将任务调度迁移到 `Celery + Redis` 或 `Kafka + Worker`。
- 引入 `Playwright` 处理动态页面。
- 对高频查询字段建立索引，例如 `source`, `fetched_at`, `processed_at`, `category`。
- 增加数据质量校验（重复率、缺失率、异常值）并记录到 `quality_reports`。
