# 架构设计（FastAPI + Vue3 + MongoDB）

## 1. 分层架构
- `crawler layer`：负责从目标站点抓取原始数据，输出统一结构。
- `ingest layer`：负责清洗基础字段并写入 Mongo 原始集合 `raw_records`。
- `process layer`：负责聚合/特征提取，结果写入 `processed_records` 和 `analytics_snapshots`。
- `api layer`：通过 FastAPI 提供触发任务、查询原始数据、查询分析结果等接口。
- `presentation layer`：Vue3 前端读取 API，进行统计卡片与趋势图展示。

## 2. 数据流
1. 定时或手动触发采集任务。
2. 爬虫抓取网页/接口数据，转换为标准结构。
3. 入库到 `raw_records`（保留采集时间、来源、原始字段）。
4. 执行处理管道，写入 `processed_records`。
5. 生成仪表盘所需汇总快照，供前端快速查询。

## 3. Mongo 集合建议
- `raw_records`: 原始采集数据（可追溯）
- `processed_records`: 清洗与标准化后的数据
- `analytics_snapshots`: 预聚合结果（按天/按类别统计）

## 4. 可扩展建议
- 采集规模扩大后，将任务调度迁移到 `Celery + Redis` 或 `Kafka + Worker`。
- 引入 `Playwright` 处理动态页面。
- 对高频查询字段建立索引，例如 `source`, `fetched_at`, `processed_at`, `category`。
- 增加数据质量校验（重复率、缺失率、异常值）并记录到 `quality_reports`。
