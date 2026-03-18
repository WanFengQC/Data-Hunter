# Data Hunter

`data_hunter` 是一个用于海量数据采集、入库、处理与可视化展示的基础项目骨架。

## 技术栈
- 后端: FastAPI + Motor(MongoDB Async Driver)
- 前端: Vue3 + Vite + ECharts
- 数据库: MongoDB
- 容器: Docker Compose

## 目录结构
- `backend/`: API、爬虫、数据处理逻辑
- `frontend/`: 可视化前端
- `docs/`: 架构设计文档
- `docker-compose.yml`: 本地一键启动编排

## 快速启动（Docker）
```bash
docker compose up --build
```

启动后：
- 后端 API: http://localhost:8000/docs
- 前端: http://localhost:5173
- MongoDB: localhost:27017

## 不使用 Docker 启动
### 后端
```bash
cd backend
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### 前端
```bash
cd frontend
npm install
npm run dev
```

## 首次调试流程
1. 打开 Swagger: `http://localhost:8000/docs`
2. 调用 `POST /api/v1/crawl/trigger` 触发采集与处理
3. 调用 `GET /api/v1/data/processed/summary` 检查聚合结果
4. 打开前端 Dashboard 查看图表
