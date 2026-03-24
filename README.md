# Data Hunter

`Data Hunter` 是一个面向电商关键词与词频分析的全栈项目。

当前项目包含两条主线：
- ABA 关键词数据的查询、筛选、趋势展示
- 词频结果的统计、打标、增长率分析与可视化

技术栈：
- 后端：FastAPI
- 前端：Vue 3 + Vite + ECharts
- 数据库：MongoDB + PostgreSQL
- 部署：Docker / Docker Compose

## 目录结构

```text
data_hunter/
├─ backend/                    后端服务、数据库访问、脚本
│  ├─ app/                     FastAPI 应用
│  ├─ scripts/seller_sprite/   ABA/词频处理脚本
│  ├─ requirements.txt
│  ├─ .env
│  └─ Dockerfile.prod
├─ frontend/                   Vue 前端
│  ├─ src/
│  ├─ package.json
│  ├─ Dockerfile.prod
│  └─ nginx.prod.conf
├─ docker-compose.yml          本地开发用 compose
├─ docker-compose.prod.yml     生产部署用 compose
├─ start_backend.bat           Windows 启动后端
├─ start_frontend.bat          Windows 启动前端
└─ start_fullstack.bat         Windows 同时启动前后端
```

## 核心功能

- ABA 表查询、筛选、分页、懒加载
- 关键词搜索量趋势查询
- 对比词趋势对比
- 关键词结果列表懒加载
- 词频表查看与导出
- 月度增长率 TOP10 / 季度增长率 TOP10 / 总搜索量 TOP10
- SellerSprite 数据抓取与导入脚本
- 词频统计、标签和原因生成、增长率回写

## 环境要求

本地开发建议：
- Python 3.12
- Node.js 22
- MongoDB
- PostgreSQL

## 配置文件

后端配置文件：
- [backend/.env](C:\Users\EDY\WebstormProjects\data_hunter\backend\.env)

关键配置项：

```env
APP_NAME=Data Hunter API
APP_ENV=dev
API_PREFIX=/api/v1

MONGO_URI=mongodb://admin:123456@192.168.110.107:27017/?authSource=admin
MONGO_DB=testdb

PG_HOST=192.168.110.107
PG_USER=postgres
PG_PASS=123456
PG_PORT=5432
PG_DB=hunter
PG_SCHEMA=public
PG_TABLE=seller_sprite_items
```

说明：
- `PG_DB` 决定后端默认连接哪个 PostgreSQL 数据库
- 如果要切到新库，把 `PG_DB=hunter_new`

## 本地运行

### 方式一：Windows 脚本

直接双击或在命令行执行：

```bat
start_fullstack.bat
```

单独启动：

```bat
start_backend.bat
start_frontend.bat
```

默认地址：
- 前端：[http://127.0.0.1:5173](http://127.0.0.1:5173)
- 后端文档：[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

### 方式二：手动启动

后端：

```bash
cd backend
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

前端：

```bash
cd frontend
npm install
npm run dev
```

## 生产 Docker 部署

生产部署文件：
- [docker-compose.prod.yml](C:\Users\EDY\WebstormProjects\data_hunter\docker-compose.prod.yml)

当前生产端口：
- `18080`

访问地址：
- [http://192.168.110.107:18080](http://192.168.110.107:18080)

### 1. 安装 Docker

CentOS 7.9 参考命令：

```bash
yum install -y yum-utils device-mapper-persistent-data lvm2
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl enable docker
systemctl start docker
```

### 2. 上传代码

将整个项目上传到服务器，例如：

```bash
scp -r data_hunter root@192.168.110.107:/opt/
```

### 3. 修改后端配置

编辑：
- [backend/.env](C:\Users\EDY\WebstormProjects\data_hunter\backend\.env)

确认 `MONGO_URI`、`PG_HOST`、`PG_DB` 等连接信息正确。

### 4. 启动

```bash
cd /opt/data_hunter
docker compose -f docker-compose.prod.yml up -d --build
```

如果服务器没有 `docker compose` 子命令：

```bash
docker-compose -f docker-compose.prod.yml up -d --build
```

### 5. 查看状态

```bash
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs -f backend
docker compose -f docker-compose.prod.yml logs -f frontend
```

### 6. 开放防火墙端口

```bash
firewall-cmd --permanent --add-port=18080/tcp
firewall-cmd --reload
```

## 常用脚本

脚本目录：
- `backend/scripts/seller_sprite/`

常见用途：
- `get_pages.py`：抓 ABA 原始数据
- `import_items_to_postgres.py`：导入 ABA 数据到 PostgreSQL
- `get_keywords.py`：生成词频 CSV
- `import_word_frequency_to_postgres.py`：导入词频 CSV 到 PostgreSQL
- `calc_word_frequency_growth.py`：计算月度/季度增长率

## 数据说明

主要 PostgreSQL 表：
- `public.seller_sprite_items`
  - ABA 明细数据
- `public.seller_sprite_word_frequency`
  - 词频分析结果

词频表中常见字段：
- `word`
- `word_zh`
- `标签`
- `原因`
- `freq`
- `total_searches`
- `total_searches_growth_rate`
- `total_searches_quarter_avg_growth_rate`

## 切换数据库

默认数据库由后端 `.env` 控制：

```env
PG_DB=hunter
```

切换到新库：

```env
PG_DB=hunter_new
```

修改后需要重启后端服务或重建容器。

## 故障排查

### 前端能打开，但接口报错

重点检查：
- 后端是否正常启动
- `backend/.env` 里的 MongoDB / PostgreSQL 配置是否可连
- Docker 部署时 `backend` 容器日志是否报错

### 前端没有数据

重点检查：
- `PG_DB` 是否指向正确数据库
- `seller_sprite_items` 和 `seller_sprite_word_frequency` 是否已经导入

### Docker 启动失败

检查：

```bash
docker compose -f docker-compose.prod.yml logs -f
```

## 后续建议

- 开发环境和生产环境配置分离
- 将敏感信息从仓库配置中移出
- 为词频处理脚本补充统一入口和参数说明
- 为 Docker 生产环境增加 MongoDB / PostgreSQL 的可选编排
