# Excel Table Mapper

将表1（如 `2512`）转换为表2结构（如 `Unique`）：

- 输入：搜索词明细（至少包含 `搜索词`、`预估周曝光量`、`展示量`、`点击量`）
- 输出：`单词 / 频次 / 打标 / mark / Weight / Total / 占比`
- 打标：优先命中现有缓存，再按需调用 AI 补全
- 计算：`Weight`、`Total`、`占比`按你模板表2公式等价实现

## 目录

```text
excel_table_mapper/
  requirements.txt
  run.py
  src/excel_table_mapper/
    __init__.py
    cli.py
    processor.py
    tagger.py
```

## 安装

```powershell
cd C:\Users\EDY\WebstormProjects\data_hunter\excel_table_mapper
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
```

## 运行

### 可执行文件（推荐）

```powershell
cd C:\Users\EDY\WebstormProjects\data_hunter\excel_table_mapper
.\build_exe.bat
```

构建完成后运行：

`dist\ExcelTableMapper\ExcelTableMapper.exe`

界面能力：
- 选择输入文件（xlsx/csv）
- 选择输入 sheet（xlsx）
- 设置输出路径
- 运行模式可选：`在线模式（本地+数据库同步）` / `离线模式（仅本地缓存）`
- 默认启用 `AI打标`
- 预览前 200 行结果

### 命令行

```powershell
.\.venv\Scripts\python .\run.py `
  --input "C:\Users\EDY\Downloads\流量分析模型V5-毛绒玩具-gnomantic-B0DRHXCWTM-20-2512.xlsx" `
  --sheet "2512" `
  --output ".\outputs\unique_result.xlsx" `
  --enable-ai
```

### 开发调试（不打包）

```powershell
.\.venv\Scripts\python .\desktop_app.py
```

在线模式直连 PostgreSQL（示例：`192.168.110.107`），不依赖 FastAPI。

## 参数说明

- `--input`：输入 Excel/CSV 文件（表1）
- `--sheet`：输入 sheet 名（输入为 xlsx 时必填，默认第一个 sheet）
- `--output`：输出文件路径，支持 `.xlsx` / `.csv`
- `--cache-dir`：缓存目录，默认复用现有项目缓存：
  - `backend/scripts/seller_sprite/translation_cache.json`
  - `backend/scripts/seller_sprite/tag_reason_cache.json`
- `--enable-ai`：缓存缺失时调用 AI 补打标
- `--model`：AI 模型名，默认 `gpt-5.4`
- `--base-url`：AI 网关，默认 `https://api.wfqc8.cn/v1`
- `--batch-size`：AI 批量大小，默认 `50`

## 环境变量

- `OPENAI_API_KEY`：开启 `--enable-ai` 时必填
- `AI_BASE_URL`：可覆盖 `--base-url`
- `AI_MODEL`：可覆盖 `--model`

## 公式等价实现

按模板 `Unique` 的逻辑（从第4行开始）：

- `Total = SUMPRODUCT(点击量 * 是否包含该单词)`
- `Weight = ROUND((Total / SUMPRODUCT(展示量 * 是否包含该单词)) * (4.3 * SUMPRODUCT(预估周曝光量 * 是否包含该单词)) * 10, 0)`
- `占比 = IFERROR(Weight / Total, "")`

其中“是否包含该单词”使用词边界分词后匹配，避免子串误匹配。
