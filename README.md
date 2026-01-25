# 飞猪酒店评论数据爬取项目

<div align="center">

![Version](https://img.shields.io/badge/version-v1.0-blue)
![Python](https://img.shields.io/badge/python-3.10%2B-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue)
![Crawler](https://img.shields.io/badge/DrissionPage-≥4.0.0-red)
![ORM](https://img.shields.io/badge/SQLAlchemy-≥2.0.0-orange)
![Data](https://img.shields.io/badge/Pydantic-≥2.0.0-yellow)

**飞猪酒店评论数据爬取项目**

基于酒店评论知识库与高效语言模型微调的推荐智能体构建研究

[项目简介](#项目简介) • [快速开始](#快速开始) • [爬取策略](#爬取策略) • [技术栈](#技术栈) • [详细文档](#详细文档)

</div>

## 项目简介

本项目是<strong>“基于酒店评论知识库与高效语言模型微调的推荐智能体构建研究”</strong>毕业课题的数据采集模块，用于从飞猪官网爬取广州市酒店评论数据构建高质量的酒店评论知识库。

### 核心目标
- **酒店样本**: 250-270家（覆盖广州6大功能区）
- **评论样本**: 75,000-81,000条（每家酒店最多300条）
- **数据质量**: 高信噪比、分布均衡、覆盖全面

### 应用场景
1. **RAG检索系统**: 支持基于评论的酒店推荐
2. **LLM微调**: 为7B规模模型提供领域训练数据
3. **情感分析**: 方面级情感分析（ABSA）研究

## 项目结构

```
HotelReviewCrawing/
├── config/                      # 配置模块
│   ├── __init__.py             # 模块初始化，导出Settings和GUANGZHOU_REGIONS
│   ├── settings.py             # 全局配置（数据库、Chrome、爬虫参数）
│   └── regions.py              # 广州6大功能区及18个商圈配置
│
├── crawler/                     # 爬虫模块
│   ├── __init__.py             # 模块初始化，导出三个爬虫类
│   ├── anti_crawler.py         # 反爬虫策略（DrissionPage接管Chrome、滑块处理）
│   ├── hotel_list_crawler.py   # 酒店列表爬虫（分层抽样策略实现）
│   └── review_crawler.py       # 评论爬虫（瀑布流采集策略实现）
│
├── database/                    # 数据库模块
│   ├── __init__.py             # 模块初始化，导出ORM模型和连接函数
│   ├── connection.py           # 数据库连接管理（SQLAlchemy引擎、会话）
│   ├── models.py               # ORM模型定义（Hotel、Review、CrawlTask等6张表）
│   └── init_db.sql             # PostgreSQL初始化脚本（含索引、视图、触发器）
│
├── utils/                       # 工具模块
│   ├── __init__.py             # 模块初始化
│   ├── logger.py               # 日志配置（Loguru，按天轮转，错误日志分离）
│   ├── cleaner.py              # 数据清洗（HTML清理、评分解析、日期解析）
│   └── validator.py            # Pydantic数据验证模型（HotelModel、ReviewModel）
│
├── scheduler/                   # 任务调度模块
│   ├── __init__.py             # 模块初始化
│   └── task_scheduler.py       # 任务管理器（创建、执行、状态跟踪、统计）
│
├── tests/                       # 测试模块
│   ├── __init__.py
│   └── test_crawler.py         # 单元测试（清洗工具、验证模型、配置测试）
│
├── plans/                       # 设计文档目录
│   ├── README.md               # 文档总览与快速导航
│   ├── 01_HTML结构分析.md       # 页面结构与数据提取点分析
│   ├── 02_数据库Schema设计.md   # 完整的数据库表结构设计
│   ├── 03_爬取流程架构.md       # 三阶段爬取流程详解
│   ├── 04_综合实施方案.md       # 完整的实施指南
│   └── 05_项目架构与文件清单.md  # 项目架构规划文档
│
├── logs/                        # 日志目录（自动生成）
├── data/                        # 数据目录
│   ├── raw/                    # 原始数据存放
│   ├── processed/              # 处理后数据存放
│   └── exports/                # 导出数据存放
│
├── .env.example                 # 环境变量示例文件
├── .gitignore                   # Git忽略配置
├── requirements.txt             # Python依赖清单
├── main.py                      # 主程序入口（CLI接口）
└── README.md                    # 项目说明（本文件）
```

## 快速开始

### 前置条件
- Python 3.10+
- PostgreSQL 12+ (推荐安装pgvector扩展)
- Chrome浏览器
- 飞猪账号

### 安装步骤

#### 1. 进入项目目录
```bash
cd HotelReviewCrawing
```

#### 2. 创建虚拟环境
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

#### 3. 安装依赖
```bash
pip install -r requirements.txt
```

#### 4. 配置环境变量
```bash
# 复制环境变量示例文件
cp .env.example .env

# 编辑.env文件，填入实际配置
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=hotel_reviews
# DB_USER=postgres
# DB_PASSWORD=your_password
```

#### 5. 初始化数据库
```bash
# 创建数据库
createdb hotel_reviews

# 执行初始化SQL（创建表、索引、视图）
psql -d hotel_reviews -f database/init_db.sql

# 可选：安装pgvector扩展（用于后续向量检索）
psql -d hotel_reviews -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

#### 6. 启动Chrome调试模式
```bash
# Windows
chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\selenium\automation_profile"

# Mac
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --user-data-dir="/tmp/chrome_debug"

# Linux
google-chrome --remote-debugging-port=9222 --user-data-dir="/tmp/chrome_debug"
```

#### 7. 手动登录飞猪
在调试模式的Chrome中访问 https://www.fliggy.com/ 并完成扫码登录

### 使用方法

#### 测试模式（验证配置和连接）
```bash
python main.py --mode test
```

#### 爬取酒店列表
```bash
# 爬取指定功能区
python main.py --mode hotel_list --region "CBD商务区"

# 爬取所有功能区
python main.py --mode hotel_list --all
```

#### 爬取评论
```bash
# 爬取指定酒店的评论
python main.py --mode reviews --hotel-id 10019773

# 爬取所有酒店的评论
python main.py --mode reviews --all
```

#### 查看帮助
```bash
python main.py --help
```

## 爬取策略

### 阶段一：酒店候选集构建（分层抽样）

采用**区域分层 + 价格分层**策略，确保数据分布均衡：

| 功能区 | 代表商圈 | 价格档次 | 预计酒店数 |
|--------|----------|----------|-----------|
| CBD商务区 | 珠江新城、天河体育中心、环市东路 | 4档 | 45 |
| 老城文化区 | 北京路、上下九、越秀公园 | 4档 | 45 |
| 交通枢纽区 | 白云机场、广州南站、白云黄石 | 4档 | 45 |
| 会展活动区 | 琶洲会展、江南西、大沙地 | 4档 | 45 |
| 度假亲子区 | 长隆旅游度假区、从化温泉、花都融创 | 4档 | 45 |
| 高校科技区 | 大学城、科学城、知识城 | 4档 | 45 |
| **总计** | **18个商圈** | **4档** | **270家** |

**价格档次配置**:
- 经济型 (0-300元): 每商圈Top 4，按评分排序
- 舒适型 (300-600元): 每商圈Top 6，按销量排序
- 高档型 (600-1200元): 每商圈Top 3，按销量排序
- 奢华型 (1200元+): 每商圈Top 2，按综合排序

### 阶段二：评论详情采集（瀑布流策略）

每家酒店最多采集300条评论，按优先级依次采集：

| 优先级 | 来源池 | 筛选条件 | 上限 | 目的 |
|--------|--------|----------|------|------|
| 1 | 负面警示池 | 差评(1-2分) + 中评(3分) | 100条 | 挖掘酒店具体缺点 |
| 2 | 高质量证据池 | 有图/视频评论 | 150条 | RAG检索素材 |
| 3 | 时效性补全池 | 全部评论（最新） | 剩余配额 | 保证评论时效性 |

**熔断机制**: 评论数少于50条的酒店直接跳过

### 阶段三：数据清洗与存储

- **数据验证**: Pydantic模型校验
- **文本清洗**: 去HTML标签、规范化空白、提取标签
- **评分解析**: CSS width百分比转换为1-5分
- **去重检查**: 基于review_id去重
- **入库存储**: SQLAlchemy ORM批量写入

## 技术栈

| 组件 | 技术选型 | 版本要求 | 说明 |
|------|---------|---------|------|
| **爬虫框架** | DrissionPage | >=4.0.0 | 基于CDP协议，绕过webdriver检测 |
| **浏览器** | Chrome | 最新版 | Debug模式，手动登录后接管 |
| **数据库** | PostgreSQL | >=12 | 支持pgvector向量检索 |
| **ORM** | SQLAlchemy | >=2.0.0 | Python ORM框架 |
| **数据验证** | Pydantic | >=2.0.0 | 数据模型验证 |
| **日志** | Loguru | >=0.7.0 | 简洁的日志库 |
| **配置管理** | python-dotenv | >=1.0.0 | 环境变量管理 |
| **进度条** | tqdm | >=4.65.0 | 命令行进度显示 |
| **重试机制** | tenacity | >=8.2.0 | 自动重试装饰器 |

## 数据库表结构

| 表名 | 说明 | 主要字段 |
|------|------|----------|
| `hotels` | 酒店基础信息 | hotel_id, name, address, rating_score, region_type, price_level |
| `reviews` | 评论主表 | review_id, hotel_id, content, score_*, source_pool |
| `review_images` | 评论图片 | review_id, image_url, thumbnail_url |
| `review_replies` | 商家回复 | review_id, content, reply_date |
| `crawl_tasks` | 爬取任务 | task_id, task_type, status, priority |
| `crawl_logs` | 爬取日志 | task_id, level, message, details |

详细Schema设计请参考 [plans/02_数据库Schema设计.md](plans/02_数据库Schema设计.md)

## 详细文档

完整的设计文档请查看 [plans/](plans/) 目录：

| 文档 | 内容 |
|------|------|
| [README.md](plans/README.md) | 文档总览与快速导航 |
| [00_爬取策略逻辑.md](plans/00_爬取策略逻辑.md) | 酒店评论爬取逻辑设计方案 |
| [01_HTML结构分析.md](plans/01_HTML结构分析.md) | 飞猪页面结构与数据提取点分析 |
| [02_数据库Schema设计.md](plans/02_数据库Schema设计.md) | 完整的数据库表结构设计 |
| [03_爬取流程架构.md](plans/03_爬取流程架构.md) | 三阶段爬取流程详解 |
| [04_综合实施方案.md](plans/04_综合实施方案.md) | 关键技术难点与解决方案 |
| [05_项目架构与文件清单.md](plans/05_项目架构与文件清单.md) | 爬取项目整体架构规划 |

## 注意事项

### 法律合规
- 仅用于学术研究，不得用于商业用途
- 遵守robots.txt协议
- 控制爬取频率，避免对服务器造成压力
- 不得泄露用户隐私信息

### 技术风险
- **账号风险**: 准备多个账号备用
- **IP风险**: 必要时使用代理IP
- **数据丢失**: 定期备份数据库
- **验证码**: 降低频率，支持人工介入

### 运行建议
- 建议在网络稳定的环境下运行
- 首次运行建议使用测试模式验证配置
- 爬取过程中保持Chrome浏览器窗口可见
- 遇到验证码时根据提示手动处理

## 预期成果

| 指标 | 预期值 |
|------|--------|
| 酒店数量 | 250-270家 |
| 评论数量 | 75,000-81,000条 |
| 存储空间 | 约2GB |

## 后续数据处理

### 1. 方面级情感分析（PyABSA）
```python
from pyabsa import ATEPCCheckpointManager

aspect_extractor = ATEPCCheckpointManager.get_aspect_extractor(checkpoint='chinese')
result = aspect_extractor.extract_aspect(inference_source=[review_text])
```

### 2. 向量化（BGE-small-zh）
```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('BAAI/bge-small-zh')
embeddings = model.encode(texts)
```

### 3. LLM微调数据生成
```python
instruction_data = {
    "instruction": "根据以下酒店评论，推荐适合的酒店",
    "input": "我需要在广州找一家位置方便、卫生干净的酒店",
    "output": "推荐广工大酒店。理由：..."
}
```

## 许可证

本项目仅用于个人学术研究，不得用于商业用途。

---

**作者**：Marcus Ao
**最后更新**: 2026-01-25
**版本**: v1.0
