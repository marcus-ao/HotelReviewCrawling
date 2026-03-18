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
│   ├── anti_crawler.py         # 反爬虫策略（DrissionPage接管Chrome、自动化滑块验证码处理）
│   ├── hotel_list_crawler.py   # 酒店列表爬虫（分层抽样策略实现）
│   └── review_crawler.py       # 评论爬虫（双池策略：negative自动 / positive人工辅助）
│
├── database/                    # 数据库模块
│   ├── __init__.py             # 模块初始化，导出ORM模型和连接函数
│   ├── connection.py           # 数据库连接管理（SQLAlchemy引擎、会话）
│   ├── models.py               # ORM模型定义（Hotel、Review、ReviewNegative、ReviewPositive）
│   └── init_db.sql             # PostgreSQL初始化脚本（含索引、视图、触发器）
│
├── utils/                       # 工具模块
│   ├── __init__.py             # 模块初始化
│   ├── logger.py               # 日志配置（Loguru，按天轮转，错误日志分离）
│   ├── cleaner.py              # 数据清洗（HTML清理、评分解析、日期解析）
│   └── validator.py            # Pydantic数据验证模型（HotelModel、ReviewModel）
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
# 爬取指定酒店评论（negative自动 + positive人工辅助）
python main.py --mode reviews --hotel-id 10019773

# 仅爬取指定酒店的负向评论
python main.py --mode reviews --hotel-id 10019773 --negative-only

# 明确启用正向人工辅助模式
python main.py --mode reviews --hotel-id 10019773 --positive-manual

# 爬取所有酒店的负向评论
python main.py --mode reviews --all
```

正向人工辅助模式说明：

1. 程序会自动进入酒店评论页，并尽量切到“好评”视图。
2. 终端会提示你手工翻页或确认当前页已渲染完成。
3. 每按一次 Enter，程序会抓取当前页评论 HTML、去重、过滤掉非正向评论后保存。
4. 输入 `s` 可跳过当前页，输入 `q` 可结束正向采集。

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

### 阶段二：评论详情采集（双池策略 + 动态质量控制）

每家酒店当前采用双池策略：

| 池名 | 方式 | 目标 | 说明 |
|------|------|------|------|
| `negative` | 全自动 | 差评在线主采 | 先按动态负评软目标抓取，再根据实际可得性接受短缺 |
| `positive` | 半自动 | 正向评论补充 | 人工翻页/渲染，程序负责 HTML 提取、去重、质量筛选和入库 |

说明：
- 单酒店默认启用 `positive` 人工辅助模式
- 批量 `--all` 默认只跑 `negative`
- 单酒店总评论目标不再固定为 300，而是根据 `review_count` 动态计算
- 评论会先做信息量分层：优先保留中长、高信息评论；数量不足时再按酒店评论量分档动态放宽
- 评论数少于阈值的酒店仍会被记录告警，但不强制跳过

### 阶段三：数据清洗与存储

- **数据验证**: Pydantic模型校验
- **文本清洗**: 去HTML标签、规范化空白、提取标签
- **信息量分层**: 计算有效长度、方面词命中数、质量等级（S/A/B/C/D）
- **动态放宽**: 高评论量酒店更严格，低评论量酒店允许少量“短但具体”的评论进入
- **评分解析**: CSS width百分比转换为1-5分
- **去重检查**: 基于review_id去重
- **入库存储**: SQLAlchemy ORM批量写入

### 关键评论调参项

以下配置是当前评论采集最常调整的几组参数，建议优先通过 `.env` 调整：

| 参数 | 作用 | 调大后效果 | 调小后效果 |
|------|------|-----------|-----------|
| `REVIEW_TOTAL_SAMPLE_RATIO` | 单酒店动态总量比例 | 每家酒店抓得更深 | 每家酒店抓得更浅 |
| `REVIEW_NEGATIVE_TARGET_RATIO` | 负评软目标比例 | 负评占比更高 | 更偏向正评与主流体验 |
| `REVIEW_POSITIVE_MIN_EFFECTIVE_LEN` | 正评默认最低有效长度 | 正评更偏长评论、证据更充分 | 更容易保留短正评 |
| `REVIEW_NEGATIVE_MIN_EFFECTIVE_LEN` | 负评默认最低有效长度 | 负评更严格 | 更容易保留短问题评论 |
| `REVIEW_SHORT_COMMENT_MAX_RATIO_HIGH/MID/LOW` | 各评论量档位短评占比上限 | 更容易放宽短评 | 更强调中长评论 |
| `REVIEW_QUALITY_RELAX_TRIGGER_RATIO` | 进入放宽阶段的触发阈值 | 更早放宽，覆盖更全 | 更晚放宽，质量更高 |

经验建议：
- 如果知识库里“空泛好评”过多，优先提高 `REVIEW_POSITIVE_MIN_EFFECTIVE_LEN`
- 如果低评论量酒店有效评论太少，优先提高 `REVIEW_SHORT_COMMENT_MAX_RATIO_LOW`
- 如果负评明显抓不到，优先降低 `REVIEW_NEGATIVE_MIN_EFFECTIVE_LEN`

### 验证码自动化处理

项目已实现完全自动化的滑块验证码处理：

**核心特性**:
- ✅ 人类化拖拽轨迹（S曲线、随机抖动、超冲回弹）
- ✅ 有界重试机制（最多3次重试）
- ✅ 超时感知（120秒超时检测）
- ✅ 验证页刷新（过期页面自动刷新，最多2次）
- ✅ 冷却停止（重试耗尽后180秒冷却，当前任务停止）
- ✅ 中断安全（增量保存，已完成工作不丢失）

**配置参数** (`config/settings.py`):
```python
CAPTCHA_MAX_RETRIES = 3          # 最大重试次数
CAPTCHA_TIMEOUT = 120            # 验证超时（秒）
CAPTCHA_COOLDOWN = 180           # 冷却时间（秒）
CAPTCHA_REFRESH_RETRY_LIMIT = 2  # 验证页刷新次数
```

**异常处理**:
- `CaptchaException`: 可重试的验证码失败（自动重试）
- `CaptchaCooldownException`: 达到重试上限，任务停止并进入冷却期

**恢复机制**:
- 酒店列表：每页保存，中断后从下一页继续
- 评论采集：负向池分段保存，正向人工辅助页按当前页提取

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
| `reviews` | 评论主表（全部评论） | review_id, hotel_id, content, score_*, source_pool |
| `reviews_negative` | 负面评论子表 | review_id, hotel_id, content, overall_score, review_date |
| `reviews_positive` | 正面评论子表 | review_id, hotel_id, content, overall_score, review_date |

说明：当前双池策略仅将文本评论和必要元数据写入数据库，图片、商家回复、任务日志与任务表均已从活跃架构中移除。

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
- **验证码**: 已实现自动化处理，有界重试和冷却停止机制

### 运行建议
- 建议在网络稳定的环境下运行
- 首次运行建议使用测试模式验证配置
- 爬取过程中保持Chrome浏览器窗口可见
- 验证码已自动化处理，无需人工介入（除非达到重试上限）

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
