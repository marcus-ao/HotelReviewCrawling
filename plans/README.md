# 飞猪酒店评论爬取方案 - 设计文档

## 项目概述

本目录包含**基于酒店评论知识库与高效语言模型微调的推荐智能体构建研究**数据采集部分的完整设计文档。

### 核心目标
- **酒店样本**: 250-270家（覆盖广州6大功能区）
- **评论样本**: 75,000-81,000条（每家酒店最多300条）
- **数据质量**: 高信噪比、分布均衡、覆盖全面

### 应用场景
1. **RAG检索系统**: 支持基于评论的酒店推荐
2. **LLM微调**: 为7B规模模型提供领域训练数据
3. **情感分析**: 方面级情感分析（ABSA）研究

## 文档结构

本方案包含5个核心文档，按顺序阅读可全面了解爬取方案：

### 1. [HTML结构分析](./01_HTML结构分析.md)

**内容概要**:
- 酒店列表页面结构分析
- 酒店详情页面结构分析
- 评论区域HTML结构详解
- 关键数据提取点总结

**关键发现**:
- 酒店列表数据通过`div.list-row.J_ListRow`元素呈现，包含`data-shid`、`data-name`等属性
- 评论数据在`li.tb-r-comment`元素中，包含4维度评分（清洁、位置、服务、性价比）
- 评分通过CSS width属性表示（width:100% = 5星，width:80% = 4星）
- 评论筛选支持：全部/好评/中评/差评/有图

### 2. [数据库Schema设计](./02_数据库Schema设计.md)

**内容概要**:
- 6张核心数据表设计
- 索引优化策略
- 数据统计视图
- 存储空间估算

**核心表结构**:

| 表名 | 说明 | 记录数估算 |
|------|------|-----------|
| `hotels` | 酒店基础信息 | 270 |
| `reviews` | 评论主表 | 80,000 |
| `review_images` | 评论图片 | 40,000 |
| `review_replies` | 商家回复 | 20,000 |
| `crawl_tasks` | 爬取任务管理 | 1,000 |
| `crawl_logs` | 爬取日志 | 10,000 |

**技术亮点**:
- 使用PostgreSQL，预留pgvector扩展支持向量检索
- 设计了完整的方面情感分析数据结构
- 包含统计视图和自动更新触发器

### 3. [爬取流程架构](./03_爬取流程架构.md)

**内容概要**:
- 系统整体架构设计
- 三阶段爬取流程详解
- 反爬虫策略实现
- 监控与日志系统

**爬取策略**:

#### 阶段一：酒店候选集构建（分层抽样）
```
6大功能区 × 3个商圈 × 4个价格档次 = 约270家酒店
```
- CBD商务区、老城文化区、交通枢纽区
- 会展活动区、度假亲子区、高校科技区

#### 阶段二：评论详情采集（瀑布流策略）
```
每家酒店最多300条评论：
1. 负面警示池：差评+中评，最多100条
2. 高质量证据池：有图评论，最多150条
3. 时效性补全池：全部评论，填满剩余配额
```

#### 阶段三：数据清洗与存储
- 数据验证（Pydantic）
- 文本清洗（去HTML、规范化）
- 去重检查
- 入库存储

### 4. [综合实施方案](./04_综合实施方案.md)

**内容概要**:
- 关键技术难点与解决方案
- 完整实施流程
- 后续数据处理流程
- 风险提示与注意事项

**关键技术难点**:

| 难点 | 解决方案 |
|------|---------|
| 动态加载的酒店列表 | DrissionPage等待渲染后提取DOM属性 |
| 评论分页加载机制 | 模拟点击翻页，基于review_id去重 |
| 评分提取 | 解析CSS width属性，转换为1-5分 |
| 反爬虫对抗 | DrissionPage接管Chrome + 智能延迟 + 滑块处理 |

### 5. [项目架构与文件清单](./05_项目架构与文件清单.md)

**内容概要**:
- 完整的项目目录结构规划
- 各模块功能说明
- 文件清单与职责划分

## 已实现的项目架构

```
HotelReviewCrawing/
├── config/                      # 配置模块
│   ├── __init__.py             # 导出Settings和GUANGZHOU_REGIONS
│   ├── settings.py             # 全局配置（Pydantic Settings）
│   └── regions.py              # 广州6大功能区及18个商圈配置
│
├── crawler/                     # 爬虫模块
│   ├── __init__.py             # 导出AntiCrawler、HotelListCrawler、ReviewCrawler
│   ├── anti_crawler.py         # 反爬虫策略（DrissionPage接管、滑块处理）
│   ├── hotel_list_crawler.py   # 酒店列表爬虫（分层抽样实现）
│   └── review_crawler.py       # 评论爬虫（瀑布流采集实现）
│
├── database/                    # 数据库模块
│   ├── __init__.py             # 导出ORM模型和连接函数
│   ├── connection.py           # SQLAlchemy引擎和会话管理
│   ├── models.py               # ORM模型（Hotel、Review等6张表）
│   └── init_db.sql             # PostgreSQL初始化脚本
│
├── utils/                       # 工具模块
│   ├── __init__.py
│   ├── logger.py               # Loguru日志配置
│   ├── cleaner.py              # 数据清洗工具
│   └── validator.py            # Pydantic验证模型
│
├── scheduler/                   # 任务调度模块
│   ├── __init__.py
│   └── task_scheduler.py       # 任务管理器
│
├── tests/                       # 测试模块
│   ├── __init__.py
│   └── test_crawler.py         # 单元测试
│
├── logs/                        # 日志目录
├── data/                        # 数据目录
│   ├── raw/
│   ├── processed/
│   └── exports/
│
├── .env.example                 # 环境变量示例
├── .gitignore                   # Git忽略配置
├── requirements.txt             # Python依赖
├── main.py                      # 主程序入口（CLI）
└── README.md                    # 项目说明
```

## 技术栈

| 组件 | 技术选型 | 说明 |
|------|---------|------|
| **爬虫框架** | DrissionPage | 基于CDP协议，绕过webdriver检测 |
| **浏览器** | Chrome (Debug模式) | 手动登录后接管 |
| **数据库** | PostgreSQL | 支持pgvector向量检索 |
| **ORM** | SQLAlchemy 2.0 | Python ORM框架 |
| **数据验证** | Pydantic 2.0 | 数据模型验证 |
| **配置管理** | pydantic-settings | 环境变量管理 |
| **日志** | Loguru | 简洁的日志库 |
| **进度条** | tqdm | 命令行进度显示 |

## 快速开始

### 环境准备

```bash
# 1. 进入项目目录
cd HotelReviewCrawing

# 2. 创建虚拟环境
python -m venv venv
venv\Scripts\activate  # Windows

# 3. 安装依赖
pip install -r requirements.txt

# 4. 配置环境变量
cp .env.example .env
# 编辑.env填入数据库密码

# 5. 初始化数据库
psql -d hotel_reviews -f database/init_db.sql

# 6. 启动Chrome调试模式
chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\selenium\automation_profile"

# 7. 手动登录飞猪
# 在Chrome中访问 https://www.fliggy.com/ 并登录
```

### 执行爬取

```bash
# 测试连接
python main.py --mode test

# 爬取酒店列表（指定功能区）
python main.py --mode hotel_list --region "CBD商务区"

# 爬取酒店列表（所有功能区）
python main.py --mode hotel_list --all

# 爬取评论（指定酒店）
python main.py --mode reviews --hotel-id 10019773

# 爬取评论（所有酒店）
python main.py --mode reviews --all
```

## 数据规模估算

### 存储空间
| 组件 | 大小 |
|------|------|
| 原始数据（含索引） | ~1-2GB |
| 向量索引（后续） | ~200MB |
| 日志文件 | ~10MB |
| **总计** | **~2GB** |

## 重要提示

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
    "output": "推荐广州海航威斯汀酒店。理由：..."
}
```

## 项目交付物

### 代码交付物
- [x] 完整的爬虫代码（crawler/）
- [x] 数据库模块（database/）
- [x] 配置模块（config/）
- [x] 工具模块（utils/）
- [x] 任务调度模块（scheduler/）
- [x] 主程序入口（main.py）
- [x] 单元测试（tests/）

### 文档交付物
- [x] HTML结构分析文档
- [x] 数据库Schema设计文档
- [x] 爬取流程架构文档
- [x] 综合实施方案文档
- [x] 项目架构与文件清单

### 待完成数据交付物
- [ ] PostgreSQL数据库备份文件
- [ ] 酒店基础信息CSV
- [ ] 评论数据CSV
- [ ] 方面情感分析结果CSV
- [ ] 向量索引文件

## 关键技术亮点

### 1. 分层抽样策略
- 按功能区、商圈、价格档次三维度分层
- 确保数据分布均衡，避免"幸存者偏差"

### 2. 瀑布流采集策略
- 优先采集负面评论（差评+中评）
- 其次采集有图评论（高质量证据）
- 最后补充最新评论（时效性）

### 3. 反爬虫技术
- DrissionPage基于CDP协议，绕过webdriver检测
- 手动登录后接管浏览器，继承登录状态
- 智能延迟策略，支持人工介入处理验证码

### 4. 数据质量保证
- Pydantic数据验证
- 多层次去重机制
- 完整的数据清洗流程

---

**最后更新**: 2026-01-25
**版本**: v1.0
