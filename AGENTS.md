# PROJECT KNOWLEDGE BASE

## OVERVIEW

飞猪(Fliggy)酒店评论爬虫。Python 3.10+ / DrissionPage CDP / PostgreSQL / SQLAlchemy 2 / Pydantic 2。
毕业课题数据采集模块：广州270家酒店 × 300条评论 ≈ 81K条，用于RAG/LLM微调/ABSA。

## STRUCTURE

```
HotelReviewCrawling/
├── main.py                 # CLI入口 (argparse: test/hotel_list/enrich_details/reviews)
├── config/                 # 配置层
│   ├── settings.py         # pydantic-settings, 读.env, 全局单例 `settings`
│   └── regions.py          # 6功能区×3商圈×4价格档 = 72采集单元, PRICE_RANGES
├── crawler/                # 核心爬虫 (项目最复杂模块, 见 crawler/AGENTS.md)
│   ├── anti_crawler.py     # DrissionPage接管Chrome, 滑块验证码, 随机延迟
│   ├── hotel_list_crawler.py  # 酒店列表爬取 (1304行, 分层抽样+弹性补位+翻页)
│   └── review_crawler.py   # 评论瀑布流采集 (三池策略: negative→evidence→latest)
├── database/               # 数据层
│   ├── models.py       # 6张ORM表: Hotel/Review/ReviewImage/ReviewReply/CrawlTask/CrawlLog
│   └── connection.py       # 单例引擎, QueuePool(5+10), session_scope()上下文管理器
├── utils/                  # 工具层
│   ├── cleaner.py          # clean_text/extract_tags/parse_star_score/parse_date/extract_price
│   ├── validator.py        # Pydantic模型: HotelModel/ReviewModel/CrawlTaskModel
│   └── logger.py         # Loguru配置, 按天轮转, 错误日志分离
├── scheduler/
│   └── task_scheduler.py   # 任务状态机: pending→in_progress→completed/failed/skipped
├── tests/
│   └── test_crawler.py     # 仅覆盖工具函数+配置, 无爬虫集成测试
├── plans/                  # 设计文档 (00-05, 中文)
├── data/                # raw/processed/exports (gitignored)
└── logs/                   # 自动生成, 按天轮转
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| 修改爬取目标城市/商圈 | `config/regions.py` | GUANGZHOU_REGIONS dict, zone codes来自飞猪URL |
| 调整延迟/重试/阈值 | `.env` + `config/settings.py` | MIN_DELAY, MAX_RETRIES, MIN_REVIEWS_THRESHOLD |
| 修改酒店列表提取逻辑 | `crawler/hotel_list_crawler.py` | `_extract_hotels_from_query_data` (JSON优先) / `_extract_hotel_from_html` (回退) |
| 修改评论提取逻辑 | `crawler/review_crawler.py` | `_parse_review_element` + CSS选择器 |
| 修改数据库Schema | `database/models.py` | SQLAlchemy ORM, 改后需 `init_db()` 或 alembic |
| 添加数据验证字段 | `utils/validator.py` | Pydantic BaseModel, field_validator |
| 调整瀑布流池配额 | `crawler/review_crawler.py:434` | `waterfall_crawl` 方法内硬编码: negative=100, evidence=150 |
| 调试反爬/验证码 | `crawler/anti_crawler.py` | `check_captcha` 选择器列表, `_auto_slide_captcha` |

## CODE MAP

| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `Settings` | class | config/settings.py | pydantic-settings全局配置, `.env`绑定 |
| `settings` | instance | config/settings.py:63 | 全局单例, 所有模块import使用 |
| `GUANGZHOU_REGIONS` | dict | config/regions.py:16 | 6功能区配置, 含商圈code和价格档 |
| `PRICE_RANGES` | list | config/regions.py:8 | 4价格档: 经济/舒适/高档/奢华, 含top_n |
| `AntiCrawler` | class | crawler/anti_crawler.py | CDP接管Chrome, 延迟/验证码/导航/滚动 |
| `HotelListCrawler` | class | crawler/hotel_list_crawler.py | 酒店列表爬取, 分层抽样+弹性补位 |
| `ReviewCrawler` | class | crawler/review_crawler.py | 评论瀑布流采集, 三池策略 |
| `Hotel` | ORM | database/models.py:32 | 酒店表, hotel_id unique |
| `Review` | ORM | database/models.py:71 | 评论表, FK→hotels.hotel_id |
| `ReviewImage` | ORM | database/models.py:125 | 评论图片, FK→reviews.review_id |
| `ReviewReply` | ORM | database/models.py:143 | 商家回复, 1:1→Review |
| `CrawlTask` | ORM | database/models.py:160 | 任务表, 状态机 |
| `session_scope` | contextmanager | database/connection.py:52 | 自动commit/rollback/close |
| `TaskScheduler` | class | scheduler/task_scheduler.py | 任务创建/执行/状态跟踪/统计 |
| `HotelModel` | Pydantic | utils/validator.py:7 | 酒店数据校验 |
| `ReviewModel` | Pydantic | utils/validator.py:40 | 评论数据校验, calculate_overall_score |

## CONVENTIONS

- 所有模块通过 `__init__.py` 导出公共API, 外部只 `from crawler import HotelListCrawler`
- 配置统一走 `settings` 单例, 不直接读 `os.environ`
- 数据库操作统一用 `session_scope()` 上下文管理器
- 日志统一用 `get_logger("module_name")` 获取带模块标识的logger
- 数据入库前必须经过 Pydantic 模型校验
- 酒店ID字段名统一为 `hotel_id` (飞猪原始字段 `shid`)
- 评论ID通过 MD5(hotel_id + content + user_nick) 生成, 前缀 `{hotel_id}_`
- 价格档次由 `_map_price_level(base_price)` 动态映射, 不硬编码

## ANTI-PATTERNS (THIS PROJECT)

- **禁止** 直接 `import settings` 后修改属性 — 它是 pydantic frozen
- **禁止** 在爬虫方法中直接 `time.sleep` — 必须走 `anti_crawler.random_delay`
- **禁止** 跳过 Pydantic 校验直接构造 ORM 对象
- **禁止** 在 `session_scope` 外执行数据库写操作
- **注意** `hotel_list_crawler.py:225` 回退分支有bug: `hotel_data` 变量引用了上一次循环的值

## UNIQUE STYLES

- 飞猪页面数据提取采用"JSON优先 + HTML正则回退"双通道策略
- 酒店列表翻页采用"URL参数优先 + 点击按钮回退"双通道策略
- 弹性补位: 价格档不足时正向顺延→反向回补, 保证商圈总量
- 评论瀑布流: 差评优先→有图优先→最新补全, 为下游ABSA/RAG定制

## COMMANDS

```bash
# 激活虚拟环境
venv312\Scripts\activate          # Windows

# 测试配置
python main.py --mode test

# 爬取酒店列表
python main.py --mode hotel_list --region "CBD商务区"
python main.py --mode hotel_list --all

# 补充酒店详情
python main.py --mode enrich_details

# 爬取评论
python main.py --mode reviews --hotel-id 10019773
python main.py --mode reviews --all

# 运行测试
pytest tests/ -v

# 前置: 启动Chrome调试模式
chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\selenium\automation_profile"
```

## NOTES

- Chrome必须先手动启动调试模式并登录飞猪, 程序通过CDP接管已有session
- `min_reviews_threshold` 默认200, .env.example写的50 — 以settings.py为准
- `hotel_list_crawler.py` 1304行, 是项目复杂度热点, 建议拆分
- 无CI/CD, 无Docker, 无代理IP支持, 无alembic迁移
- `plans/` 目录含完整中文设计文档(00-05), 修改爬取策略前务必先读
- 测试仅覆盖工具函数, 核心爬虫逻辑无自动化测试
- 飞猪页面改版会导致CSS选择器和正则批量失效 — 这是最大维护风险
