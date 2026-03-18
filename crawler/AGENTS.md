# CRAWLER MODULE — 爬虫核心模块

项目复杂度热点。3个文件共2234行，承载全部页面交互和数据提取逻辑。

## OVERVIEW

```
crawler/
├── anti_crawler.py        # 320行 — 浏览器接管/自动化验证码/导航/滚动
├── hotel_list_crawler.py    # 1304行 — 酒店列表爬取 (最大文件, 建议拆分)
└── review_crawler.py        # 评论双池采集：negative自动 / positive人工辅助
```

## ARCHITECTURE

```
main.py
  ├─→ HotelListCrawler.crawl_all_regions()   # 阶段一: 酒店列表
  │     └─→ crawl_region() × 6功能区
  │           └─→ _crawl_business_zone_elastic() × 3商圈  ← 弹性补位核心
  │            └─→ crawl_by_zone_and_price() × 4价格档
  │             ├─→ build_search_url()           ← 构建飞猪搜索URL
  │               ├─→ anti_crawler.navigate_to()   ← CDP导航
  │              ├─→ extract_hotels_from_page()   ← 多页提取+翻页
  │               │     ├─→ _extract_hotels_from_query_data()  ← JSON通道
  │           │     ├─→ _extract_hotel_from_html()         ← HTML回退
  │                 │     └─→ _go_to_next_page()              ← 翻页
  │            └─→ _save_hotels()               ← Pydantic校验→ORM入库
  │
  ├─→ HotelListCrawler.enrich_hotel_details()  # 阶段1.5: 详情补充
  │     └─→ fetch_hotel_details() × N酒店      ← 正则提取详情页字段
  │
  └─→ ReviewCrawler.crawl_hotel_reviews()      # 阶段二: 评论采集
        └─→ waterfall_crawl()          ← 双池入口
              ├─→ _crawl_pool("negative", [BAD], max=100)
              └─→ _crawl_positive_pool_manual(...)
               ├─→ filter_reviews()              ← 点击筛选tab
        ├─→ extract_reviews_from_page()    ← DOM解析评论
                    │     └─→ _parse_review_element()  ← 单条评论提取
                    │           └─→ _parse_scores()    ← CSS width→5分制
              ├─→ load_more_reviews()      ← 评论翻页
                  └─→ save_reviews()                  ← 入库(Review+Image+Reply)
```

## anti_crawler.py — 反爬层

所有页面交互的底层。其他爬虫类不直接操作浏览器，统一通过此类。

| 方法 | 行 | 作用 | 关键细节 |
|------|-----|------|-------|
| `init_browser` | 26 | CDP连接Chrome | 地址来自 `settings.chrome_address` (127.0.0.1:9222) |
| `random_delay` | 54 | 随机延迟 | `random.uniform(min, max)` → `time.sleep` |
| `check_captcha` | 67 | 检测验证码 | 5个CSS选择器: `#nc_1_n1z`, `.nc-container` 等 |
| `handle_captcha` | 146 | 验证码处理 | 自动滑块→有界重试→冷却停止（无人工回退） |
| `_is_verification_expired` | 116 | 验证页过期检测 | 检测验证页面是否超时失效 |
| `_refresh_verification_page` | 139 | 刷新验证页 | 刷新过期的验证页面，最多2次 |
| `_auto_slide_captcha` | 231 | 自动滑块 | S曲线轨迹+抖动+超冲，模拟人类行为 |
| `navigate_to` | 204 | 导航+验证码检查 | 每次导航后自动check_captcha |
| `scroll_to_bottom` | 250 | 滚动到底 | JS获取scrollHeight判断是否到底 |
| `safe_navigate` | 276 | 带重试导航 | `@retry(stop=3, wait=exponential)` |

## hotel_list_crawler.py — 酒店列表爬取 (1304行)

### 数据提取双通道

**通道1 (JSON优先)**: `_extract_hotels_from_query_data` (line 352)
- 从HTML中提取 `__QUERY_RESULT_DATA__` JS变量的JSON
- `_extract_json_blob`: 手写大括号匹配解析器（非正则），处理嵌套JSON
- 字段映射: `shid`→hotel_id, `rateScore`→rating_score, `rateNum`→review_count
- 价格提取三级回退: `priceDesp` → `priceWithoutTax.amountCNY` → `price`
- `price > 1000` 时除以100（飞猪价格有时以分为单位）

**通道2 (HTML回退)**: `_extract_hotel_from_html` (line 496)
- 正则匹配 `data-shid="{id}"` 的 `<div class="list-row">` 或 `<div class="hotel-marker">`
- 仅能提取 hotel_id + name + 经纬度，无评分/价格/评论数
- **已知BUG (line 225)**: 回退分支中 `hotel_data` 引用了上一次循环的变量

### 翻页双通道

**通道1 (URL参数)**: `_go_to_next_page` (line 588)
- 从 `__QUERY_RESULT_DATA__.query` 获取 currentPage/totalPage/pageSize
- 尝试4种URL参数名: `currentPage`, `pageNo`, `page`, `pageNum`
- 导航后验证 currentPage 确实变化

**通道2 (点击回退)**: `_go_to_next_page_by_click` (line 648)
- 5个备用选择器: `a.page-next`, `.pagination .next` 等
- 检查 disabled 状态避免无效点击

### 弹性补位算法

`_crawl_business_zone_elastic` (line 966):
```
正向遍历: 经济→舒适→高档→奢华
  每档 target = base_top_n + carry_over(上一档缺口)
  实际不足 → carry_over传递给下一档

如果正向遍历后仍有缺口:
  反向遍历: 高档→舒适→经济 (跳过奢华)
  用低价档补足剩余
```

### 排序策略

`extract_hotels_from_page` 内的 `_sort_key` (line 150):
- 默认: 评论数降序 → 价格升序 → 评分降序
- `price_desc`: 评论数降序 → 价格降序 → 评分降序
- `score`: 评论数降序 → 评分降序 → 价格升序
- 评论数始终是第一排序键（保证数据质量）

### 入库逻辑

`_save_hotels` (line 1103):
- 批量查询已存在hotel_id，构建 `{hotel_id: {(zone_code, price_level): Hotel}}` 字典
- 同商圈+同价格档 → 跳过（完全重复）
- 同hotel_id不同商圈 → 更新基本信息字段，保留分层信息
- 新酒店 → `HotelModel` 校验后 `session.add`
- 二次评论数过滤: `review_count <= min_reviews_threshold` 再次跳过

## review_crawler.py — 评论双池采集

### 双池策略

`waterfall_crawl`
- negative池: `filter_types=[FILTER_BAD(2)]`, 在线分页主采
- positive池: `_crawl_positive_pool_manual()`, 人工翻页 + HTML提取

### 评论筛选

`filter_reviews` (line 116): 每种筛选类型3个备用选择器
- 好评: `#review-t-2`, `input[value="1"]`, `.review-filter-good`
- 中评: `#review-t-4`, `input[value="2"]`, `.review-filter-medium`
- 差评: `#review-t-5`, `input[value="3"]`, `.review-filter-bad`
- input元素 → 找对应label点击（飞猪用label包裹radio）

### 单条评论解析

`_parse_review_element` (line 273), CSS选择器:
| 数据 | 选择器 | 提取方式 |
|------|-----|---------|
| 昵称 | `.tb-r-nick a` | `attr('title')` or `.text` |
| 内容 | `.tb-r-cnt` | `.text` → `clean_text()` |
| 摘要 | `.comment-name` | `.text` → `clean_text()` |
| 日期 | `.tb-r-date` | `.text` → `parse_date()` |
| 图片 | `.tb-r-photos img` | `attr('data-val')` |
| 回复 | `.tb-r-seller` | `.text` → `clean_text()` |
| 评分 | `.starscore li em` | `attr('style')` → `parse_star_score()` |

评分顺序固定: `[clean, location, service, value]`

### 去重机制

- `_generate_review_id`: MD5(hotel_id + content + user_nick)[:16], 前缀hotel_id
- 内存去重: `self.crawled_review_ids` set, 每次 `waterfall_crawl` 开始时清空
- 数据库去重: `save_reviews` 中 `session.query(Review).filter_by(review_id=...)` 检查

### 入库事务

`save_reviews` (line 566): 单个session_scope内
- `review_data.pop('image_urls')` 分离图片数据
- `review_data.pop('reply_content')` 分离回复数据
- `ReviewModel(**review_data)` 校验
- `session.add(Review)` → `session.flush()` 获取ID
- 循环 `session.add(ReviewImage)` + `session.add(ReviewReply)`
- 单条失败不影响其他（try/except continue）

## KNOWN BUGS & RISKS

1. **hotel_list_crawler.py:225** — HTML回退分支 `hotel_data` 变量泄漏: `_price_in_range(hotel_data.get('base_price'), price_range)` 中的 `hotel_data` 是上一次循环的值，应为当前 `hotel_id` 对应的数据
2. **正向池依赖人工翻页** — CLI/终端提示与页面变化校验是核心流程，修改时需优先保持幂等和去重安全
3. **CSS选择器脆弱** — 飞猪改版会导致 `.tb-r-comment`, `.tb-r-cnt`, `.starscore` 等批量失效
4. **hotel_list_crawler.py 过大** — 1304行单文件，建议拆分: 提取逻辑/翻页逻辑/入库逻辑/弹性补位

## 验证码自动化

### 配置参数 (config/settings.py)
- `CAPTCHA_MAX_RETRIES = 3`: 最大重试次数
- `CAPTCHA_TIMEOUT = 120`: 验证超时时间（秒）
- `CAPTCHA_COOLDOWN = 180`: 达到重试上限后的冷却时间（秒）
- `CAPTCHA_REFRESH_RETRY_LIMIT = 2`: 验证页刷新次数上限

### 异常层次结构 (crawler/exceptions.py)
- `CrawlerException`: 基础异常类
  - `CaptchaException`: 可重试的验证码失败 (should_retry() = True)
  - `CaptchaCooldownException`: 终止冷却 (should_retry() = False)

### 自动化流程
1. 导航到页面 → check_captcha()
2. 检测到验证码 → handle_captcha()
3. 尝试 auto_slide_captcha() 使用人类化轨迹
4. 验证页过期 → refresh_verification_page()
5. 重试最多 CAPTCHA_MAX_RETRIES 次
6. 达到最大重试次数 → 抛出 CaptchaCooldownException
7. CLI/调度器捕获冷却异常 → 停止任务，记录冷却消息

### 恢复行为
- 酒店列表: 每页增量保存，从下一页恢复
- 评论采集: 每池检查点保存，已完成的池在中断时保留
- 任务状态保留以便冷却期后重试

### 测试覆盖 (tests/)
- `test_anti_crawler_captcha.py`: 7个验证码引擎测试
- `test_captcha_recovery.py`: 6个恢复/中断测试
- 全部32个测试通过

## WHERE TO LOOK (CRAWLER-SPECIFIC)

| Task | Method | Line |
|------|--------|----|
| 修改JSON字段映射 | `_extract_hotels_from_query_data` | 352 |
| 修改HTML回退提取 | `_extract_hotel_from_html` | 496 |
| 修改翻页逻辑 | `_go_to_next_page` | 588 |
| 修改弹性补位算法 | `_crawl_business_zone_elastic` | 966 |
| 修改评论CSS选择器 | `_parse_review_element` | 273 |
| 修改负向池配额 | `waterfall_crawl` | 见双池入口 |
| 修改验证码选择器 | `check_captcha` | 67 |
| 修改滑块模拟行为 | `_auto_slide_captcha` | 129 |
| 修改排序策略 | `_sort_key` in `extract_hotels_from_page` | 150 |
| 修改详情页提取 | `fetch_hotel_details` | 689 |
