# 飞猪酒店评论数据库Schema设计

## 一、数据库选型

### 推荐方案：PostgreSQL
**理由**:
1. 支持 `pgvector` 扩展，可直接存储和检索向量数据
2. 支持JSON字段，灵活存储非结构化数据
3. 支持全文检索
4. 事务支持完善
5. 开源免费，社区活跃

## 二、数据表设计

### 2.1 酒店基础信息表 (`hotel`)

```sql
CREATE TABLE hotel (
    -- 主键
    hotel_id BIGINT PRIMARY KEY,  -- 飞猪酒店ID
    
    -- 基本信息
    hotel_name VARCHAR(200) NOT NULL,  -- 酒店名称
    hotel_name_en VARCHAR(200),  -- 酒店英文名称
    brand VARCHAR(100),  -- 品牌（如：麗枫、全季、维也纳等）
    
    -- 位置信息
    city_code VARCHAR(20) NOT NULL,  -- 城市代码（如：440100）
    city_name VARCHAR(50) NOT NULL,  -- 城市名称（如：广州）
    district VARCHAR(50),  -- 行政区（如：天河区）
    business_zone VARCHAR(100),  -- 商圈（如：珠江新城/五羊新城商圈）
    address TEXT,  -- 详细地址
    latitude DECIMAL(10, 7),  -- 纬度
    longitude DECIMAL(10, 7),  -- 经度
    
    -- 分类信息
    star_level SMALLINT,  -- 星级：0-客栈公寓, 1-经济连锁, 2-二星及以下, 3-三星/舒适, 4-四星/高档, 5-五星/豪华
    hotel_type VARCHAR(50),  -- 酒店类型
    
    -- 价格信息
    base_price DECIMAL(10, 2),  -- 基准价格
    price_min DECIMAL(10, 2),  -- 最低价格
    price_max DECIMAL(10, 2),  -- 最高价格
    price_level VARCHAR(10),  -- 价格档次：R1-R5
    
    -- 评价信息
    rating DECIMAL(3, 1),  -- 综合评分（如：4.7）
    review_count INT DEFAULT 0,  -- 评论总数
    
    -- 设施标签（JSON格式存储）
    facilities JSONB,  -- 如：{"wifi": true, "parking": true, "breakfast": true}
    
    -- 地铁信息
    nearest_subway VARCHAR(100),  -- 最近地铁站
    subway_distance INT,  -- 距离地铁站距离（米）
    
    -- 爬取信息
    crawl_status VARCHAR(20) DEFAULT 'pending',  -- 爬取状态：pending, crawling, completed, failed
    crawl_time TIMESTAMP,  -- 爬取时间
    last_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 最后更新时间
    
    -- 索引
    CONSTRAINT chk_star_level CHECK (star_level BETWEEN 0 AND 5),
    CONSTRAINT chk_rating CHECK (rating BETWEEN 0 AND 5)
);

-- 创建索引
CREATE INDEX idx_hotel_city ON hotel(city_code);
CREATE INDEX idx_hotel_district ON hotel(district);
CREATE INDEX idx_hotel_business_zone ON hotel(business_zone);
CREATE INDEX idx_hotel_star_level ON hotel(star_level);
CREATE INDEX idx_hotel_price_level ON hotel(price_level);
CREATE INDEX idx_hotel_rating ON hotel(rating DESC);
CREATE INDEX idx_hotel_crawl_status ON hotel(crawl_status);
CREATE INDEX idx_hotel_facilities ON hotel USING GIN(facilities);
```

### 2.2 评论主表 (`review`)

```sql
CREATE TABLE review (
    -- 主键
    review_id BIGINT PRIMARY KEY,  -- 评论唯一ID（可以用雪花算法生成或使用飞猪的ID）
    
    -- 关联信息
    hotel_id BIGINT NOT NULL REFERENCES hotel(hotel_id),  -- 关联酒店ID
    
    -- 用户信息
    user_name VARCHAR(100),  -- 用户昵称（脱敏）
    user_avatar TEXT,  -- 用户头像URL
    
    -- 评论内容
    content TEXT NOT NULL,  -- 评论正文
    content_length INT,  -- 评论字数
    comment_tags TEXT,  -- 评论标签（从comment-name提取）
    
    -- 评分信息
    score_clean SMALLINT,  -- 清洁程度评分（1-5星）
    score_service SMALLINT,  -- 服务体验评分（1-5星）
    score_value SMALLINT,  -- 性价比评分（1-5星）
    score_avg DECIMAL(3, 1),  -- 平均评分
    
    -- 情感分类（预留字段，用于后续情感分析）
    sentiment VARCHAR(20),  -- 情感倾向：positive, neutral, negative
    sentiment_score DECIMAL(3, 2),  -- 情感得分（-1到1）
    
    -- 房型信息
    room_type VARCHAR(200),  -- 房型名称
    
    -- 图片信息
    has_images BOOLEAN DEFAULT FALSE,  -- 是否有图片
    image_count SMALLINT DEFAULT 0,  -- 图片数量
    
    -- 时间信息
    create_time TIMESTAMP NOT NULL,  -- 评论发布时间
    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 爬取时间
    
    -- 商家回复
    has_reply BOOLEAN DEFAULT FALSE,  -- 是否有商家回复
    
    -- 数据质量标记
    is_valid BOOLEAN DEFAULT TRUE,  -- 是否有效（用于标记垃圾评论）
    data_source VARCHAR(50) DEFAULT 'fliggy',  -- 数据来源
    
    -- 索引
    CONSTRAINT chk_score_clean CHECK (score_clean BETWEEN 1 AND 5),
    CONSTRAINT chk_score_service CHECK (score_service BETWEEN 1 AND 5),
    CONSTRAINT chk_score_value CHECK (score_value BETWEEN 1 AND 5)
);

-- 创建索引
CREATE INDEX idx_review_hotel_id ON review(hotel_id);
CREATE INDEX idx_review_create_time ON review(create_time DESC);
CREATE INDEX idx_review_score_avg ON review(score_avg);
CREATE INDEX idx_review_has_images ON review(has_images);
CREATE INDEX idx_review_sentiment ON review(sentiment);
CREATE INDEX idx_review_content_length ON review(content_length);
CREATE INDEX idx_review_hotel_time ON review(hotel_id, create_time DESC);
```

### 2.3 评论图片表 (`review_image`)

```sql
CREATE TABLE review_image (
    -- 主键
    image_id BIGSERIAL PRIMARY KEY,
    
    -- 关联信息
    review_id BIGINT NOT NULL REFERENCES review(review_id) ON DELETE CASCADE,
    hotel_id BIGINT NOT NULL REFERENCES hotel(hotel_id),
    
    -- 图片信息
    image_url TEXT NOT NULL,  -- 原始图片URL
    image_url_thumb TEXT,  -- 缩略图URL
    image_order SMALLINT,  -- 图片顺序
    
    -- 图片属性
    image_width INT,  -- 图片宽度
    image_height INT,  -- 图片高度
    image_size INT,  -- 图片大小（字节）
    
    -- 本地存储路径（如果下载到本地）
    local_path TEXT,  -- 本地存储路径
    
    -- 时间信息
    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_review_image_review_id ON review_image(review_id);
CREATE INDEX idx_review_image_hotel_id ON review_image(hotel_id);
```

### 2.4 商家回复表 (`review_reply`)

```sql
CREATE TABLE review_reply (
    -- 主键
    reply_id BIGSERIAL PRIMARY KEY,
    
    -- 关联信息
    review_id BIGINT NOT NULL REFERENCES review(review_id) ON DELETE CASCADE,
    hotel_id BIGINT NOT NULL REFERENCES hotel(hotel_id),
    
    -- 回复内容
    reply_content TEXT NOT NULL,  -- 回复内容
    reply_time TIMESTAMP,  -- 回复时间
    
    -- 回复者信息
    replier_name VARCHAR(100),  -- 回复者名称（通常是酒店名称或客服）
    
    -- 时间信息
    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_review_reply_review_id ON review_reply(review_id);
CREATE INDEX idx_review_reply_hotel_id ON review_reply(hotel_id);
```

### 2.5 方面情感表 (`aspect_opinion`)

**说明**: 此表用于存储方面级情感分析的结果（PyABSA分析后的数据）

```sql
CREATE TABLE aspect_opinion (
    -- 主键
    aspect_id BIGSERIAL PRIMARY KEY,
    
    -- 关联信息
    review_id BIGINT NOT NULL REFERENCES review(review_id) ON DELETE CASCADE,
    hotel_id BIGINT NOT NULL REFERENCES hotel(hotel_id),
    
    -- 方面信息
    aspect_category VARCHAR(50) NOT NULL,  -- 方面类别：location, hygiene, service, facility, price, noise
    aspect_term VARCHAR(100),  -- 方面词（如：地铁站、前台服务、房间卫生）
    
    -- 观点信息
    opinion_term VARCHAR(200),  -- 观点词/观点片段
    opinion_span TEXT,  -- 完整的观点表达
    
    -- 情感信息
    sentiment VARCHAR(20) NOT NULL,  -- 情感极性：positive, neutral, negative
    sentiment_score DECIMAL(3, 2),  -- 情感得分
    confidence DECIMAL(3, 2),  -- 置信度
    
    -- 位置信息（在原文中的位置）
    start_pos INT,  -- 起始位置
    end_pos INT,  -- 结束位置
    
    -- 时间信息
    analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 分析时间
    
    -- 索引
    CONSTRAINT chk_aspect_category CHECK (aspect_category IN ('location', 'hygiene', 'service', 'facility', 'price', 'noise', 'other'))
);

-- 创建索引
CREATE INDEX idx_aspect_review_id ON aspect_opinion(review_id);
CREATE INDEX idx_aspect_hotel_id ON aspect_opinion(hotel_id);
CREATE INDEX idx_aspect_category ON aspect_opinion(aspect_category);
CREATE INDEX idx_aspect_sentiment ON aspect_opinion(sentiment);
CREATE INDEX idx_aspect_hotel_category ON aspect_opinion(hotel_id, aspect_category);
```

### 2.6 向量索引表 (`embedding_index`)

**说明**: 用于存储评论和酒店描述的向量表示，支持RAG检索

```sql
-- 需要先安装pgvector扩展
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE embedding_index (
    -- 主键
    embedding_id BIGSERIAL PRIMARY KEY,
    
    -- 关联信息
    content_type VARCHAR(20) NOT NULL,  -- 内容类型：review, hotel_desc, sentence
    content_id BIGINT NOT NULL,  -- 内容ID（review_id或hotel_id）
    hotel_id BIGINT NOT NULL REFERENCES hotel(hotel_id),
    
    -- 文本内容
    text_content TEXT NOT NULL,  -- 原始文本内容
    text_length INT,  -- 文本长度
    
    -- 向量表示（使用bge-small-zh，维度为512）
    embedding vector(512) NOT NULL,  -- 向量表示
    
    -- 元数据
    metadata JSONB,  -- 额外的元数据（如：方面类别、情感等）
    
    -- 时间信息
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 索引
    CONSTRAINT chk_content_type CHECK (content_type IN ('review', 'hotel_desc', 'sentence'))
);

-- 创建向量索引（使用HNSW算法，适合大规模数据）
CREATE INDEX idx_embedding_vector ON embedding_index USING hnsw (embedding vector_cosine_ops);
CREATE INDEX idx_embedding_hotel_id ON embedding_index(hotel_id);
CREATE INDEX idx_embedding_content_type ON embedding_index(content_type);
CREATE INDEX idx_embedding_metadata ON embedding_index USING GIN(metadata);
```

### 2.7 爬取任务表 (`crawl_task`)

**说明**: 用于管理爬取任务和进度跟踪

```sql
CREATE TABLE crawl_task (
    -- 主键
    task_id BIGSERIAL PRIMARY KEY,
    
    -- 任务信息
    task_type VARCHAR(50) NOT NULL,  -- 任务类型：hotel_list, hotel_detail, review_list
    task_status VARCHAR(20) DEFAULT 'pending',  -- 任务状态：pending, running, completed, failed
    
    -- 目标信息
    target_id BIGINT,  -- 目标ID（酒店ID等）
    target_url TEXT,  -- 目标URL
    
    -- 任务参数（JSON格式）
    task_params JSONB,  -- 如：{"city": "440100", "business_zone": "39584", "price_level": "R3"}
    
    -- 进度信息
    total_count INT DEFAULT 0,  -- 总数
    completed_count INT DEFAULT 0,  -- 已完成数
    failed_count INT DEFAULT 0,  -- 失败数
    progress DECIMAL(5, 2) DEFAULT 0,  -- 进度百分比
    
    -- 时间信息
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    
    -- 错误信息
    error_message TEXT,  -- 错误信息
    retry_count SMALLINT DEFAULT 0,  -- 重试次数
    
    -- 索引
    CONSTRAINT chk_task_type CHECK (task_type IN ('hotel_list', 'hotel_detail', 'review_list')),
    CONSTRAINT chk_task_status CHECK (task_status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))
);

-- 创建索引
CREATE INDEX idx_crawl_task_status ON crawl_task(task_status);
CREATE INDEX idx_crawl_task_type ON crawl_task(task_type);
CREATE INDEX idx_crawl_task_create_time ON crawl_task(create_time DESC);
```

### 2.8 爬取日志表 (`crawl_log`)

```sql
CREATE TABLE crawl_log (
    -- 主键
    log_id BIGSERIAL PRIMARY KEY,
    
    -- 关联信息
    task_id BIGINT REFERENCES crawl_task(task_id),
    
    -- 日志信息
    log_level VARCHAR(20) NOT NULL,  -- 日志级别：DEBUG, INFO, WARNING, ERROR
    log_message TEXT NOT NULL,  -- 日志消息
    log_detail JSONB,  -- 详细信息（JSON格式）
    
    -- 时间信息
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 索引
    CONSTRAINT chk_log_level CHECK (log_level IN ('DEBUG', 'INFO', 'WARNING', 'ERROR'))
);

-- 创建索引
CREATE INDEX idx_crawl_log_task_id ON crawl_log(task_id);
CREATE INDEX idx_crawl_log_level ON crawl_log(log_level);
CREATE INDEX idx_crawl_log_time ON crawl_log(log_time DESC);
```

## 三、数据统计视图

### 3.1 酒店方面情感统计视图

```sql
CREATE VIEW hotel_aspect_stats AS
SELECT 
    h.hotel_id,
    h.hotel_name,
    ao.aspect_category,
    COUNT(*) as mention_count,
    SUM(CASE WHEN ao.sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN ao.sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
    SUM(CASE WHEN ao.sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
    AVG(ao.sentiment_score) as avg_sentiment_score,
    ROUND(SUM(CASE WHEN ao.sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as positive_rate
FROM hotel h
JOIN aspect_opinion ao ON h.hotel_id = ao.hotel_id
GROUP BY h.hotel_id, h.hotel_name, ao.aspect_category;
```

### 3.2 酒店评论统计视图

```sql
CREATE VIEW hotel_review_stats AS
SELECT 
    h.hotel_id,
    h.hotel_name,
    COUNT(r.review_id) as total_reviews,
    AVG(r.score_avg) as avg_score,
    AVG(r.score_clean) as avg_clean_score,
    AVG(r.score_service) as avg_service_score,
    AVG(r.score_value) as avg_value_score,
    SUM(CASE WHEN r.has_images THEN 1 ELSE 0 END) as reviews_with_images,
    SUM(CASE WHEN r.has_reply THEN 1 ELSE 0 END) as reviews_with_reply,
    SUM(CASE WHEN r.sentiment = 'positive' THEN 1 ELSE 0 END) as positive_reviews,
    SUM(CASE WHEN r.sentiment = 'negative' THEN 1 ELSE 0 END) as negative_reviews,
    MAX(r.create_time) as latest_review_time
FROM hotel h
LEFT JOIN review r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.hotel_name;
```

## 四、数据规模估算

根据爬取计划：
- **酒店数量**: 250-270家
- **每家酒店评论数**: 最多300条
- **总评论数**: 约75,000-81,000条

### 存储空间估算

| 表名 | 单条记录大小 | 记录数 | 总大小估算 |
|------|------------|--------|-----------|
| hotel | ~2KB | 270 | ~540KB |
| review | ~1KB | 80,000 | ~80MB |
| review_image | ~500B | 40,000 | ~20MB |
| review_reply | ~500B | 20,000 | ~10MB |
| aspect_opinion | ~300B | 400,000 | ~120MB |
| embedding_index | ~2.5KB | 80,000 | ~200MB |
| crawl_task | ~1KB | 1,000 | ~1MB |
| crawl_log | ~500B | 10,000 | ~5MB |

**总计**: 约 436MB（不含索引）
**含索引**: 约 1-2GB

## 五、数据库优化建议

### 5.1 分区策略
对于大表可以考虑分区：
```sql
-- 按时间分区review表（如果数据量持续增长）
CREATE TABLE review_2026_01 PARTITION OF review
FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

### 5.2 定期维护
```sql
-- 定期VACUUM和ANALYZE
VACUUM ANALYZE hotel;
VACUUM ANALYZE review;
VACUUM ANALYZE aspect_opinion;
VACUUM ANALYZE embedding_index;
```

### 5.3 备份策略
- 每日增量备份
- 每周全量备份
- 重要数据实时同步到备份库

## 六、数据导出格式

### 6.1 用于PyABSA分析的数据格式
```json
{
  "review_id": 123456,
  "hotel_id": 3472,
  "content": "位置的话在东站，去哪都比较方便...",
  "sentences": [
    "位置的话在东站，去哪都比较方便。",
    "卫生嘛一如既往的好安心。"
  ]
}
```

### 6.2 用于LLM微调的数据格式
```json
{
  "instruction": "根据以下酒店评论，推荐适合的酒店",
  "input": "我需要在广州找一家位置方便、卫生干净的酒店，预算600元左右",
  "output": "推荐广工大酒店。理由：1. 位置优越，位于东站附近，交通便利；2. 卫生状况优秀，多位住客评价'万豪一如既往的好'；3. 价格适中，符合您的预算。"
}
```

## 七、下一步工作

1. ✅ 完成数据库Schema设计
2. ⏭️ 编写数据库初始化脚本
3. ⏭️ 设计数据提取和存储的ORM模型
4. ⏭️ 实现数据清洗和验证逻辑
