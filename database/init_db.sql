-- 飞猪酒店评论爬虫数据库初始化脚本
-- 数据库: PostgreSQL
-- 用途: 创建酒店评论知识库所需的表结构

-- 创建数据库（如果不存在）
-- CREATE DATABASE hotel_reviews WITH ENCODING 'UTF8';

-- 连接到数据库
-- \c hotel_reviews

-- 创建pgvector扩展（用于后续向量检索）
-- CREATE EXTENSION IF NOT EXISTS vector;

-- =====================================================
-- 1. 酒店基础信息表
-- =====================================================
CREATE TABLE IF NOT EXISTS hotels (
    id SERIAL PRIMARY KEY,
    hotel_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    address VARCHAR(500),
    city_code VARCHAR(20) DEFAULT '440100',
    latitude FLOAT,
    longitude FLOAT,
    star_level VARCHAR(50),
    rating_score FLOAT,
    review_count INTEGER DEFAULT 0,
    base_price INTEGER,
    price_range VARCHAR(50),
    region_type VARCHAR(50),
    business_zone VARCHAR(100),
    business_zone_code VARCHAR(20),
    price_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_hotel_region_zone ON hotels(region_type, business_zone_code);
CREATE INDEX IF NOT EXISTS idx_hotel_price_level ON hotels(price_level);
CREATE INDEX IF NOT EXISTS idx_hotel_rating ON hotels(rating_score);

COMMENT ON TABLE hotels IS '酒店基础信息表';
COMMENT ON COLUMN hotels.hotel_id IS '飞猪酒店ID(shid)';
COMMENT ON COLUMN hotels.region_type IS '功能区类型(CBD商务区/老城文化区等)';
COMMENT ON COLUMN hotels.price_level IS '价格档次(经济型/舒适型/高档型/奢华型)';

-- =====================================================
-- 2. 评论主表
-- =====================================================
CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(100) UNIQUE,
    hotel_id VARCHAR(50) NOT NULL REFERENCES hotels(hotel_id) ON DELETE CASCADE,
    user_nick VARCHAR(100),
    content TEXT NOT NULL,
    summary VARCHAR(500),
    score_clean FLOAT,
    score_location FLOAT,
    score_service FLOAT,
    score_value FLOAT,
    overall_score FLOAT,
    tags JSONB DEFAULT '[]',
    has_images BOOLEAN DEFAULT FALSE,
    room_type VARCHAR(100),
    review_date TIMESTAMP,
    source_pool VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_review_hotel_id ON reviews(hotel_id);
CREATE INDEX IF NOT EXISTS idx_review_hotel_date ON reviews(hotel_id, review_date);
CREATE INDEX IF NOT EXISTS idx_review_source_pool ON reviews(source_pool);
CREATE INDEX IF NOT EXISTS idx_review_overall_score ON reviews(overall_score);

COMMENT ON TABLE reviews IS '评论主表';
COMMENT ON COLUMN reviews.source_pool IS '来源池(negative/evidence/latest)';
COMMENT ON COLUMN reviews.tags IS '标签列表(JSON数组)';

-- =====================================================
-- 3. 评论图片表
-- =====================================================
CREATE TABLE IF NOT EXISTS review_images (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(100) NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
    image_url VARCHAR(500) NOT NULL,
    thumbnail_url VARCHAR(500),
    sort_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_review_images_review_id ON review_images(review_id);

COMMENT ON TABLE review_images IS '评论图片表';

-- =====================================================
-- 4. 商家回复表
-- =====================================================
CREATE TABLE IF NOT EXISTS review_replies (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(100) UNIQUE NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    reply_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE review_replies IS '商家回复表';

-- =====================================================
-- 5. 爬取任务表
-- =====================================================
CREATE TABLE IF NOT EXISTS crawl_tasks (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100) UNIQUE,
    task_type VARCHAR(50) NOT NULL,
    region_type VARCHAR(50),
    business_zone_code VARCHAR(20),
    hotel_id VARCHAR(50),
    price_level VARCHAR(20),
    status VARCHAR(20) DEFAULT 'pending',
    priority INTEGER DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,
    items_crawled INTEGER DEFAULT 0,
    items_total INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_task_status_priority ON crawl_tasks(status, priority);
CREATE INDEX IF NOT EXISTS idx_task_type ON crawl_tasks(task_type);

COMMENT ON TABLE crawl_tasks IS '爬取任务表';
COMMENT ON COLUMN crawl_tasks.task_type IS '任务类型(hotel_list/review)';
COMMENT ON COLUMN crawl_tasks.status IS '任务状态(pending/in_progress/completed/failed/skipped)';

-- =====================================================
-- 6. 爬取日志表
-- =====================================================
CREATE TABLE IF NOT EXISTS crawl_logs (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100),
    level VARCHAR(20),
    message TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crawl_logs_task_id ON crawl_logs(task_id);
CREATE INDEX IF NOT EXISTS idx_crawl_logs_created_at ON crawl_logs(created_at);

COMMENT ON TABLE crawl_logs IS '爬取日志表';

-- =====================================================
-- 7. 视图：酒店评论统计
-- =====================================================
CREATE OR REPLACE VIEW v_hotel_review_stats AS
SELECT
    h.hotel_id,
    h.name,
    h.region_type,
    h.business_zone,
    h.price_level,
    h.rating_score,
    COUNT(r.id) AS actual_review_count,
    AVG(r.overall_score) AS avg_overall_score,
    AVG(r.score_clean) AS avg_clean_score,
    AVG(r.score_location) AS avg_location_score,
    AVG(r.score_service) AS avg_service_score,
    AVG(r.score_value) AS avg_value_score,
    SUM(CASE WHEN r.source_pool = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN r.source_pool = 'evidence' THEN 1 ELSE 0 END) AS evidence_count,
    SUM(CASE WHEN r.source_pool = 'latest' THEN 1 ELSE 0 END) AS latest_count,
    SUM(CASE WHEN r.has_images THEN 1 ELSE 0 END) AS with_images_count
FROM hotels h
LEFT JOIN reviews r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.name, h.region_type, h.business_zone, h.price_level, h.rating_score;

COMMENT ON VIEW v_hotel_review_stats IS '酒店评论统计视图';

-- =====================================================
-- 8. 视图：功能区采集进度
-- =====================================================
CREATE OR REPLACE VIEW v_region_crawl_progress AS
SELECT
    region_type,
    COUNT(DISTINCT hotel_id) AS hotel_count,
    SUM(review_count) AS total_reviews,
    AVG(rating_score) AS avg_rating
FROM hotels
WHERE region_type IS NOT NULL
GROUP BY region_type;

COMMENT ON VIEW v_region_crawl_progress IS '功能区采集进度视图';

-- =====================================================
-- 触发器：自动更新updated_at
-- =====================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_hotels_updated_at ON hotels;
CREATE TRIGGER update_hotels_updated_at
    BEFORE UPDATE ON hotels
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_crawl_tasks_updated_at ON crawl_tasks;
CREATE TRIGGER update_crawl_tasks_updated_at
    BEFORE UPDATE ON crawl_tasks
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
