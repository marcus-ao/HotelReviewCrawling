-- 飞猪酒店评论爬虫数据库初始化脚本

DROP TABLE IF EXISTS crawl_logs;
DROP TABLE IF EXISTS crawl_tasks;
DROP TABLE IF EXISTS review_images;
DROP TABLE IF EXISTS review_replies;

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

CREATE INDEX IF NOT EXISTS idx_hotel_region_zone ON hotels(region_type, business_zone_code);
CREATE INDEX IF NOT EXISTS idx_hotel_price_level ON hotels(price_level);
CREATE INDEX IF NOT EXISTS idx_hotel_rating ON hotels(rating_score);

COMMENT ON TABLE hotels IS '酒店基础信息表';
COMMENT ON COLUMN hotels.hotel_id IS '飞猪酒店ID(shid)';
COMMENT ON COLUMN hotels.region_type IS '功能区类型';
COMMENT ON COLUMN hotels.price_level IS '价格档次';

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
    room_type VARCHAR(100),
    review_date TIMESTAMP,
    source_pool VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_review_hotel_id ON reviews(hotel_id);
CREATE INDEX IF NOT EXISTS idx_review_hotel_date ON reviews(hotel_id, review_date);
CREATE INDEX IF NOT EXISTS idx_review_source_pool ON reviews(source_pool);
CREATE INDEX IF NOT EXISTS idx_review_overall_score ON reviews(overall_score);

COMMENT ON TABLE reviews IS '评论主表(全部评论)';
COMMENT ON COLUMN reviews.source_pool IS '来源池(negative/positive)';
COMMENT ON COLUMN reviews.tags IS '标签列表(JSON数组)';

CREATE TABLE IF NOT EXISTS reviews_negative (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(100) UNIQUE NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
    hotel_id VARCHAR(50) NOT NULL REFERENCES hotels(hotel_id) ON DELETE CASCADE,
    user_nick VARCHAR(100),
    content TEXT NOT NULL,
    summary VARCHAR(500),
    overall_score FLOAT,
    review_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reviews_negative_hotel_date ON reviews_negative(hotel_id, review_date);
COMMENT ON TABLE reviews_negative IS '负面评论子表';

CREATE TABLE IF NOT EXISTS reviews_positive (
    id SERIAL PRIMARY KEY,
    review_id VARCHAR(100) UNIQUE NOT NULL REFERENCES reviews(review_id) ON DELETE CASCADE,
    hotel_id VARCHAR(50) NOT NULL REFERENCES hotels(hotel_id) ON DELETE CASCADE,
    user_nick VARCHAR(100),
    content TEXT NOT NULL,
    summary VARCHAR(500),
    overall_score FLOAT,
    review_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reviews_positive_hotel_date ON reviews_positive(hotel_id, review_date);
COMMENT ON TABLE reviews_positive IS '正面评论子表';

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
    SUM(CASE WHEN r.source_pool = 'positive' THEN 1 ELSE 0 END) AS positive_count
FROM hotels h
LEFT JOIN reviews r ON h.hotel_id = r.hotel_id
GROUP BY h.hotel_id, h.name, h.region_type, h.business_zone, h.price_level, h.rating_score;

COMMENT ON VIEW v_hotel_review_stats IS '酒店评论统计视图';

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
