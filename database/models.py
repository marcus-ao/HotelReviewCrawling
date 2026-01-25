"""ORM模型定义模块"""
from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    Column, String, Integer, Float, Text, DateTime, Boolean,
    ForeignKey, Index, JSON, Enum as SQLEnum
)
from sqlalchemy.orm import declarative_base, relationship
import enum


Base = declarative_base()


class TaskStatus(enum.Enum):
    """任务状态枚举"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class SourcePool(enum.Enum):
    """评论来源池枚举"""
    NEGATIVE = "negative"      # 负面警示池
    EVIDENCE = "evidence"      # 高质量证据池
    LATEST = "latest"          # 时效性补全池


class Hotel(Base):
    """酒店基础信息表"""
    __tablename__ = "hotels"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hotel_id = Column(String(50), unique=True, nullable=False, index=True, comment="飞猪酒店ID(shid)")
    name = Column(String(200), nullable=False, comment="酒店名称")
    address = Column(String(500), comment="酒店地址")
    city_code = Column(String(20), default="440100", comment="城市代码")
    latitude = Column(Float, comment="纬度")
    longitude = Column(Float, comment="经度")
    star_level = Column(String(50), comment="星级/档次")
    rating_score = Column(Float, comment="评分")
    review_count = Column(Integer, default=0, comment="评论数量")
    base_price = Column(Integer, comment="起步价格")
    price_range = Column(String(50), comment="价格区间")

    # 分层抽样相关字段
    region_type = Column(String(50), comment="功能区类型")
    business_zone = Column(String(100), comment="商圈名称")
    business_zone_code = Column(String(20), comment="商圈代码")
    price_level = Column(String(20), comment="价格档次")

    # 元数据
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    # 关系
    reviews = relationship("Review", back_populates="hotel", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_hotel_region_zone", "region_type", "business_zone_code"),
        Index("idx_hotel_price_level", "price_level"),
    )

    def __repr__(self):
        return f"<Hotel(id={self.id}, hotel_id={self.hotel_id}, name={self.name})>"


class Review(Base):
    """评论主表"""
    __tablename__ = "reviews"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(String(100), unique=True, index=True, comment="评论ID")
    hotel_id = Column(String(50), ForeignKey("hotels.hotel_id"), nullable=False, index=True, comment="酒店ID")

    # 用户信息
    user_nick = Column(String(100), comment="用户昵称")

    # 评论内容
    content = Column(Text, nullable=False, comment="评论内容")
    summary = Column(String(500), comment="评论摘要")

    # 评分（1-5分）
    score_clean = Column(Float, comment="清洁程度评分")
    score_location = Column(Float, comment="地理位置评分")
    score_service = Column(Float, comment="服务体验评分")
    score_value = Column(Float, comment="性价比评分")
    overall_score = Column(Float, comment="综合评分")

    # 标签
    tags = Column(JSON, default=list, comment="标签列表")

    # 图片
    has_images = Column(Boolean, default=False, comment="是否有图片")

    # 房型
    room_type = Column(String(100), comment="房型")

    # 日期
    review_date = Column(DateTime, comment="评论日期")

    # 来源池
    source_pool = Column(String(20), comment="来源池(negative/evidence/latest)")

    # 元数据
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    # 关系
    hotel = relationship("Hotel", back_populates="reviews")
    images = relationship("ReviewImage", back_populates="review", cascade="all, delete-orphan")
    reply = relationship("ReviewReply", back_populates="review", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_review_hotel_date", "hotel_id", "review_date"),
        Index("idx_review_source_pool", "source_pool"),
    )

    def __repr__(self):
        return f"<Review(id={self.id}, hotel_id={self.hotel_id}, score={self.overall_score})>"


class ReviewImage(Base):
    """评论图片表"""
    __tablename__ = "review_images"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(String(100), ForeignKey("reviews.review_id"), nullable=False, index=True, comment="评论ID")
    image_url = Column(String(500), nullable=False, comment="图片URL")
    thumbnail_url = Column(String(500), comment="缩略图URL")
    sort_order = Column(Integer, default=0, comment="排序顺序")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    # 关系
    review = relationship("Review", back_populates="images")

    def __repr__(self):
        return f"<ReviewImage(id={self.id}, review_id={self.review_id})>"


class ReviewReply(Base):
    """商家回复表"""
    __tablename__ = "review_replies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(String(100), ForeignKey("reviews.review_id"), unique=True, nullable=False, comment="评论ID")
    content = Column(Text, nullable=False, comment="回复内容")
    reply_date = Column(DateTime, comment="回复日期")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    # 关系
    review = relationship("Review", back_populates="reply")

    def __repr__(self):
        return f"<ReviewReply(id={self.id}, review_id={self.review_id})>"


class CrawlTask(Base):
    """爬取任务表"""
    __tablename__ = "crawl_tasks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(100), unique=True, index=True, comment="任务ID")
    task_type = Column(String(50), nullable=False, comment="任务类型(hotel_list/review)")

    # 任务参数
    region_type = Column(String(50), comment="功能区类型")
    business_zone_code = Column(String(20), comment="商圈代码")
    hotel_id = Column(String(50), comment="酒店ID(评论任务)")
    price_level = Column(String(20), comment="价格档次")

    # 任务状态
    status = Column(String(20), default="pending", index=True, comment="任务状态")
    priority = Column(Integer, default=0, comment="优先级")
    retry_count = Column(Integer, default=0, comment="重试次数")
    error_message = Column(Text, comment="错误信息")

    # 结果统计
    items_crawled = Column(Integer, default=0, comment="已爬取数量")
    items_total = Column(Integer, comment="总数量")

    # 时间戳
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    started_at = Column(DateTime, comment="开始时间")
    completed_at = Column(DateTime, comment="完成时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    __table_args__ = (
        Index("idx_task_status_priority", "status", "priority"),
    )

    def __repr__(self):
        return f"<CrawlTask(id={self.id}, task_type={self.task_type}, status={self.status})>"


class CrawlLog(Base):
    """爬取日志表"""
    __tablename__ = "crawl_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(100), index=True, comment="任务ID")
    level = Column(String(20), comment="日志级别")
    message = Column(Text, comment="日志消息")
    details = Column(JSON, comment="详细信息")
    created_at = Column(DateTime, default=datetime.now, index=True, comment="创建时间")

    def __repr__(self):
        return f"<CrawlLog(id={self.id}, level={self.level})>"
