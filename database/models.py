from datetime import datetime

from sqlalchemy import Column, String, Integer, Float, Text, DateTime, ForeignKey, Index, JSON
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class Hotel(Base):
    """酒店基础信息表。"""

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
    region_type = Column(String(50), comment="功能区类型")
    business_zone = Column(String(100), comment="商圈名称")
    business_zone_code = Column(String(20), comment="商圈代码")
    price_level = Column(String(20), comment="价格档次")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间")

    reviews = relationship("Review", back_populates="hotel", cascade="all, delete-orphan")
    negative_reviews = relationship("ReviewNegative", back_populates="hotel", cascade="all, delete-orphan")
    positive_reviews = relationship("ReviewPositive", back_populates="hotel", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_hotel_region_zone", "region_type", "business_zone_code"),
        Index("idx_hotel_price_level", "price_level"),
    )

    def __repr__(self):
        return f"<Hotel(id={self.id}, hotel_id={self.hotel_id}, name={self.name})>"


class Review(Base):
    """全部评论主表。"""

    __tablename__ = "reviews"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(String(100), unique=True, index=True, comment="评论ID")
    hotel_id = Column(String(50), ForeignKey("hotels.hotel_id"), nullable=False, index=True, comment="酒店ID")
    user_nick = Column(String(100), comment="用户昵称")
    content = Column(Text, nullable=False, comment="评论内容")
    summary = Column(String(500), comment="评论摘要")
    score_clean = Column(Float, comment="清洁评分")
    score_location = Column(Float, comment="位置评分")
    score_service = Column(Float, comment="服务评分")
    score_value = Column(Float, comment="性价比评分")
    overall_score = Column(Float, comment="综合评分")
    tags = Column(JSON, default=list, comment="标签列表")
    room_type = Column(String(100), comment="房型")
    review_date = Column(DateTime, comment="评论日期")
    source_pool = Column(String(20), comment="来源池(negative/positive)")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    hotel = relationship("Hotel", back_populates="reviews")
    negative_entry = relationship("ReviewNegative", back_populates="review", uselist=False, cascade="all, delete-orphan")
    positive_entry = relationship("ReviewPositive", back_populates="review", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_review_hotel_date", "hotel_id", "review_date"),
        Index("idx_review_source_pool", "source_pool"),
    )

    def __repr__(self):
        return f"<Review(id={self.id}, hotel_id={self.hotel_id}, score={self.overall_score})>"


class ReviewNegative(Base):
    """负面评论子表。"""

    __tablename__ = "reviews_negative"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(String(100), ForeignKey("reviews.review_id"), unique=True, nullable=False, index=True, comment="评论ID")
    hotel_id = Column(String(50), ForeignKey("hotels.hotel_id"), nullable=False, index=True, comment="酒店ID")
    user_nick = Column(String(100), comment="用户昵称")
    content = Column(Text, nullable=False, comment="评论内容")
    summary = Column(String(500), comment="评论摘要")
    overall_score = Column(Float, comment="综合评分")
    review_date = Column(DateTime, comment="评论日期")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    hotel = relationship("Hotel", back_populates="negative_reviews")
    review = relationship("Review", back_populates="negative_entry")

    __table_args__ = (Index("idx_review_negative_hotel_date", "hotel_id", "review_date"),)

    def __repr__(self):
        return f"<ReviewNegative(id={self.id}, review_id={self.review_id})>"


class ReviewPositive(Base):
    """正面评论子表。"""

    __tablename__ = "reviews_positive"

    id = Column(Integer, primary_key=True, autoincrement=True)
    review_id = Column(String(100), ForeignKey("reviews.review_id"), unique=True, nullable=False, index=True, comment="评论ID")
    hotel_id = Column(String(50), ForeignKey("hotels.hotel_id"), nullable=False, index=True, comment="酒店ID")
    user_nick = Column(String(100), comment="用户昵称")
    content = Column(Text, nullable=False, comment="评论内容")
    summary = Column(String(500), comment="评论摘要")
    overall_score = Column(Float, comment="综合评分")
    review_date = Column(DateTime, comment="评论日期")
    created_at = Column(DateTime, default=datetime.now, comment="创建时间")

    hotel = relationship("Hotel", back_populates="positive_reviews")
    review = relationship("Review", back_populates="positive_entry")

    __table_args__ = (Index("idx_review_positive_hotel_date", "hotel_id", "review_date"),)

    def __repr__(self):
        return f"<ReviewPositive(id={self.id}, review_id={self.review_id})>"
