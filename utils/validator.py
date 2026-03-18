"""数据验证模型模块。"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class HotelModel(BaseModel):
    """酒店数据验证模型。"""

    hotel_id: str = Field(..., description="酒店ID (shid)")
    name: str = Field(..., min_length=1, max_length=200, description="酒店名称")
    address: Optional[str] = Field(None, max_length=500, description="酒店地址")
    city_code: str = Field(default="440100", description="城市代码")
    latitude: Optional[float] = Field(None, ge=-90, le=90, description="纬度")
    longitude: Optional[float] = Field(None, ge=-180, le=180, description="经度")
    star_level: Optional[str] = Field(None, description="星级/档次")
    rating_score: Optional[float] = Field(None, ge=0, le=5, description="评分")
    review_count: Optional[int] = Field(None, ge=0, description="评论数量")
    base_price: Optional[int] = Field(None, ge=0, description="起步价格")
    region_type: Optional[str] = Field(None, description="功能区类型")
    business_zone: Optional[str] = Field(None, description="商圈名称")
    business_zone_code: Optional[str] = Field(None, description="商圈代码")
    price_level: Optional[str] = Field(None, description="价格档次")

    @field_validator('hotel_id')
    @classmethod
    def validate_hotel_id(cls, v):
        if not v or not v.strip():
            raise ValueError('酒店ID不能为空')
        return v.strip()

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        if not v or not v.strip():
            raise ValueError('酒店名称不能为空')
        return v.strip()


class ReviewModel(BaseModel):
    """评论数据验证模型。"""

    review_id: Optional[str] = Field(None, description="评论ID")
    hotel_id: str = Field(..., description="酒店ID")
    user_nick: Optional[str] = Field(None, max_length=100, description="用户昵称")
    content: str = Field(..., min_length=1, description="评论内容")
    summary: Optional[str] = Field(None, max_length=500, description="评论摘要")
    score_clean: Optional[float] = Field(None, ge=0, le=5, description="清洁评分")
    score_location: Optional[float] = Field(None, ge=0, le=5, description="位置评分")
    score_service: Optional[float] = Field(None, ge=0, le=5, description="服务评分")
    score_value: Optional[float] = Field(None, ge=0, le=5, description="性价比评分")
    overall_score: Optional[float] = Field(None, ge=0, le=5, description="综合评分")
    tags: Optional[list[str]] = Field(default_factory=list, description="标签列表")
    review_date: Optional[datetime] = Field(None, description="评论日期")
    room_type: Optional[str] = Field(None, description="房型")
    source_pool: Optional[str] = Field(None, description="来源池(negative/positive)")

    @field_validator('hotel_id')
    @classmethod
    def validate_hotel_id(cls, v):
        if not v or not v.strip():
            raise ValueError('酒店ID不能为空')
        return v.strip()

    @field_validator('content')
    @classmethod
    def validate_content(cls, v):
        if not v or not v.strip():
            raise ValueError('评论内容不能为空')
        return v.strip()

    def calculate_overall_score(self) -> float:
        """计算综合评分。"""
        scores = [
            self.score_clean,
            self.score_location,
            self.score_service,
            self.score_value,
        ]
        valid_scores = [s for s in scores if s is not None]
        if valid_scores:
            return round(sum(valid_scores) / len(valid_scores), 1)
        return 0.0
