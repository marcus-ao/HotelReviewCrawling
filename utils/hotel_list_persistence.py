"""Persistence helpers for hotel-list crawling."""

from typing import Any, Callable, Optional

from config.settings import settings
from database.connection import session_scope
from database.models import CrawlLog, Hotel
from utils.validator import HotelModel


def get_saved_hotel_ids(region_type: str, business_zone_code: str, price_level: str) -> set[str]:
    """Load saved hotel ids for one region/zone/price bucket."""
    with session_scope() as session:
        rows = session.query(Hotel.hotel_id).filter(
            Hotel.region_type == region_type,
            Hotel.business_zone_code == business_zone_code,
            Hotel.price_level == price_level,
        ).all()
    return {hotel_id for hotel_id, in rows if hotel_id}


def get_region_saved_hotel_ids(region_type: str) -> set[str]:
    """Load all saved hotel ids in one functional region."""
    with session_scope() as session:
        rows = session.query(Hotel.hotel_id).filter(Hotel.region_type == region_type).all()
    return {hotel_id for hotel_id, in rows if hotel_id}


def count_region_tier_hotels(region_type: str, price_level: str) -> int:
    """Count saved hotels in one region-tier bucket."""
    with session_scope() as session:
        return session.query(Hotel).filter(
            Hotel.region_type == region_type,
            Hotel.price_level == price_level,
        ).count()


def record_sampling_audit(
    session: Any,
    hotel_data: dict,
    sample_source: str,
    min_review_count: int,
    borrow_for_region: Optional[str] = None,
) -> None:
    """Persist per-hotel sampling source for auditability."""
    hotel_id = hotel_data.get("hotel_id")
    if not hotel_id:
        return

    details: dict[str, Any] = {
        "hotel_id": hotel_id,
        "region_type": hotel_data.get("region_type"),
        "business_zone_code": hotel_data.get("business_zone_code"),
        "price_level": hotel_data.get("price_level"),
        "sample_source": sample_source,
        "min_review_threshold": min_review_count,
    }
    if borrow_for_region:
        details["borrow_for_region"] = borrow_for_region

    session.add(
        CrawlLog(
            task_id=f"sampling:{hotel_id}",
            level="INFO",
            message="hotel_sampling_source",
            details=details,
        )
    )


def save_hotels(
    hotels: list[dict],
    fetch_details: bool,
    min_review_count_override: Optional[int],
    sample_source: str,
    borrow_for_region: Optional[str],
    logger: Any,
    fetch_hotel_details: Callable[[str], Optional[dict]],
    random_delay: Callable[[float, float], None],
    map_price_level: Callable[[Optional[int]], Optional[str]],
) -> int:
    """Save hotels with deduplication and sampling audit."""
    if not hotels:
        return 0

    min_review_count = (
        min_review_count_override
        if min_review_count_override is not None
        else settings.min_reviews_threshold
    )
    filtered_hotels = []
    skipped_low_review = 0
    if min_review_count:
        for hotel in hotels:
            review_count = hotel.get("review_count") or 0
            try:
                review_count = int(review_count)
            except Exception:
                review_count = 0
            if review_count <= min_review_count:
                skipped_low_review += 1
                continue
            filtered_hotels.append(hotel)
        hotels = filtered_hotels

    if not hotels:
        if skipped_low_review:
            logger.info(f"低评论酒店已过滤: {skipped_low_review} 家")
        return 0

    saved_count = 0
    updated_count = 0
    skipped_count = 0

    with session_scope() as session:
        hotel_ids = [h["hotel_id"] for h in hotels if "hotel_id" in h]
        existing_hotels = session.query(Hotel).filter(Hotel.hotel_id.in_(hotel_ids)).all()

        existing_dict = {}
        for hotel in existing_hotels:
            if hotel.hotel_id not in existing_dict:
                existing_dict[hotel.hotel_id] = {}
            key = (hotel.business_zone_code, hotel.price_level)
            existing_dict[hotel.hotel_id][key] = hotel

        logger.debug(f"批量查询: {len(hotel_ids)}个酒店ID, 已存在{len(existing_hotels)}条记录")

        for hotel_data in hotels:
            try:
                hotel_id = hotel_data.get("hotel_id")
                if hotel_id is None:
                    logger.debug("跳过缺少 hotel_id 的酒店详情获取")
                    continue
                business_zone_code = hotel_data.get("business_zone_code")
                price_level = hotel_data.get("price_level")

                key = (business_zone_code, price_level)
                is_duplicate = hotel_id in existing_dict and key in existing_dict[hotel_id]

                if is_duplicate:
                    skipped_count += 1
                    logger.debug(
                        f"跳过重复酒店: {hotel_data.get('name')} "
                        f"(商圈:{business_zone_code}, 价格档次:{price_level})"
                    )
                    continue

                if fetch_details and hotel_id not in existing_dict:
                    logger.info(f"获取酒店 {hotel_id} 的详细信息...")
                    details = fetch_hotel_details(str(hotel_id))

                    if details:
                        hotel_data.update(details)
                        logger.debug(f"成功获取详细信息，更新了 {len(details)} 个字段")

                    random_delay(2, 4)

                mapped_level = map_price_level(hotel_data.get("base_price"))
                if mapped_level:
                    hotel_data["price_level"] = mapped_level

                validated = HotelModel(**hotel_data)

                if hotel_id in existing_dict:
                    first_existing = list(existing_dict[hotel_id].values())[0]

                    basic_fields = [
                        "name",
                        "address",
                        "latitude",
                        "longitude",
                        "star_level",
                        "rating_score",
                        "review_count",
                        "base_price",
                    ]
                    validated_dump = validated.model_dump()
                    for field in basic_fields:
                        value = validated_dump.get(field)
                        if value is not None:
                            setattr(first_existing, field, value)

                    if mapped_level:
                        first_existing.price_level = mapped_level

                    updated_count += 1
                    record_sampling_audit(
                        session=session,
                        hotel_data=validated_dump,
                        sample_source=sample_source,
                        min_review_count=min_review_count,
                        borrow_for_region=borrow_for_region,
                    )
                    logger.debug(f"更新酒店基本信息: {validated.name}")
                else:
                    session.add(Hotel(**validated.model_dump()))
                    saved_count += 1
                    record_sampling_audit(
                        session=session,
                        hotel_data=validated.model_dump(),
                        sample_source=sample_source,
                        min_review_count=min_review_count,
                        borrow_for_region=borrow_for_region,
                    )
                    logger.debug(
                        f"新增酒店: {validated.name} "
                        f"(商圈:{business_zone_code}, 价格档次:{price_level})"
                    )

            except Exception as exc:
                logger.warning(f"保存酒店失败: {exc}")
                continue

    total = saved_count + updated_count
    logger.info(f"保存完成: 新增{saved_count}家, 更新{updated_count}家, 跳过{skipped_count}家, 共处理{total}家")
    return total
