"""Helpers for parsing Fliggy query payloads."""

import json
from typing import Any, Optional

from utils.cleaner import clean_text, extract_price, normalize_hotel_name


def extract_json_blob(html: str, var_name: str) -> Optional[str]:
    """Extract JSON object assigned to a JS variable from HTML."""
    token = f"{var_name} ="
    start_idx = html.find(token)
    if start_idx == -1:
        return None

    brace_start = html.find("{", start_idx)
    if brace_start == -1:
        return None

    depth = 0
    in_string = False
    escape = False

    for i in range(brace_start, len(html)):
        ch = html[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return html[brace_start: i + 1]

    return None


def extract_query_page_info(html: str) -> Optional[dict]:
    """Extract paging metadata from ``__QUERY_RESULT_DATA__`` payload."""
    blob = extract_json_blob(html, "__QUERY_RESULT_DATA__")
    if not blob:
        return None

    try:
        data = json.loads(blob)
    except Exception:
        return None

    query = data.get("query")
    if not isinstance(query, dict):
        return None

    return {
        "currentPage": query.get("currentPage"),
        "totalPage": query.get("totalPage"),
        "pageSize": query.get("pageSize"),
        "offset": query.get("offset"),
    }


def extract_hotels_from_query_data(html: str, logger: Optional[Any] = None) -> list[dict]:
    """Extract hotel list from ``__QUERY_RESULT_DATA__`` JSON payload."""
    blob = extract_json_blob(html, "__QUERY_RESULT_DATA__")
    if not blob:
        return []

    try:
        data = json.loads(blob)
    except Exception as exc:
        if logger is not None:
            logger.debug(f"query data json parse failed: {exc}")
        return []

    hotel_list = data.get("hotelList") or []
    if not isinstance(hotel_list, list):
        return []

    hotels: list[dict] = []

    for item in hotel_list:
        if not isinstance(item, dict):
            continue

        hotel_id = item.get("shid") or item.get("hotelId") or item.get("hotel_id")
        if hotel_id is None:
            continue
        hotel_id = str(hotel_id)

        name = item.get("name") or item.get("hotelName") or item.get("title")
        if not name:
            continue

        name = normalize_hotel_name(clean_text(str(name)))
        if not name:
            continue

        rating_score = None
        rate_score_raw = item.get("rateScore")
        if rate_score_raw is not None:
            try:
                rating_score = float(rate_score_raw)
            except Exception:
                rating_score = None

        review_count = 0
        rate_num_raw = item.get("rateNum")
        if rate_num_raw is not None:
            try:
                review_count = int(rate_num_raw)
            except Exception:
                review_count = 0

        base_price = None
        price_disp = item.get("priceDesp")
        if price_disp is not None:
            base_price = extract_price(str(price_disp))

        if base_price is None:
            price_without_tax = item.get("priceWithoutTax") or {}
            if isinstance(price_without_tax, dict):
                amount_cny = price_without_tax.get("amountCNY")
                if amount_cny is not None:
                    try:
                        base_price = int(amount_cny)
                    except Exception:
                        base_price = None

        if base_price is None:
            price_raw = item.get("price")
            if price_raw is not None:
                try:
                    price_raw = float(price_raw)
                    base_price = int(price_raw / 100) if price_raw > 1000 else int(price_raw)
                except Exception:
                    base_price = None

        address = item.get("address")
        if address:
            address = clean_text(str(address))

        star_level = None
        level = item.get("level") or {}
        if isinstance(level, dict):
            star_level = level.get("desc") or level.get("starRate") or level.get("star")

        latitude = None
        longitude = None
        lat_raw = item.get("lat")
        lng_raw = item.get("lng")
        try:
            if lat_raw is not None:
                latitude = float(lat_raw)
            if lng_raw is not None:
                longitude = float(lng_raw)
        except Exception:
            latitude = None
            longitude = None

        hotels.append(
            {
                "hotel_id": hotel_id,
                "name": name,
                "address": address,
                "latitude": latitude,
                "longitude": longitude,
                "star_level": star_level,
                "rating_score": rating_score,
                "review_count": review_count,
                "base_price": base_price,
            }
        )

    return hotels
