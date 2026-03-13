"""Context helpers for hotel list crawling logs."""

from typing import Any


_ORDERED_KEYS = [
    "region_type",
    "business_zone",
    "business_zone_code",
    "price_level",
    "target_count",
    "saved_count",
    "current_page",
    "current_url",
]


def update_position_context(context: dict[str, object], **kwargs: object) -> None:
    """Update crawl position context for logging and resume hints."""
    for key, value in kwargs.items():
        if value is None:
            context.pop(key, None)
        else:
            context[key] = value


def clear_position_context(context: dict[str, object]) -> None:
    """Clear crawl position context."""
    context.clear()


def format_position_context(context: dict[str, object]) -> str:
    """Format crawl position context for logs."""
    parts: list[str] = []
    for key in _ORDERED_KEYS:
        value = context.get(key)
        if value is not None:
            parts.append(f"{key}={value}")
    return ", ".join(parts) if parts else "context=unknown"


def log_captcha_failure(logger: Any, context: dict[str, object], action: str, exc: Exception) -> None:
    """Log captcha failure with current crawl context."""
    logger.error(f"{action} 遇到验证码失败: {exc} | {format_position_context(context)}")
