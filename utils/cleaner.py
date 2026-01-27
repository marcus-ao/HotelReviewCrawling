"""数据清洗工具模块"""
import re
from typing import Optional
from bs4 import BeautifulSoup


def clean_text(text: str, remove_emoji: bool = False) -> str:
    """清洗文本内容

    Args:
        text: 原始文本
        remove_emoji: 是否移除表情符号

    Returns:
        清洗后的文本
    """
    if not text:
        return ""

    # 去除HTML标签
    text = BeautifulSoup(text, "lxml").get_text()

    # 去除HTML实体
    text = re.sub(r'&[a-zA-Z]+;', '', text)
    text = re.sub(r'&#\d+;', '', text)

    # 规范化空白字符
    text = re.sub(r'\s+', ' ', text)

    # 去除首尾空白
    text = text.strip()

    # 去除引号装饰
    text = text.strip('"').strip('"').strip('"')

    if remove_emoji:
        # 移除表情符号
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE
        )
        text = emoji_pattern.sub('', text)

    return text


def extract_tags(text: str) -> list[str]:
    """从评论中提取标签

    Args:
        text: 评论文本

    Returns:
        标签列表
    """
    if not text:
        return []

    # 匹配 #标签 格式
    hash_tags = re.findall(r'#([^\s#,，]+)', text)

    # 匹配常见的评价标签
    common_tags = [
        "交通便利", "位置好", "服务热情", "干净卫生", "设施齐全",
        "早餐丰盛", "性价比高", "安静舒适", "停车方便", "环境优雅",
        "前台热情", "住宿舒适", "吃饭方便", "体验感强", "设施很好",
    ]

    found_tags = []
    for tag in common_tags:
        if tag in text:
            found_tags.append(tag)

    # 合并去重
    all_tags = list(set(hash_tags + found_tags))
    return all_tags


def parse_star_score(style_width: str) -> float:
    """解析星级评分

    从CSS style中的width百分比解析评分
    例如: "width:80%" -> 4.0 (80% * 5 = 4)

    Args:
        style_width: CSS width样式值，如 "width:80%"

    Returns:
        评分值 (1-5)
    """
    if not style_width:
        return 0.0

    # 提取百分比数值
    match = re.search(r'(\d+)%', style_width)
    if match:
        percentage = int(match.group(1))
        # 转换为5分制
        score = percentage / 100 * 5
        return round(score, 1)

    return 0.0


def parse_date(date_str: str) -> Optional[str]:
    """解析日期字符串
    
    支持多种日期格式：
    - 标准格式: "[2026-01-11 20:34]" 或 "2026-01-11 20:34"
    - 相对时间: "2天前", "昨天", "今天", "刚刚"
    - 其他格式: "2026/01/11", "2026.01.11"

    Args:
        date_str: 日期字符串

    Returns:
        标准化日期字符串 "YYYY-MM-DD HH:MM:SS" 或 None
    """
    if not date_str:
        return None
    
    try:
        from datetime import datetime, timedelta
        
        # 去除方括号和首尾空白
        date_str = date_str.strip('[]').strip()
        
        # 处理相对时间
        if '天前' in date_str:
            match = re.search(r'(\d+)天前', date_str)
            if match:
                days = int(match.group(1))
                target_date = datetime.now() - timedelta(days=days)
                return target_date.strftime("%Y-%m-%d %H:%M:%S")
        
        elif '小时前' in date_str:
            match = re.search(r'(\d+)小时前', date_str)
            if match:
                hours = int(match.group(1))
                target_date = datetime.now() - timedelta(hours=hours)
                return target_date.strftime("%Y-%m-%d %H:%M:%S")
        
        elif '分钟前' in date_str:
            match = re.search(r'(\d+)分钟前', date_str)
            if match:
                minutes = int(match.group(1))
                target_date = datetime.now() - timedelta(minutes=minutes)
                return target_date.strftime("%Y-%m-%d %H:%M:%S")
        
        elif '昨天' in date_str:
            target_date = datetime.now() - timedelta(days=1)
            # 尝试提取时间部分
            time_match = re.search(r'(\d{2}:\d{2})', date_str)
            if time_match:
                time_str = time_match.group(1)
                return f"{target_date.strftime('%Y-%m-%d')} {time_str}:00"
            return target_date.strftime("%Y-%m-%d 00:00:00")
        
        elif '今天' in date_str or '刚刚' in date_str:
            target_date = datetime.now()
            # 尝试提取时间部分
            time_match = re.search(r'(\d{2}:\d{2})', date_str)
            if time_match:
                time_str = time_match.group(1)
                return f"{target_date.strftime('%Y-%m-%d')} {time_str}:00"
            return target_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # 匹配标准日期时间格式 (YYYY-MM-DD HH:MM 或 YYYY-MM-DD)
        match = re.search(r'(\d{4}[-/\.]\d{2}[-/\.]\d{2})\s*(\d{2}:\d{2})?', date_str)
        if match:
            date_part = match.group(1).replace('/', '-').replace('.', '-')
            time_part = match.group(2) or "00:00"
            
            # 验证日期有效性
            try:
                datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M")
                return f"{date_part} {time_part}:00"
            except ValueError:
                # 日期无效（如2月30日）
                return None
        
        return None
        
    except (ValueError, AttributeError) as e:
        # 日期解析失败，返回None
        return None


def extract_price(price_str: str) -> Optional[int]:
    """提取价格数值

    Args:
        price_str: 价格字符串，如 "¥857" 或 "857"

    Returns:
        价格整数值
    """
    if not price_str:
        return None

    # 提取数字
    match = re.search(r'(\d+)', price_str)
    if match:
        return int(match.group(1))

    return None


def normalize_hotel_name(name: str) -> str:
    """规范化酒店名称

    Args:
        name: 原始酒店名称

    Returns:
        规范化后的名称
    """
    if not name:
        return ""

    # 去除HTML实体
    name = clean_text(name)

    # 去除多余空格
    name = re.sub(r'\s+', '', name)

    return name
