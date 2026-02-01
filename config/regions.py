"""广州功能区配置

根据爬取逻辑.md中的分层抽样策略，定义广州市6大核心功能区及其商圈配置。
每个功能区包含3个代表性商圈，每个商圈按4个价格档次采集酒店。
"""

# 价格档次配置（适用于所有功能区）
PRICE_RANGES = [
    {"level": "经济型", "min": 0, "max": 300, "top_n": 4},
    {"level": "舒适型", "min": 300, "max": 600, "top_n": 6},
    {"level": "高档型", "min": 600, "max": 900, "top_n": 3},
    {"level": "奢华型", "min": 900, "max": 99999, "top_n": 2},
]

# 广州6大功能区配置
GUANGZHOU_REGIONS = {
    "CBD商务区": {
        "description": "商务精英/购物达人，诉求：效率、排面、景观、便利",
        "typical_price_range": "600-2000+",
        "business_zones": [
            {"name": "珠江新城/五羊新城商圈", "code": "39584"},
            {"name": "火车东站/天河体育中心商圈", "code": "39585"},
            {"name": "环市东路商圈", "code": "39581"},
        ],
        "price_ranges": PRICE_RANGES,
        "keywords": ["广州塔景观", "地铁上盖", "行政酒廊", "豪华", "太古汇", "天环广场"],
        "aspect_focus": ["景观", "位置", "设施"],
    },

    "老城文化区": {
        "description": "文化游客/吃货/文青，诉求：地道美食、历史氛围、打卡",
        "typical_price_range": "300-1000",
        "business_zones": [
            {"name": "海珠广场/北京路步行街商圈", "code": "39580"},
            {"name": "沙面岛/上下九步行街商圈", "code": "39582"},
            {"name": "火车站/越秀公园商圈", "code": "39583"},
        ],
        "price_ranges": PRICE_RANGES,
        "keywords": ["早茶", "骑楼", "步行街", "老字号", "珠江夜游", "复古"],
        "aspect_focus": ["餐饮", "周边", "卫生"],
    },

    "交通枢纽区": {
        "description": "中转旅客/赶车族，诉求：不误点、免费接送、安静睡觉",
        "typical_price_range": "200-600",
        "business_zones": [
            {"name": "新白云国际机场商圈", "code": "39591"},
            {"name": "长隆番禺/广州南站商圈", "code": "39587"},
            {"name": "白云黄石/同德围", "code": "920"},
        ],
        "price_ranges": PRICE_RANGES,
        "keywords": ["免费接送机", "叫醒服务", "离进站口近", "隔音好", "24小时前台"],
        "aspect_focus": ["服务", "噪音", "位置"],
    },

    "会展活动区": {
        "description": "参展商/看展观众，诉求：步行直达、展期不涨价太离谱",
        "typical_price_range": "400-1500+",
        "business_zones": [
            {"name": "琶洲国际会展中心商圈", "code": "39589"},
            {"name": "江南西/国际轻纺城/珠江南附近商圈", "code": "39588"},
            {"name": "大沙地商圈", "code": "40110"},
        ],
        "price_ranges": PRICE_RANGES,
        "keywords": ["广交会", "步行可达", "展馆对面", "商务中心", "早餐早开"],
        "aspect_focus": ["位置", "价格", "网络"],
    },

    "度假亲子区": {
        "description": "家庭亲子/情侣度假，诉求：孩子开心、一站式躺平",
        "typical_price_range": "800-3000+",
        "business_zones": [
            {"name": "从化温泉旅游区", "code": "14803"},
            {"name": "白水寨/挂绿广场商圈", "code": "40109"},
            {"name": "花都融创文旅城周边", "code": "95"},
        ],
        "price_ranges": PRICE_RANGES,
        "keywords": ["野生动物世界", "亲子房", "儿童牙刷", "接驳车", "温泉", "无边泳池"],
        "aspect_focus": ["设施", "服务", "卫生"],
    },

    "高校科技区": {
        "description": "考研学生/家长/IT从业者，诉求：安静、便宜、近考点/园区",
        "typical_price_range": "200-500",
        "business_zones": [
            {"name": "广州大学城附近商圈", "code": "39590"},
            {"name": "萝岗科学城/宝能演艺中心", "code": "923"},
            {"name": "中新广州知识城", "code": "42647"},
        ],
        "price_ranges": PRICE_RANGES,
        "keywords": ["中山大学", "考研房", "安静", "网易总部", "干净卫生"],
        "aspect_focus": ["噪音", "价格", "网络"],
    },
}


def get_all_business_zones() -> list:
    """获取所有商圈列表"""
    zones = []
    for region_name, region_data in GUANGZHOU_REGIONS.items():
        for zone in region_data["business_zones"]:
            zones.append({
                "region": region_name,
                "zone_name": zone["name"],
                "zone_code": zone["code"],
            })
    return zones


def get_region_by_zone_code(zone_code: str) -> str | None:
    """根据商圈代码获取所属功能区"""
    for region_name, region_data in GUANGZHOU_REGIONS.items():
        for zone in region_data["business_zones"]:
            if zone["code"] == zone_code:
                return region_name
    return None


def calculate_expected_hotels() -> dict:
    """计算预期采集的酒店数量"""
    total = 0
    breakdown = {}

    for region_name, region_data in GUANGZHOU_REGIONS.items():
        zone_count = len(region_data["business_zones"])
        hotels_per_zone = sum(pr["top_n"] for pr in region_data["price_ranges"])
        region_total = zone_count * hotels_per_zone
        breakdown[region_name] = {
            "zones": zone_count,
            "hotels_per_zone": hotels_per_zone,
            "total": region_total,
        }
        total += region_total

    return {
        "total": total,
        "breakdown": breakdown,
    }
