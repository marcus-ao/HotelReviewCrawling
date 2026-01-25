# 飞猪酒店页面HTML结构分析

## 一、酒店列表页面结构分析

基于 `hotel_list3.htm` 文件的分析：

### 1.1 页面基本信息
- **URL格式**: `//hotel.fliggy.com/hotel_list3.htm`
- **城市参数**: `cityName=广州`, `city=440100`
- **日期参数**: `checkIn=2026-01-26`, `checkOut=2026-01-27`

### 1.2 筛选条件结构
页面提供了多维度的筛选条件：

#### 位置筛选 (`#J_FilterLocation`)
- **商圈**: 如"长隆番禺/广州南站商圈"、"珠江新城/五羊新城商圈"等
- **行政区**: 天河区、从化区、番禺区等
- **地铁线路**: 1号线至22号线、APM线、广佛线
- **景点**: 广州塔、北京路文化旅游区等
- **车站机场**: 广州南站、白云国际机场等
- **医院**: 中山大学附属第一医院等
- **大学**: 中山大学、华南理工大学等

#### 价格筛选 (`#J_FilterPrice`)
- 100元以下 (R1)
- 100-300元 (R2)
- 300-600元 (R3)
- 600-1500元 (R4)
- 1500元以上 (R5)
- 自定义价格区间

#### 星级筛选 (`#J_FilterLevel`)
- 五星/豪华 (5)
- 四星/高档 (4)
- 三星/舒适 (3)
- 二星及以下 (2)
- 经济连锁 (1)
- 客栈公寓 (0)

#### 品牌筛选 (`#J_FilterBrand`)
分为三大类型：
- **经济型**: 菲住、逸米、汉庭、7天、如家等
- **舒适型**: 麗枫、全季、维也纳、城市便捷等
- **高档型**: 亚朵、宜尚酒店、柏高酒店等

### 1.3 酒店列表数据结构
**注意**: 从HTML源代码分析，酒店列表数据很可能是通过JavaScript动态加载的，需要分析AJAX请求。

关键JavaScript变量：
```javascript
window.g_config = {
    appId:10,
    toolbar:false,
    one_search_version:'0.2.11',
    assetsServer:"//g.alicdn.com/",
    app_version: "0.5.20"
};
```

## 二、酒店详情页面结构分析

基于 `hotel_detail2.htm` 文件的分析：

### 2.1 酒店基本信息
- **酒店ID**: `hid:3472` (从JavaScript中提取)
- **酒店名称**: 广州海航威斯汀酒店
- **评分**: 4.7分
- **点评数**: 8155条

### 2.2 评论区域结构 (`#hotel-review`)

#### 评论列表容器
```html
<div id="J_ReviewList" class="review-list">
```

#### 评论筛选条件 (`#J_ReviewTag`)
- **全部评论** (rateScore=0)
- **好评** (rateScore=1)
- **差评** (rateScore=2)
- **有图** (rateScore=3)
- **追加评价** (checkbox)

#### 单条评论结构 (`li.tb-r-comment`)

**评论者信息** (`div.tb-r-buyer`):
- 用户头像
- 用户昵称

**评论主体** (`div.tb-r-body`):

1. **评分信息** (`ul.starscore`):
```html
<li><span>清洁程度:</span><span class="stars">★★★★★<em style="width:100%">★★★★★</em></span></li>
<li><span>服务体验:</span><span class="stars">★★★★★<em style="width:100%">★★★★★</em></span></li>
<li><span>性价比:</span><span class="stars">★★★★★<em style="width:100%">★★★★★</em></span></li>
```
**关键**: 通过 `em` 标签的 `width` 样式来表示评分（如 width:100% = 5星）

2. **评论标签** (`div.comment-name`):
```html
<div class="comment-name">"设施老了,非常特别,非常棒"</div>
```

3. **评论内容** (`div.tb-r-cnt`):
```html
<div class="tb-r-cnt">位置的话在东站，去哪都比较方便 ，但也堵车...</div>
```

4. **评论图片** (`ul.tb-r-photos`):
```html
<ul class="tb-r-photos clearfix">
    <li><img src="..." /></li>
</ul>
```

5. **评论时间** (`span.tb-r-date`):
```html
<span class="tb-r-date">[2026-01-11 20:34]</span>
```

6. **商家回复** (如果有):
```html
<div class="tb-r-reply">
    <div class="tb-r-cnt">...</div>
    <div class="tb-r-info">
        <span class="tb-r-date">[2026-01-12 10:24]</span>
    </div>
</div>
```

### 2.3 评论加载机制

从JavaScript代码分析：
```javascript
var _hotel_data = {"detail":{"hotel":{"id":3472}}};
KISSY.use('hotel-search/mods/detail-review/index',
function(S, Review) {
    var review = new Review();
    review.type='hotel';
    review.hotelParams={hid:3472, sellerId:'', page:1, showContent:0};
    review.showPagation(8159);
});
```

**关键参数**:
- `hid`: 酒店ID
- `page`: 页码
- `showContent`: 是否显示内容
- 总评论数: 8159条

## 三、关键数据提取点总结

### 3.1 酒店列表页需要提取的数据
1. 酒店ID (hotel_id)
2. 酒店名称 (hotel_name)
3. 酒店地址 (address)
4. 基准价格 (base_price)
5. 价格区间 (price_range)
6. 星级 (star_level)
7. 品牌 (brand)
8. 经纬度 (lat/lng)
9. 评分 (rating)
10. 评论数 (review_count)

### 3.2 酒店详情页需要提取的数据
1. 酒店完整信息
2. 房型列表及价格
3. 设施信息
4. 酒店图片

### 3.3 评论数据需要提取的字段
1. **review_id**: 评论唯一标识
2. **hotel_id**: 酒店ID
3. **user_name**: 用户昵称
4. **user_avatar**: 用户头像URL
5. **content**: 评论正文内容
6. **comment_tags**: 评论标签（从comment-name提取）
7. **create_time**: 评论时间
8. **score_clean**: 清洁程度评分 (1-5星)
9. **score_service**: 服务体验评分 (1-5星)
10. **score_value**: 性价比评分 (1-5星)
11. **has_images**: 是否有图片
12. **image_urls**: 图片URL列表
13. **reply_content**: 商家回复内容（如果有）
14. **reply_time**: 商家回复时间（如果有）
15. **room_type**: 房型名称（如果有）

## 四、数据加载方式判断

### 4.1 酒店列表
- **判断**: 列表数据很可能通过AJAX动态加载
- **需要**: 抓包分析实际的API请求

### 4.2 评论列表
- **判断**: 评论数据通过AJAX分页加载
- **需要**: 分析评论加载的API接口
- **预估API**: 可能类似 `/ajax/getReviews.do` 或类似接口

## 五、下一步行动

1. ✅ 完成HTML结构分析
2. ⏭️ 使用浏览器开发者工具抓包，分析实际的AJAX请求
3. ⏭️ 确定酒店列表API接口
4. ⏭️ 确定评论列表API接口
5. ⏭️ 设计数据提取方案
