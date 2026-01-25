"""爬虫模块初始化"""
from .anti_crawler import AntiCrawler
from .hotel_list_crawler import HotelListCrawler
from .review_crawler import ReviewCrawler

__all__ = ['AntiCrawler', 'HotelListCrawler', 'ReviewCrawler']
