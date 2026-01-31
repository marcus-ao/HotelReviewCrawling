"""飞猪酒店评论爬虫主程序

使用方法:
    # 测试模式（验证配置和连接）
    python main.py --mode test

    # 爬取酒店列表（基本信息）
    python main.py --mode hotel_list --region "CBD商务区"

    # 爬取所有功能区的酒店列表
    python main.py --mode hotel_list --all

    # 补充酒店详细信息（评分、价格等）
    python main.py --mode enrich_details

    # 爬取评论
    python main.py --mode reviews --hotel-id 10019773

    # 爬取所有酒店的评论
    python main.py --mode reviews --all
"""
import argparse
import sys
from tqdm import tqdm

from config.settings import settings
from config.regions import GUANGZHOU_REGIONS, calculate_expected_hotels
from utils.logger import setup_logger, get_logger
from database.connection import init_db, check_connection
from crawler import AntiCrawler, HotelListCrawler, ReviewCrawler
from scheduler import TaskScheduler

logger = get_logger("main")


def test_mode():
    """测试模式：验证配置和连接"""
    print("=" * 60)
    print("飞猪酒店评论爬虫 - 测试模式")
    print("=" * 60)

    # 1. 检查配置
    print("\n[1/4] 检查配置...")
    print(f"  - 数据库: {settings.db_host}:{settings.db_port}/{settings.db_name}")
    print(f"  - Chrome调试端口: {settings.chrome_debug_port}")
    print(f"  - 日志目录: {settings.log_path}")
    print(f"  - 延迟范围: {settings.min_delay}-{settings.max_delay}秒")

    # 2. 检查功能区配置
    print("\n[2/4] 检查功能区配置...")
    expected = calculate_expected_hotels()
    print(f"  - 功能区数量: {len(GUANGZHOU_REGIONS)}")
    print(f"  - 预期酒店总数: {expected['total']}")
    for region, data in expected['breakdown'].items():
        print(f"    - {region}: {data['zones']}个商圈 × {data['hotels_per_zone']}家 = {data['total']}家")

    # 3. 检查数据库连接
    print("\n[3/4] 检查数据库连接...")
    try:
        if check_connection():
            print("  - 数据库连接成功")
            init_db()
            print("  - 数据库表初始化成功")
        else:
            print("  - 数据库连接失败，请检查配置")
    except Exception as e:
        print(f"  - 数据库错误: {e}")

    # 4. 检查浏览器连接
    print("\n[4/4] 检查浏览器连接...")
    try:
        anti_crawler = AntiCrawler()
        anti_crawler.init_browser()
        print("  - 浏览器连接成功")
        anti_crawler.close()
    except Exception as e:
        print(f"  - 浏览器连接失败: {e}")
        print(f"  - 请先手动启动Chrome:")
        print(f'    chrome.exe --remote-debugging-port={settings.chrome_debug_port} '
              f'--user-data-dir="{settings.chrome_user_data_dir}"')

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


def crawl_hotel_list(region: str = None, all_regions: bool = False):
    """爬取酒店列表

    Args:
        region: 指定功能区
        all_regions: 是否爬取所有功能区
    """
    logger.info("开始爬取酒店列表")

    anti_crawler = AntiCrawler()
    crawler = HotelListCrawler(anti_crawler)

    try:
        anti_crawler.init_browser()

        if all_regions:
            # 爬取所有功能区
            hotels = crawler.crawl_all_regions(save_to_db=True)
            logger.info(f"全部爬取完成，共 {len(hotels)} 家酒店")

        elif region:
            if region not in GUANGZHOU_REGIONS:
                logger.error(f"未知的功能区: {region}")
                logger.info(f"可用的功能区: {list(GUANGZHOU_REGIONS.keys())}")
                return

            hotels = crawler.crawl_region(region, save_to_db=True)
            logger.info(f"功能区 {region} 爬取完成，共 {len(hotels)} 家酒店")

        else:
            logger.error("请指定功能区 (--region) 或使用 --all 爬取所有功能区")

    except KeyboardInterrupt:
        logger.warning("用户中断爬取")
    except Exception as e:
        logger.error(f"爬取失败: {e}")
    finally:
        anti_crawler.close()


def enrich_hotel_details():
    """补充酒店详细信息"""
    logger.info("开始补充酒店详细信息")
    
    anti_crawler = AntiCrawler()
    crawler = HotelListCrawler(anti_crawler)
    
    try:
        anti_crawler.init_browser()
        
        # 补充所有缺少详细信息的酒店
        enriched_count = crawler.enrich_hotel_details()
        
        logger.info(f"补充完成，成功更新 {enriched_count} 家酒店的详细信息")
        
    except KeyboardInterrupt:
        logger.warning("用户中断补充")
    except Exception as e:
        logger.error(f"补充失败: {e}")
    finally:
        anti_crawler.close()


def crawl_reviews(hotel_id: str = None, all_hotels: bool = False):
    """爬取评论

    Args:
        hotel_id: 指定酒店ID
        all_hotels: 是否爬取所有酒店的评论
    """
    logger.info("开始爬取评论")

    anti_crawler = AntiCrawler()
    crawler = ReviewCrawler(anti_crawler)
    scheduler = TaskScheduler()

    try:
        anti_crawler.init_browser()

        if hotel_id:
            # 爬取指定酒店的评论
            reviews = crawler.crawl_hotel_reviews(hotel_id, save_to_db=True)
            logger.info(f"酒店 {hotel_id} 评论爬取完成，共 {len(reviews)} 条")

        elif all_hotels:
            # 创建评论爬取任务
            task_ids = scheduler.create_review_tasks()
            logger.info(f"创建了 {len(task_ids)} 个评论爬取任务")

            # 执行任务
            for task in tqdm(scheduler.get_pending_tasks(task_type="review"), desc="爬取评论"):
                try:
                    scheduler.start_task(task.task_id)

                    reviews = crawler.crawl_hotel_reviews(task.hotel_id, save_to_db=True)

                    if reviews:
                        scheduler.complete_task(task.task_id, len(reviews))
                    else:
                        scheduler.skip_task(task.task_id, "无评论数据")

                except Exception as e:
                    scheduler.fail_task(task.task_id, str(e))
                    logger.error(f"任务 {task.task_id} 失败: {e}")

                # 任务间延迟
                anti_crawler.random_delay(3, 6)

            # 打印统计
            stats = scheduler.get_task_stats()
            logger.info(f"任务统计: {stats}")

        else:
            logger.error("请指定酒店ID (--hotel-id) 或使用 --all 爬取所有酒店的评论")

    except KeyboardInterrupt:
        logger.warning("用户中断爬取")
    except Exception as e:
        logger.error(f"爬取失败: {e}")
    finally:
        anti_crawler.close()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="飞猪酒店评论爬虫",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py --mode test                          # 测试模式
  python main.py --mode hotel_list --region "CBD商务区"  # 爬取指定功能区
  python main.py --mode hotel_list --all              # 爬取所有功能区
  python main.py --mode reviews --hotel-id 10019773   # 爬取指定酒店评论
  python main.py --mode reviews --all                 # 爬取所有酒店评论
        """
    )

    parser.add_argument(
        "--mode",
        choices=["test", "hotel_list", "enrich_details", "reviews"],
        required=True,
        help="运行模式: test(测试), hotel_list(酒店列表), enrich_details(补充详情), reviews(评论)"
    )

    parser.add_argument(
        "--region",
        type=str,
        help="功能区名称 (用于hotel_list模式)"
    )

    parser.add_argument(
        "--hotel-id",
        type=str,
        help="酒店ID (用于reviews模式)"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="爬取所有 (功能区或酒店)"
    )

    args = parser.parse_args()

    # 初始化日志
    setup_logger()

    # 根据模式执行
    if args.mode == "test":
        test_mode()

    elif args.mode == "hotel_list":
        crawl_hotel_list(region=args.region, all_regions=args.all)

    elif args.mode == "enrich_details":
        enrich_hotel_details()

    elif args.mode == "reviews":
        crawl_reviews(hotel_id=args.hotel_id, all_hotels=args.all)


if __name__ == "__main__":
    main()
