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
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from config.settings import settings
from config.regions import GUANGZHOU_REGIONS, calculate_expected_hotels
from utils.logger import setup_logger, get_logger
from database.connection import init_db, check_connection
from database.connection import session_scope
from database.models import Hotel
from crawler import (
    AntiCrawler,
    HotelListCrawler,
    ReviewCrawler,
    BrowserConnectionException,
    CaptchaException,
    CaptchaCooldownException,
    RecoverableInterruption,
)

logger = get_logger("main")

EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_CAPTCHA_COOLDOWN = 2


def _write_review_batch_report(report: dict) -> Optional[Path]:
    """把批量评论采集摘要落地到日志目录。"""
    base_log_path = getattr(settings, "log_path", Path("logs"))
    report_dir = Path(base_log_path) / "review_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = report_dir / f"review_batch_report_{timestamp}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_path


def _log_retryable_captcha_stop(action: str, error: CaptchaException):
    """记录可立即重试的验证码失败。"""
    logger.error(f"{action}遇到验证码失败，可立即重试: {error}")


def _log_captcha_cooldown_stop(action: str, error: CaptchaCooldownException):
    """记录验证码冷却停止。"""
    logger.warning(f"{action}因验证码冷却停止: {error.reason}")
    logger.info(f"请等待至少 {error.cooldown_seconds}s 后再重试当前任务")


def _prompt_recoverable_resume(action: str, error: RecoverableInterruption) -> str:
    """为可恢复中断提供人工恢复窗口。"""
    logger.warning(f"{action} 遇到可恢复中断: {error}")
    if error.checkpoint_path:
        logger.info(f"已保存断点: {error.checkpoint_path}")
    if error.context:
        logger.info(f"恢复上下文: {error.context}")

    while True:
        user_input = input(
            f"[恢复窗口] {action} 已暂停。修复网络/页面后按 Enter 重试，"
            "输入 s 安全停止并保留断点，输入 q 终止并关闭连接："
        ).strip().lower()
        if user_input in {"q", "quit", "exit"}:
            return "quit"
        if user_input in {"s", "stop"}:
            return "stop"
        return "retry"


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


def crawl_hotel_list(region: Optional[str] = None, all_regions: bool = False):
    """爬取酒店列表

    Args:
        region: 指定功能区
        all_regions: 是否爬取所有功能区
    """
    logger.info("开始爬取酒店列表")

    anti_crawler = AntiCrawler()
    crawler = HotelListCrawler(anti_crawler)
    should_close = True

    try:
        anti_crawler.init_browser()
        while True:
            try:
                if all_regions:
                    hotels = crawler.crawl_all_regions(save_to_db=True)
                    logger.info(f"全部爬取完成，共 {len(hotels)} 家酒店")
                elif region:
                    if region not in GUANGZHOU_REGIONS:
                        logger.error(f"未知的功能区: {region}")
                        logger.info(f"可用的功能区: {list(GUANGZHOU_REGIONS.keys())}")
                        return EXIT_FAILURE

                    hotels = crawler.crawl_region(region, save_to_db=True)
                    logger.info(f"功能区 {region} 爬取完成，共 {len(hotels)} 家酒店")
                else:
                    logger.error("请指定功能区 (--region) 或使用 --all 爬取所有功能区")
                    return EXIT_FAILURE
                break
            except RecoverableInterruption as e:
                decision = _prompt_recoverable_resume("酒店列表爬取", e)
                if decision == "retry":
                    anti_crawler.init_browser()
                    continue
                should_close = decision == "quit"
                return EXIT_FAILURE

    except KeyboardInterrupt:
        logger.warning("用户中断爬取")
        return EXIT_FAILURE
    except CaptchaCooldownException as e:
        _log_captcha_cooldown_stop("酒店列表爬取", e)
        return EXIT_CAPTCHA_COOLDOWN
    except CaptchaException as e:
        _log_retryable_captcha_stop("酒店列表爬取", e)
        return EXIT_FAILURE
    except Exception as e:
        logger.error(f"爬取失败: {e}")
        return EXIT_FAILURE
    finally:
        if should_close:
            anti_crawler.close()

    return EXIT_SUCCESS


def enrich_hotel_details():
    """补充酒店详细信息"""
    logger.info("开始补充酒店详细信息")
    
    anti_crawler = AntiCrawler()
    crawler = HotelListCrawler(anti_crawler)
    should_close = True
    
    try:
        anti_crawler.init_browser()
        while True:
            try:
                enriched_count = crawler.enrich_hotel_details()
                logger.info(f"补充完成，成功更新 {enriched_count} 家酒店的详细信息")
                break
            except RecoverableInterruption as e:
                decision = _prompt_recoverable_resume("酒店详情补充", e)
                if decision == "retry":
                    anti_crawler.init_browser()
                    continue
                should_close = decision == "quit"
                return EXIT_FAILURE
        
    except KeyboardInterrupt:
        logger.warning("用户中断补充")
        return EXIT_FAILURE
    except CaptchaCooldownException as e:
        _log_captcha_cooldown_stop("酒店详情补充", e)
        return EXIT_CAPTCHA_COOLDOWN
    except CaptchaException as e:
        _log_retryable_captcha_stop("酒店详情补充", e)
        return EXIT_FAILURE
    except Exception as e:
        logger.error(f"补充失败: {e}")
        return EXIT_FAILURE
    finally:
        if should_close:
            anti_crawler.close()

    return EXIT_SUCCESS


def crawl_reviews(
    hotel_id: Optional[str] = None,
    all_hotels: bool = False,
    positive_manual: Optional[bool] = None,
):
    """爬取评论

    Args:
        hotel_id: 指定酒店ID
        all_hotels: 是否爬取所有酒店的评论
    """
    logger.info("开始爬取评论")

    anti_crawler = AntiCrawler()
    should_close = True
    resolved_positive_manual = (
        False
        if all_hotels and positive_manual is None
        else (
            getattr(settings, "review_positive_manual_default_enabled", True)
            if positive_manual is None
            else positive_manual
        )
    )
    crawler = ReviewCrawler(anti_crawler, positive_manual=resolved_positive_manual)
    try:
        anti_crawler.init_browser()
        while True:
            try:
                if hotel_id:
                    reviews = crawler.crawl_hotel_reviews(hotel_id, save_to_db=True)
                    logger.info(f"酒店 {hotel_id} 评论爬取完成，共 {len(reviews)} 条")

                elif all_hotels:
                    logger.info(
                        f"批量评论采集启动: positive_manual={resolved_positive_manual}, "
                        "将按单酒店弹性配额执行"
                    )
                    with session_scope() as session:
                        hotels = session.query(Hotel).filter(
                            Hotel.review_count >= settings.min_reviews_threshold
                        ).order_by(Hotel.review_count.desc(), Hotel.hotel_id.asc()).all()

                    logger.info(f"批量评论采集酒店数量: {len(hotels)}")

                    completed = 0
                    failed = 0
                    skipped = 0
                    hotel_summaries = []

                    for hotel in tqdm(hotels, desc="爬取评论"):
                        current_hotel_id = str(hotel.hotel_id)
                        try:
                            reviews = crawler.crawl_hotel_reviews(current_hotel_id, save_to_db=True)
                            crawl_summary = dict(getattr(crawler, "last_crawl_summary", {}) or {})
                            if not crawl_summary:
                                actual_negative = sum(
                                    1 for item in reviews if str(item.get("source_pool") or "").lower() == "negative"
                                )
                                actual_positive = sum(
                                    1 for item in reviews if str(item.get("source_pool") or "").lower() == "positive"
                                )
                                actual_total = len(reviews)
                                crawl_summary = {
                                    "hotel_id": current_hotel_id,
                                    "review_count": int(getattr(hotel, "review_count", 0) or 0),
                                    "target_total": actual_total,
                                    "target_negative": actual_negative,
                                    "target_positive": actual_positive,
                                    "actual_total": actual_total,
                                    "actual_negative": actual_negative,
                                    "actual_positive": actual_positive,
                                    "actual_negative_ratio": round(actual_negative / actual_total, 4) if actual_total else 0.0,
                                    "candidate_total": actual_total,
                                    "accepted_total": actual_total,
                                    "filtered_quality_total": 0,
                                    "high_quality_count": 0,
                                    "short_comment_count": 0,
                                    "specific_short_count": 0,
                                    "short_comment_ratio": 0.0,
                                    "high_quality_ratio": 0.0,
                                    "quality_relaxed_used": False,
                                    "quality_fallback_used": False,
                                }
                            hotel_summaries.append(crawl_summary)
                            if reviews:
                                completed += 1
                            else:
                                skipped += 1
                        except RecoverableInterruption as e:
                            decision = _prompt_recoverable_resume(f"评论爬取[{current_hotel_id}]", e)
                            if decision == "retry":
                                anti_crawler.init_browser()
                                reviews = crawler.crawl_hotel_reviews(current_hotel_id, save_to_db=True)
                                crawl_summary = dict(getattr(crawler, "last_crawl_summary", {}) or {})
                                if crawl_summary:
                                    hotel_summaries.append(crawl_summary)
                                if reviews:
                                    completed += 1
                                else:
                                    skipped += 1
                                continue
                            should_close = decision == "quit"
                            return EXIT_FAILURE
                        except CaptchaCooldownException as e:
                            logger.warning(
                                f"酒店 {current_hotel_id} 因验证码冷却停止，需等待至少 {e.cooldown_seconds}s 后再继续"
                            )
                            return EXIT_CAPTCHA_COOLDOWN
                        except CaptchaException as e:
                            failed += 1
                            logger.error(f"酒店 {current_hotel_id} 验证码失败，可立即重试: {e}")
                        except Exception as e:
                            failed += 1
                            logger.exception(f"酒店 {current_hotel_id} 失败: {e}")

                        anti_crawler.random_delay(3, 6)

                    total_negative = sum(int(item.get("actual_negative", 0) or 0) for item in hotel_summaries)
                    total_positive = sum(int(item.get("actual_positive", 0) or 0) for item in hotel_summaries)
                    total_actual = sum(int(item.get("actual_total", 0) or 0) for item in hotel_summaries)
                    total_target_negative = sum(int(item.get("target_negative", 0) or 0) for item in hotel_summaries)
                    filtered_quality_total = sum(int(item.get("filtered_quality_total", 0) or 0) for item in hotel_summaries)
                    high_quality_total = sum(int(item.get("high_quality_count", 0) or 0) for item in hotel_summaries)
                    short_comment_total = sum(int(item.get("short_comment_count", 0) or 0) for item in hotel_summaries)
                    specific_short_total = sum(int(item.get("specific_short_count", 0) or 0) for item in hotel_summaries)
                    negative_ratio = round(total_negative / total_actual, 4) if total_actual else 0.0
                    high_quality_ratio = round(high_quality_total / total_actual, 4) if total_actual else 0.0
                    short_comment_ratio = round(short_comment_total / total_actual, 4) if total_actual else 0.0
                    unmet_negative_target_hotels = sum(
                        1
                        for item in hotel_summaries
                        if int(item.get("actual_negative", 0) or 0) < int(item.get("target_negative", 0) or 0)
                    )
                    low_negative_hotels = sum(
                        1 for item in hotel_summaries if int(item.get("actual_negative", 0) or 0) < 10
                    )
                    quality_relaxed_hotels = sum(
                        1 for item in hotel_summaries if bool(item.get("quality_relaxed_used"))
                    )
                    quality_fallback_hotels = sum(
                        1 for item in hotel_summaries if bool(item.get("quality_fallback_used"))
                    )

                    batch_report = {
                        "generated_at": datetime.now().isoformat(timespec="seconds"),
                        "positive_manual": bool(resolved_positive_manual),
                        "completed": completed,
                        "skipped": skipped,
                        "failed": failed,
                        "hotel_count": len(hotels),
                        "totals": {
                            "actual_total": total_actual,
                            "actual_positive": total_positive,
                            "actual_negative": total_negative,
                            "actual_negative_ratio": negative_ratio,
                            "target_negative": total_target_negative,
                            "unmet_negative_target_hotels": unmet_negative_target_hotels,
                            "low_negative_hotels": low_negative_hotels,
                            "filtered_quality_total": filtered_quality_total,
                            "high_quality_total": high_quality_total,
                            "short_comment_total": short_comment_total,
                            "specific_short_total": specific_short_total,
                            "high_quality_ratio": high_quality_ratio,
                            "short_comment_ratio": short_comment_ratio,
                            "quality_relaxed_hotels": quality_relaxed_hotels,
                            "quality_fallback_hotels": quality_fallback_hotels,
                        },
                        "hotels": hotel_summaries,
                    }
                    report_path = _write_review_batch_report(batch_report)

                    logger.info(
                        f"批量评论采集完成: total={len(hotels)}, completed={completed}, "
                        f"skipped={skipped}, failed={failed}, "
                        f"negative={total_negative}, positive={total_positive}, "
                        f"negative_ratio={negative_ratio:.2%}, high_quality_ratio={high_quality_ratio:.2%}, "
                        f"short_comment_ratio={short_comment_ratio:.2%}, report={report_path}"
                    )

                else:
                    logger.error("请指定酒店ID (--hotel-id) 或使用 --all 爬取所有酒店的评论")
                    return EXIT_FAILURE
                break
            except RecoverableInterruption as e:
                decision = _prompt_recoverable_resume("评论爬取", e)
                if decision == "retry":
                    anti_crawler.init_browser()
                    continue
                should_close = decision == "quit"
                return EXIT_FAILURE

    except KeyboardInterrupt:
        logger.warning("用户中断爬取")
        return EXIT_FAILURE
    except CaptchaCooldownException as e:
        _log_captcha_cooldown_stop("评论爬取", e)
        return EXIT_CAPTCHA_COOLDOWN
    except CaptchaException as e:
        _log_retryable_captcha_stop("评论爬取", e)
        return EXIT_FAILURE
    except Exception as e:
        logger.exception(f"爬取失败: {e}")
        return EXIT_FAILURE
    finally:
        if should_close:
            anti_crawler.close()

    return EXIT_SUCCESS


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
  python main.py --mode reviews --hotel-id 10019773   # 负向自动 + 正向人工辅助
  python main.py --mode reviews --hotel-id 10019773 --negative-only  # 仅负向自动采集
  python main.py --mode reviews --all                 # 批量酒店负向自动采集
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
        "--positive-manual",
        action="store_true",
        help="启用正向评论人工辅助采集模式（单酒店时会提示手工翻页）",
    )
    parser.add_argument(
        "--negative-only",
        action="store_true",
        help="仅采集负向评论池，跳过正向人工辅助采集",
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
        return EXIT_SUCCESS

    elif args.mode == "hotel_list":
        return crawl_hotel_list(region=args.region, all_regions=args.all)

    elif args.mode == "enrich_details":
        return enrich_hotel_details()

    elif args.mode == "reviews":
        return crawl_reviews(
            hotel_id=args.hotel_id,
            all_hotels=args.all,
            positive_manual=(False if args.negative_only else (True if args.positive_manual else None)),
        )

    return EXIT_FAILURE


if __name__ == "__main__":
    sys.exit(main())
