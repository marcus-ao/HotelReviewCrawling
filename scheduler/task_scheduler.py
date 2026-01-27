"""任务调度器模块

管理爬取任务的创建、执行和状态跟踪。
"""
import uuid
from datetime import datetime
from typing import Optional, Generator

from config.regions import GUANGZHOU_REGIONS
from utils.logger import get_logger
from database.connection import session_scope
from database.models import Hotel, CrawlTask, CrawlLog

logger = get_logger("task_scheduler")


class TaskScheduler:
    """任务调度器类"""

    def __init__(self):
        self.current_task_id = None

    def create_hotel_list_tasks(self) -> list[str]:
        """创建酒店列表爬取任务

        为每个功能区的每个商圈和价格档次创建任务

        Returns:
            创建的任务ID列表
        """
        task_ids = []

        with session_scope() as session:
            for region_type, region_config in GUANGZHOU_REGIONS.items():
                for zone in region_config['business_zones']:
                    for price_range in region_config['price_ranges']:
                        task_id = str(uuid.uuid4())[:8]

                        task = CrawlTask(
                            task_id=task_id,
                            task_type="hotel_list",
                            region_type=region_type,
                            business_zone_code=zone['code'],
                            price_level=price_range['level'],
                            status="pending",
                            priority=self._calculate_priority(region_type, price_range['level']),
                        )
                        session.add(task)
                        task_ids.append(task_id)

            logger.info(f"创建了 {len(task_ids)} 个酒店列表爬取任务")

        return task_ids

    def create_review_tasks(self) -> list[str]:
        """创建评论爬取任务

        为数据库中所有符合条件的酒店创建评论爬取任务

        Returns:
            创建的任务ID列表
        """
        task_ids = []

        with session_scope() as session:
            # 获取所有需要爬取评论的酒店
            hotels = session.query(Hotel).filter(
                Hotel.review_count >= 50  # 评论数大于50的酒店
            ).all()
    
            for hotel in hotels:
                # 检查是否已有任务
                from sqlalchemy import or_
                existing = session.query(CrawlTask).filter(
                    CrawlTask.task_type == "review",
                    CrawlTask.hotel_id == hotel.hotel_id,
                    or_(
                        CrawlTask.status == "pending",
                        CrawlTask.status == "in_progress"
                    )
                ).first()
    
                if existing:
                    continue

                task_id = str(uuid.uuid4())[:8]

                task = CrawlTask(
                    task_id=task_id,
                    task_type="review",
                    hotel_id=hotel.hotel_id,
                    region_type=hotel.region_type,
                    business_zone_code=hotel.business_zone_code,
                    status="pending",
                    priority=self._calculate_review_priority(hotel),
                    items_total=min(hotel.review_count or 300, 300),
                )
                session.add(task)
                task_ids.append(task_id)

            logger.info(f"创建了 {len(task_ids)} 个评论爬取任务")

        return task_ids

    def _calculate_priority(self, region_type: str, price_level: str) -> int:
        """计算任务优先级

        Args:
            region_type: 功能区类型
            price_level: 价格档次

        Returns:
            优先级分数（越高越优先）
        """
        # 功能区优先级
        region_priority = {
            "CBD商务区": 10,
            "老城文化区": 9,
            "交通枢纽区": 8,
            "会展活动区": 7,
            "度假亲子区": 6,
            "高校科技区": 5,
        }

        # 价格档次优先级
        price_priority = {
            "舒适型": 4,
            "经济型": 3,
            "高档型": 2,
            "奢华型": 1,
        }

        return region_priority.get(region_type, 0) + price_priority.get(price_level, 0)

    def _calculate_review_priority(self, hotel: Hotel) -> int:
        """计算评论任务优先级

        Args:
            hotel: 酒店对象

        Returns:
            优先级分数
        """
        priority = 0

        # 评论数越多优先级越高
        if hotel.review_count:
            if hotel.review_count > 1000:
                priority += 10
            elif hotel.review_count > 500:
                priority += 8
            elif hotel.review_count > 200:
                priority += 5

        # 评分高的优先
        if hotel.rating_score:
            priority += int(hotel.rating_score)

        return priority

    def get_pending_tasks(
        self,
        task_type: str = None,
        limit: int = 10
    ) -> Generator[CrawlTask, None, None]:
        """获取待执行的任务

        Args:
            task_type: 任务类型筛选
            limit: 返回数量限制

        Yields:
            CrawlTask对象
        """
        with session_scope() as session:
            query = session.query(CrawlTask).filter_by(status="pending")

            if task_type:
                query = query.filter_by(task_type=task_type)

            tasks = query.order_by(
                CrawlTask.priority.desc(),
                CrawlTask.created_at.asc()
            ).limit(limit).all()

            for task in tasks:
                yield task

    def start_task(self, task_id: str) -> bool:
        """开始执行任务

        Args:
            task_id: 任务ID

        Returns:
            是否成功
        """
        with session_scope() as session:
            task = session.query(CrawlTask).filter_by(task_id=task_id).first()

            if not task:
                logger.error(f"任务不存在: {task_id}")
                return False

            if task.status != "pending":
                logger.warning(f"任务状态不是pending: {task_id} ({task.status})")
                return False

            task.status = "in_progress"
            task.started_at = datetime.now()
            self.current_task_id = task_id

            self._log_task(session, task_id, "INFO", f"任务开始执行")
            logger.info(f"任务开始: {task_id}")

            return True

    def complete_task(self, task_id: str, items_crawled: int = 0) -> bool:
        """完成任务

        Args:
            task_id: 任务ID
            items_crawled: 爬取的数量

        Returns:
            是否成功
        """
        with session_scope() as session:
            task = session.query(CrawlTask).filter_by(task_id=task_id).first()

            if not task:
                logger.error(f"任务不存在: {task_id}")
                return False

            task.status = "completed"
            task.completed_at = datetime.now()
            task.items_crawled = items_crawled

            self._log_task(session, task_id, "INFO", f"任务完成，爬取 {items_crawled} 条")
            logger.info(f"任务完成: {task_id}，爬取 {items_crawled} 条")

            self.current_task_id = None
            return True

    def fail_task(self, task_id: str, error_message: str) -> bool:
        """标记任务失败

        Args:
            task_id: 任务ID
            error_message: 错误信息

        Returns:
            是否成功
        """
        with session_scope() as session:
            task = session.query(CrawlTask).filter_by(task_id=task_id).first()

            if not task:
                logger.error(f"任务不存在: {task_id}")
                return False

            task.retry_count += 1

            # 重试次数超过3次则标记为失败
            if task.retry_count >= 3:
                task.status = "failed"
                task.error_message = error_message
                self._log_task(session, task_id, "ERROR", f"任务失败: {error_message}")
                logger.error(f"任务失败: {task_id} - {error_message}")
            else:
                task.status = "pending"
                self._log_task(session, task_id, "WARNING", f"任务重试 ({task.retry_count}/3): {error_message}")
                logger.warning(f"任务重试: {task_id} ({task.retry_count}/3)")

            self.current_task_id = None
            return True

    def skip_task(self, task_id: str, reason: str) -> bool:
        """跳过任务

        Args:
            task_id: 任务ID
            reason: 跳过原因

        Returns:
            是否成功
        """
        with session_scope() as session:
            task = session.query(CrawlTask).filter_by(task_id=task_id).first()

            if not task:
                logger.error(f"任务不存在: {task_id}")
                return False

            task.status = "skipped"
            task.error_message = reason

            self._log_task(session, task_id, "INFO", f"任务跳过: {reason}")
            logger.info(f"任务跳过: {task_id} - {reason}")

            self.current_task_id = None
            return True

    def _log_task(self, session, task_id: str, level: str, message: str, details: dict = None):
        """记录任务日志

        Args:
            session: 数据库会话
            task_id: 任务ID
            level: 日志级别
            message: 日志消息
            details: 详细信息
        """
        log = CrawlLog(
            task_id=task_id,
            level=level,
            message=message,
            details=details,
        )
        session.add(log)

    def get_task_stats(self) -> dict:
        """获取任务统计信息

        Returns:
            统计信息字典
        """
        with session_scope() as session:
            stats = {
                "total": session.query(CrawlTask).count(),
                "pending": session.query(CrawlTask).filter_by(status="pending").count(),
                "in_progress": session.query(CrawlTask).filter_by(status="in_progress").count(),
                "completed": session.query(CrawlTask).filter_by(status="completed").count(),
                "failed": session.query(CrawlTask).filter_by(status="failed").count(),
                "skipped": session.query(CrawlTask).filter_by(status="skipped").count(),
            }

            # 按任务类型统计
            stats["by_type"] = {
                "hotel_list": session.query(CrawlTask).filter_by(task_type="hotel_list").count(),
                "review": session.query(CrawlTask).filter_by(task_type="review").count(),
            }

            return stats

    def reset_failed_tasks(self) -> int:
        """重置失败的任务为待执行状态

        Returns:
            重置的任务数量
        """
        with session_scope() as session:
            count = session.query(CrawlTask).filter_by(status="failed").update({
                "status": "pending",
                "retry_count": 0,
                "error_message": None,
            })

            logger.info(f"重置了 {count} 个失败任务")
            return count
