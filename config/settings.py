"""全局配置管理"""
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """应用配置类，支持从.env文件读取配置"""

    # 数据库配置
    db_host: str = Field(default="localhost", alias="DB_HOST")
    db_port: int = Field(default=5432, alias="DB_PORT")
    db_name: str = Field(default="hotel_reviews", alias="DB_NAME")
    db_user: str = Field(default="postgres", alias="DB_USER")
    db_password: str = Field(default="", alias="DB_PASSWORD")

    # Chrome调试配置
    chrome_debug_port: int = Field(default=9222, alias="CHROME_DEBUG_PORT")
    chrome_user_data_dir: str = Field(
        default="C:/selenium/automation_profile",
        alias="CHROME_USER_DATA_DIR"
    )

    # 爬虫配置
    min_delay: float = Field(default=3.0, alias="MIN_DELAY")
    max_delay: float = Field(default=6.0, alias="MAX_DELAY")
    max_retries: int = Field(default=3, alias="MAX_RETRIES")
    request_timeout: int = Field(default=30, alias="REQUEST_TIMEOUT")

    # 评论采集配置
    max_reviews_per_hotel: int = Field(default=300, alias="MAX_REVIEWS_PER_HOTEL")
    min_reviews_threshold: int = Field(default=200, alias="MIN_REVIEWS_THRESHOLD")

    # 日志配置
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_dir: str = Field(default="logs", alias="LOG_DIR")

    # 项目路径
    base_dir: Path = Path(__file__).parent.parent

    @property
    def database_url(self) -> str:
        """获取数据库连接URL"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def chrome_address(self) -> str:
        """获取Chrome调试地址"""
        return f"127.0.0.1:{self.chrome_debug_port}"

    @property
    def log_path(self) -> Path:
        """获取日志目录路径"""
        return self.base_dir / self.log_dir

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


# 全局配置实例
settings = Settings()
