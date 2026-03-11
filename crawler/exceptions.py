"""爬虫异常定义模块

定义验证码处理、导航、数据提取等爬虫操作中的异常类。
支持异常分类和状态转移决策。
"""


class CrawlerException(Exception):
    """爬虫基础异常类
    
    所有爬虫异常的基类，用于统一捕获和处理。
    """
    pass


class CaptchaException(CrawlerException):
    """验证码异常基类
    
    表示在验证码处理过程中发生的可恢复错误。
    调用者应该重试当前操作。
    
    Attributes:
        retry_count: 已重试次数
        max_retries: 最大重试次数
        message: 错误信息
    """
    
    def __init__(self, message: str, retry_count: int = 0, max_retries: int = 3):
        """初始化验证码异常
        
        Args:
         message: 错误描述信息
        retry_count: 当前重试次数
          max_retries: 最大允许重试次数
        """
        self.message = message
        self.retry_count = retry_count
        self.max_retries = max_retries
        super().__init__(
            f"{message} (重试 {retry_count}/{max_retries})"
        )
    
    def should_retry(self) -> bool:
        """判断是否应该重试
        
        Returns:
            True 如果还有重试次数，False 如果已达到最大重试次数
        """
        return self.retry_count < self.max_retries


class CaptchaDetectedException(CaptchaException):
    """检测到验证码异常
    
    表示页面上检测到验证码元素，需要处理。
    这是可重试的异常。
    
    Attributes:
        selector: 检测到的验证码选择器
    """
    
    def __init__(
        self,
        selector: str,
     retry_count: int = 0,
        max_retries: int = 3
    ):
        """初始化验证码检测异常
        
        Args:
        selector: 验证码元素的CSS选择器
            retry_count: 当前重试次数
            max_retries: 最大重试次数
        """
        self.selector = selector
        message = f"检测到验证码: {selector}"
        super().__init__(message, retry_count, max_retries)


class CaptchaAutoSlideFailed(CaptchaException):
    """自动滑块验证失败异常
    
    表示自动滑块验证码处理失败。
    可能需要人工介入或重试。
    """
    
    def __init__(
        self,
        reason: str = "滑块验证失败",
        retry_count: int = 0,
        max_retries: int = 3
    ):
        """初始化自动滑块失败异常
        
        Args:
            reason: 失败原因
            retry_count: 当前重试次数
            max_retries: 最大重试次数
        """
        self.reason = reason
        super().__init__(reason, retry_count, max_retries)


class CaptchaTimeoutException(CaptchaException):
    """验证码超时异常
    
    表示验证码处理超时或验证页面已过期。
    这是一个可恢复的异常，但需要重新导航。
    
    Attributes:
        timeout_seconds: 超时时长（秒）
        elapsed_seconds: 已经过的时长（秒）
    """
  
    def __init__(
        self,
        timeout_seconds: int = 300,
        elapsed_seconds: int = 0,
        retry_count: int = 0,
        max_retries: int = 3
    ):
        """初始化验证码超时异常
        
        Args:
            timeout_seconds: 设定的超时时长
            elapsed_seconds: 实际已经过的时长
        retry_count: 当前重试次数
        max_retries: 最大重试次数
        """
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds
        message = (
            f"验证码处理超时 "
            f"(已等待 {elapsed_seconds}s, 超时设置 {timeout_seconds}s)"
        )
        super().__init__(message, retry_count, max_retries)


class CaptchaManualInterventionRequired(CaptchaException):
    """需要人工介入异常
    
    表示自动处理失败，需要用户手动完成验证。
    这是可重试的异常。
    """
    
    def __init__(
        self,
        reason: str = "自动验证失败，需要人工处理",
        retry_count: int = 0,
        max_retries: int = 1
    ):
        """初始化人工介入异常
        
        Args:
            reason: 需要人工介入的原因
            retry_count: 当前重试次数
         max_retries: 最大重试次数（通常为1）
        """
        self.reason = reason
        super().__init__(reason, retry_count, max_retries)


class CaptchaCooldownException(CrawlerException):
    """验证码冷却异常（终止异常）
    
    表示因为验证码失败次数过多，系统进入冷却期。
    这是一个终止异常，调用者应该停止当前任务。
    
    不继承自 CaptchaException，因为这不是可重试的。
    
    Attributes:
      cooldown_seconds: 冷却时长（秒）
     reason: 进入冷却的原因
    """
    
    def __init__(
        self,
        cooldown_seconds: int = 3600,
        reason: str = "验证码失败次数过多，进入冷却期"
    ):
        """初始化冷却异常
        
     Args:
            cooldown_seconds: 冷却时长（秒）
            reason: 进入冷却的原因
        """
        self.cooldown_seconds = cooldown_seconds
        self.reason = reason
        message = f"{reason} (冷却时长: {cooldown_seconds}s)"
        super().__init__(message)
    
    def should_retry(self) -> bool:
        """冷却异常不应该重试
        
        Returns:
            始终返回 False
        """
        return False


class NavigationException(CrawlerException):
    """导航异常
    
    表示页面导航失败。
    """
    
    def __init__(self, url: str, reason: str):
        """初始化导航异常
        
     Args:
          url: 尝试导航的URL
            reason: 导航失败的原因
        """
        self.url = url
        self.reason = reason
        message = f"导航失败: {url} - {reason}"
        super().__init__(message)


class DataExtractionException(CrawlerException):
    """数据提取异常
    
    表示从页面提取数据失败。
    """
    
    def __init__(self, element_type: str, reason: str):
        """初始化数据提取异常
        
        Args:
            element_type: 尝试提取的元素类型（如 'hotel', 'review'）
            reason: 提取失败的原因
        """
        self.element_type = element_type
        self.reason = reason
        message = f"提取 {element_type} 数据失败: {reason}"
        super().__init__(message)


class BrowserConnectionException(CrawlerException):
    """浏览器连接异常
    
    表示与Chrome浏览器的连接失败或中断。
    """
    
    def __init__(self, reason: str):
        """初始化浏览器连接异常
      
        Args:
            reason: 连接失败的原因
        """
        self.reason = reason
        message = f"浏览器连接失败: {reason}"
        super().__init__(message)
