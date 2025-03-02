import os
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

@dataclass
class LoggerConfig:
    """日志配置类"""
    log_dir: str = 'logs'  # 日志目录
    backup_count: int = 30  # 保留的日志文件数量
    rotation: str = 'midnight'  # 日志轮转时间
    log_level: int = logging.INFO  # 日志级别
    console_output: bool = True  # 是否输出到控制台
    format_str: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'  # 日志格式

def get_logger(name: str, config: Optional[LoggerConfig] = None) -> logging.Logger:
    """
    获取logger实例
    
    Args:
        name: logger名称
        config: 日志配置，如果为None则使用默认配置
        
    Returns:
        logging.Logger: 配置好的logger实例
    """
    if config is None:
        config = LoggerConfig()
    
    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(config.log_level)
    
    # 如果logger已经有处理器，说明已经被配置过，直接返回
    if logger.handlers:
        return logger
    
    # 创建日志目录
    log_dir = Path(config.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 创建文件处理器
    log_file = log_dir / f'{name}.log'
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when=config.rotation,
        backupCount=config.backup_count,
        encoding='utf-8'
    )
    
    # 设置日志格式
    formatter = logging.Formatter(config.format_str)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 如果需要控制台输出，添加控制台处理器
    if config.console_output:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger

# 使用示例
if __name__ == '__main__':
    # 使用默认配置
    logger1 = get_logger('test_default')
    logger1.info('This is a test message with default config')
    
    # 使用自定义配置
    custom_config = LoggerConfig(
        log_dir='custom_logs',
        backup_count=7,
        rotation='D',
        log_level=logging.DEBUG,
        console_output=True,
        format_str='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger2 = get_logger('test_custom', custom_config)
    logger2.debug('This is a test message with custom config') 