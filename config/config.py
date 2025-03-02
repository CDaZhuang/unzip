"""
Configuration module for the unzip tool.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

@dataclass(frozen=True)
class MongoConfig:
    """MongoDB connection configuration."""
    host: str = "192.168.31.196"
    port: int = 27017
    
    @property
    def uri(self) -> str:
        """Get MongoDB connection URI."""
        return f"mongodb://{self.host}:{self.port}"

@dataclass(frozen=True)
class MoveConfig:
    """Configuration for file moving behavior."""
    skip_parent_levels: int = 0  # 要跳过的父文件夹层数

@dataclass(frozen=True)
class PathConfig:
    """Path configuration for different operations."""
    source_dir: Path
    unzip_temp_dir: Path
    target_temp_dir: Path
    target_dir: Path
    move_config: MoveConfig = MoveConfig()

    def __post_init__(self):
        """Ensure all paths are Path objects."""
        object.__setattr__(self, 'source_dir', Path(self.source_dir))
        object.__setattr__(self, 'unzip_temp_dir', Path(self.unzip_temp_dir))
        object.__setattr__(self, 'target_temp_dir', Path(self.target_temp_dir))
        object.__setattr__(self, 'target_dir', Path(self.target_dir))

@dataclass(frozen=True)
class ArchiveConfig:
    """Archive processing configuration."""
    # File headers for different archive types (in hex)
    SEVEN_ZIP_HEADER: str = '377abcaf271c'
    ZIP_HEADER: str = '504b03'
    RAR_HEADER: str = '526172'
    
    # Number of bytes to read for header detection
    HEADER_BYTES: int = 20
    
    # Multi-part archive extensions
    SEVEN_ZIP_PART: str = '.7z.001'
    ZIP_PART: str = '.zip.001'
    RAR_PART: str = '.part001.rar'

# Default configurations for different services
DEFAULT_CONFIGS: Dict[str, PathConfig] = {
    'vam': PathConfig(
        source_dir=Path(r"I:\ACG\vam-download"),
        unzip_temp_dir=Path(r"I:\ACG\vam-temp\Temp1"),
        # temp_dir_2=Path(r"I:\ACG\vam-temp\Temp2"),
        target_temp_dir=Path(r"I:\ACG\vam-temp\Temp3"),
        target_dir=Path(r"I:\ACG\vam")
    )
}

# MongoDB configuration
MONGO_CONFIG = MongoConfig()

# Archive configuration
ARCHIVE_CONFIG = ArchiveConfig() 