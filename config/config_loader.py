"""Configuration loader for unzip service."""
import os
from pathlib import Path
from typing import Dict, Any

import yaml

from config.config import PathConfig, MoveConfig
from src.dao.base import DatabaseType

class ConfigLoader:
    """Configuration loader for unzip service."""
    
    def __init__(self, config_path: str = None):
        """Initialize configuration loader.
        
        Args:
            config_path: Path to configuration file
        """
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        self.config_path = config_path
        self._config = None
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file.
        
        Returns:
            Configuration dictionary
        """
        if self._config is None:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
        return self._config
        
    def get_service_config(self, service: str) -> Dict[str, Any]:
        """Get configuration for specific service.
        
        Args:
            service: Service name
            
        Returns:
            Service configuration
            
        Raises:
            KeyError: If service not found in configuration
        """
        config = self.load_config()
        if service not in config['services']:
            raise KeyError(f"Service {service} not found in configuration")
        return config['services'][service]
        
    def get_path_config(self, service: str) -> PathConfig:
        """Get path configuration for service.
        
        Args:
            service: Service name
            
        Returns:
            PathConfig object
        """
        service_config = self.get_service_config(service)
        paths = service_config['paths']
        move_config = MoveConfig(
            skip_parent_levels=service_config.get('move_config', {}).get('skip_parent_levels', 0)
        )
        
        return PathConfig(
            source_dir=Path(paths['source_dir']),
            target_dir=Path(paths['target_dir']),
            unzip_temp_dir=Path(paths['unzip_temp_dir']),
            target_temp_dir=Path(paths['target_temp_dir']),
            move_config=move_config
        )
        
    def get_db_type(self, service: str) -> DatabaseType:
        """Get database type for service.
        
        Args:
            service: Service name
            
        Returns:
            DatabaseType enum value
        """
        service_config = self.get_service_config(service)
        return DatabaseType[service_config['db_type'].upper()]
        
    def get_default_password(self, service: str) -> str:
        """Get default password for service.
        
        Args:
            service: Service name
            
        Returns:
            Default password
        """
        service_config = self.get_service_config(service)
        return service_config['default_password'] 