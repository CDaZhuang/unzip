"""
Main entry point for the unzip tool.
"""
import logging
import os
from pathlib import Path

from config.config_loader import ConfigLoader
from src.dao.base import DatabaseType
from src.core.unzip_service import UnzipService
from src.utils.logger import get_logger

def main():
    """Main entry point."""
    logger = get_logger(__name__)
    config_loader = ConfigLoader()
    config = config_loader.load_config()
    
    # Process each configured service
    for service in config['services']:
        logger.info(f"Processing service: {service}")
        
        try:
            # Get service configuration
            path_config = config_loader.get_path_config(service)
            db_type = config_loader.get_db_type(service)
            default_password = config_loader.get_default_password(service)
            
            # Initialize and run service
            unzip_service = UnzipService(
                db_type=db_type,
                default_password=default_password,
                path_config=path_config
            )
            
            logger.info(f"Starting archive processing for {service}...")
            unzip_service.process_archives()
            
            logger.info("Cleaning up...")
            unzip_service.cleanup()
            
            logger.info(f"Processing completed successfully for {service}")
            
        except Exception as e:
            logger.error(f"Processing failed for {service}: {e}", exc_info=True)
            continue  # Continue with next service even if one fails

if __name__ == '__main__':
    main() 