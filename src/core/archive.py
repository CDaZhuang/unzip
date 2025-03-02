"""
Core archive handling functionality.
"""
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict

import py7zr
import pyzipper
import rarfile

from config.config import ArchiveConfig
from src.utils.file_helper import FileHelper

logger = logging.getLogger(__name__)

class ArchiveType(Enum):
    """Archive type enumeration."""
    SEVEN_ZIP = '7z'
    ZIP = 'zip'
    RAR = 'rar'
    UNKNOWN = 'unknown'

@dataclass
class ArchiveInfo:
    """Information about an archive file."""
    path: Path
    type: ArchiveType
    parts: List[Path]
    password: str = ""

class ArchiveHandler:
    """Handler for archive operations."""

    @staticmethod
    def detect_archive_type(file_path: Path) -> ArchiveType:
        """Detect archive type from file header.
        
        Args:
            file_path: Path to archive file
            
        Returns:
            Detected archive type
        """
        header = FileHelper.get_file_header(file_path)
        if not header:
            return ArchiveType.UNKNOWN
            
        if header.startswith(ArchiveConfig.SEVEN_ZIP_HEADER):
            return ArchiveType.SEVEN_ZIP
        elif header.startswith(ArchiveConfig.ZIP_HEADER):
            return ArchiveType.ZIP
        elif header.startswith(ArchiveConfig.RAR_HEADER):
            return ArchiveType.RAR
        return ArchiveType.UNKNOWN

    @staticmethod
    def extract_7z(archive_path: Path, output_dir: Path, password: str) -> None:
        """Extract 7z archive.
        
        Args:
            archive_path: Path to archive
            output_dir: Output directory
            password: Archive password
        """
        logger.info(f"Extracting 7z archive {archive_path} to {output_dir}")
        with py7zr.SevenZipFile(archive_path, mode='r', password=password) as z:
            z.extractall(path=output_dir)

    @staticmethod
    def extract_zip(archive_path: Path, output_dir: Path, password: str) -> None:
        """Extract ZIP archive.
        
        Args:
            archive_path: Path to archive
            output_dir: Output directory
            password: Archive password
        """
        logger.info(f"Extracting ZIP archive {archive_path} to {output_dir}")
        with pyzipper.AESZipFile(archive_path, 'r', 
                                compression=pyzipper.ZIP_DEFLATED,
                                encryption=pyzipper.WZ_AES) as z:
            z.extractall(pwd=str.encode(password), path=output_dir)

    @staticmethod
    def extract_rar(archive_path: Path, output_dir: Path, password: str) -> None:
        """Extract RAR archive.
        
        Args:
            archive_path: Path to archive
            output_dir: Output directory
            password: Archive password
        """
        logger.info(f"Extracting RAR archive {archive_path} to {output_dir}")
        with rarfile.RarFile(archive_path) as z:
            z.extractall(output_dir, pwd=password)

    def extract_archive(self, archive_info: ArchiveInfo, output_dir: Path) -> None:
        """Extract archive based on its type.
        
        Args:
            archive_info: Archive information
            output_dir: Output directory
        """
        if not output_dir.exists():
            FileHelper.make_directory(output_dir)
        
        extract_funcs = {
            ArchiveType.SEVEN_ZIP: self.extract_7z,
            ArchiveType.ZIP: self.extract_zip,
            ArchiveType.RAR: self.extract_rar
        }
        
        if archive_info.type not in extract_funcs:
            logger.error(f"Unsupported archive type: {archive_info.type}")
            return

        try:
            # First try: direct extraction from first part
            if len(archive_info.parts) > 1:
                logger.info(f"Attempting to extract multi-part archive ({len(archive_info.parts)} parts) directly")
                archive_path = archive_info.parts[0]
            else:
                archive_path = archive_info.path
                
            logger.info(f"Using archive file: {archive_path}")
            try:
                logger.info(f"Extracting {archive_info.type.value} archive")
                extract_funcs[archive_info.type](archive_path, output_dir, archive_info.password)
                logger.info("Direct extraction successful")
                return
            except Exception as e:
                if len(archive_info.parts) <= 1:
                    raise
                logger.warning(f"Direct extraction failed: {e}, trying with merged file")
                
            # Second try: merge parts and extract
            logger.info("Merging archive parts...")
            merged_path = output_dir / f"merged{archive_info.path.suffix}"
            try:
                FileHelper.merge_files(archive_info.parts, merged_path)
                logger.info(f"Merged file created at: {merged_path}")
                
                logger.info(f"Extracting from merged file")
                extract_funcs[archive_info.type](merged_path, output_dir, archive_info.password)
                logger.info("Extraction from merged file successful")
            finally:
                # Clean up merged file
                if merged_path.exists():
                    logger.info(f"Removing merged file: {merged_path}")
                    merged_path.unlink()
                
        except Exception as e:
            logger.error(f"Failed to extract archive {archive_info.path}: {e}")
            raise 