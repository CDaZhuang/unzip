"""
File operation utilities.
"""
import binascii
import logging
import shutil
from pathlib import Path
from typing import List, Optional

from config.config import ArchiveConfig

logger = logging.getLogger(__name__)

class FileHelper:
    """File operation helper utilities."""
    
    @staticmethod
    def make_directory(path: Path) -> None:
        """Create a directory, removing it first if it exists.
        
        Args:
            path: Directory path to create
        """
        if path.exists():
            if path.is_file():
                path.unlink()
            else:
                shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def contains_files(directory: Path) -> bool:
        """Check if directory contains any files.
        
        Args:
            directory: Directory to check
            
        Returns:
            True if directory contains files, False otherwise
        """
        if not directory.exists() or not directory.is_dir():
            return False
        
        return any(item.is_file() for item in directory.rglob('*'))

    @staticmethod
    def move_files(src_dir: Path, dst_dir: Path) -> None:
        """Move all files from source directory to destination directory.
        
        Args:
            src_dir: Source directory
            dst_dir: Destination directory
        """
        if not FileHelper.contains_files(src_dir):
            return

        FileHelper.make_directory(dst_dir)
        for file_path in src_dir.rglob('*'):
            if file_path.is_file():
                try:
                    shutil.move(str(file_path), str(dst_dir))
                    logger.info(f'Moved file {file_path} to {dst_dir}')
                except Exception as e:
                    logger.error(f"Failed to move {file_path}: {e}")

    @staticmethod
    def get_file_list(directory: Path) -> List[Path]:
        """Get list of all files in directory and subdirectories.
        
        Args:
            directory: Directory to scan
            
        Returns:
            List of file paths
        """
        return [f for f in directory.rglob('*') if f.is_file()]

    @staticmethod
    def get_file_header(file_path: Path, bytes_to_read: int = ArchiveConfig.HEADER_BYTES) -> Optional[str]:
        """Read and return file header as hex string.
        
        Args:
            file_path: Path to file
            bytes_to_read: Number of bytes to read
            
        Returns:
            Hex string of file header or None if read fails
        """
        try:
            with open(file_path, 'rb') as f:
                header = f.read(bytes_to_read)
            return binascii.b2a_hex(header).decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to read header from {file_path}: {e}")
            return None

    @staticmethod
    def merge_files(file_list: List[Path], output_path: Path, chunk_size: int = 1024 * 1024) -> None:
        """Merge multiple files into one.
        
        Args:
            file_list: List of files to merge
            output_path: Output file path
            chunk_size: Size of chunks to read/write
        """
        try:
            with open(output_path, 'wb') as out_file:
                for file_path in sorted(file_list):
                    with open(file_path, 'rb') as in_file:
                        while chunk := in_file.read(chunk_size):
                            out_file.write(chunk)
                    logger.info(f"Merged {file_path}")
        except Exception as e:
            logger.error(f"Failed to merge files to {output_path}: {e}")
            raise 