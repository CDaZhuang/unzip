"""
Core unzip service implementation.
"""
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config.config import PathConfig
from src.dao.base import DatabaseType
from src.dao.models import UnzipHistoryDao, PiecesDao
from src.utils.file_helper import FileHelper
from src.core.archive import ArchiveHandler, ArchiveInfo, ArchiveType

logger = logging.getLogger(__name__)

class UnzipService:
    """Main service for handling archive extraction operations."""

    def __init__(self, db_type: DatabaseType, default_password: str, path_config: PathConfig):
        """Initialize unzip service.
        
        Args:
            db_type: Database type for history tracking
            default_password: Default password for archives
            path_config: Path configuration
        """
        self.db_type = db_type
        self.default_password = default_password
        self.path_config = path_config
        
        # Initialize DAOs
        self.history_dao = UnzipHistoryDao(db_type)
        self.pieces_dao = PiecesDao(db_type)
        
        # Initialize handlers
        self.archive_handler = ArchiveHandler()

    def _get_path_id(self, path: Path) -> str:
        """Extract ID from path.
        
        Args:
            path: Path to extract ID from
            
        Returns:
            Extracted ID
        """
        if path.is_dir():
            return path.name
        return path.stem.split('_')[0]

    def _is_all_processed(self, _id: str) -> bool:
        """Check if all resolutions are processed for video content.
        
        Args:
            _id: Content ID
            
        Returns:
            True if all resolutions are processed
        """
        if self.db_type != DatabaseType.VAMVIDEO:
            return True

        folder_name = self._generate_folder_name(_id)
        folder_path = self.path_config.target_dir / folder_name
        
        if not folder_path.exists():
            return False

        # Resolution mapping
        resolutions = {
            "1k": "1080", "1K": "1080", "1080": "1080",
            "2k": "2k", "2K": "2k",
            "4k": "4k", "4K": "4k"
        }
        
        # Count required resolutions
        required_resolutions = set()
        for res in resolutions:
            if res in folder_name:
                required_resolutions.add(resolutions[res])
        
        if len(required_resolutions) <= 1:
            return True
            
        # Check if all required resolutions exist
        found_resolutions = set()
        for item in folder_path.iterdir():
            for res in required_resolutions:
                if res in item.name:
                    found_resolutions.add(res)
                    logger.info(f'Found resolution {res} in {item.name}')
        
        return found_resolutions == required_resolutions

    def _generate_folder_name(self, _id: str) -> str:
        """Generate folder name from content ID and metadata.
        
        Args:
            _id: Content ID
            
        Returns:
            Generated folder name
        """
        pieces = self.pieces_dao.query_pieces_by_id(_id)
        if not pieces:
            return _id
            
        title = pieces.get('title', pieces.get('loi_title', ''))
        folder_name = f'[{_id}] {title}'
        folder_name = folder_name.replace('/', '_')
        
        if folder_name.endswith('.'):
            folder_name = folder_name[:-1]
            
        return folder_name

    def _collect_archive_files(self) -> Tuple[Dict[str, List[Path]], Dict[str, Path]]:
        """Collect all archive files to process.
        
        Returns:
            Tuple of (id -> file paths mapping, id -> source path mapping)
        """
        id_files = {}
        id_sources = {}
        
        for item in self.path_config.source_dir.iterdir():
            _id = self._get_path_id(item)
            
            # Skip if already processed
            if (self.history_dao.query_history_by_src_path(item) or 
                self.history_dao.query_history_by_id(_id)):
                if self._is_all_processed(_id):
                    logger.info(f"Skipping already processed item: {item}")
                    continue
            
            id_sources[_id] = item
            if item.is_file():
                id_files[_id] = [item]
            else:
                id_files[_id] = list(item.rglob('*'))
        
        logger.info(f"Collected {len(id_sources)} items to process")
        return id_files, id_sources

    def _prepare_archive_info(self, _id: str, files: List[Path]) -> Dict[str, ArchiveInfo]:
        """Prepare archive information for extraction.
        
        Args:
            _id: Content ID
            files: List of files to process
            
        Returns:
            Dictionary mapping base names to archive info
        """
        archives = {}
        files = sorted(files)
        
        for file_path in files:
            base_name = file_path.stem.split('.')[0]
            
            if base_name not in archives:
                archive_type = self.archive_handler.detect_archive_type(file_path)
                if archive_type != ArchiveType.UNKNOWN:
                    archives[base_name] = ArchiveInfo(
                        path=file_path,
                        type=archive_type,
                        parts=[file_path],
                        password=self.pieces_dao.query_pieces_unzip_key(_id) or self.default_password
                    )
            else:
                archives[base_name].parts.append(file_path)
                
        return archives

    def process_archives(self) -> None:
        """Main method to process all archives."""
        # Prepare temporary directories
        for temp_dir in [self.path_config.unzip_temp_dir, self.path_config.target_temp_dir]:
            shutil.rmtree(temp_dir, ignore_errors=True)
            temp_dir.mkdir(parents=True, exist_ok=True)

        # Collect files to process
        id_files, id_sources = self._collect_archive_files()
        
        # Process each ID
        for _id, files in id_files.items():
            try:
                self._process_single_id(_id, files, id_sources[_id])
            except Exception as e:
                logger.error(f"Failed to process ID {_id}: {e}")

    def _is_multipart_archive(self, file_path: Path) -> bool:
        """Check if a file is part of a multi-part archive.
        
        Args:
            file_path: Path to check
            
        Returns:
            True if file is part of a multi-part archive
        """
        file_name = file_path.name.lower()
        return (any(file_name.endswith(f'.7z.{str(i).zfill(3)}') for i in range(1, 999)) or
                any(file_name.endswith(f'.zip.{str(i).zfill(3)}') for i in range(1, 999)) or
                (file_name.startswith('part') and file_name.endswith('.rar')) or
                '.part' in file_name)

    def _extract_to_target_temp(self, archive: ArchiveInfo, temp_path: Path) -> None:
        """Extract archive to temp3 directory."""
        logger.info(f'Starting extraction to {temp_path}')
        if archive.type != ArchiveType.UNKNOWN:
            logger.info(f'Extracting archive {archive.path} (type: {archive.type}) to {temp_path}')
            self.archive_handler.extract_archive(archive, temp_path)
            logger.info(f'Successfully extracted archive {archive.path} to {temp_path}')
        else:
            # Only move non-archive files (skip multi-part archive files)
            for part in archive.parts:
                try:
                    if self._is_multipart_archive(part):
                        logger.info(f'Skipping multi-part archive file: {part}')
                        continue
                            
                    logger.info(f'Copying non-archive file {part} to {temp_path}')
                    shutil.copy2(str(part), str(temp_path))
                    logger.info(f'Successfully copied {part} to {temp_path}')
                except Exception as e:
                    logger.error(f"Failed to copy {part} to {temp_path}: {e}")

    def _move_archives_to_zip_temp(self, temp_path: Path, _id: str) -> None:
        """Move archive files from temp3 to temp1."""
        logger.info(f'Moving archives from {temp_path} to unzip temp directory')
        for file_path in FileHelper.get_file_list(temp_path):
            if (self.archive_handler.detect_archive_type(file_path) != ArchiveType.UNKNOWN or 
                self._is_multipart_archive(file_path)):
                # Calculate relative path to maintain directory structure
                rel_path = file_path.relative_to(temp_path)
                dst_path = self.path_config.unzip_temp_dir / self._generate_folder_name(_id) / rel_path.parent
                
                # Remove existing directory if it exists
                if dst_path.exists():
                    logger.info(f'Removing existing directory: {dst_path}')
                    if dst_path.is_file():
                        dst_path.unlink()
                        logger.info(f'Removed existing file: {dst_path}')
                    else:
                        shutil.rmtree(dst_path)
                        logger.info(f'Removed existing directory: {dst_path}')
                
                logger.info(f'Creating directory: {dst_path}')
                dst_path.mkdir(parents=True, exist_ok=True)
                
                # Move archive file to unzip_temp_dir
                target_file = dst_path / file_path.name
                if target_file.exists():
                    logger.info(f'Removing existing target file: {target_file}')
                    if target_file.is_file():
                        target_file.unlink()
                    else:
                        shutil.rmtree(target_file)
                
                logger.info(f'Moving archive file {file_path} to {target_file}')
                shutil.move(str(file_path), str(target_file))
                logger.info(f'Successfully moved archive file to: {target_file}')

    def _collect_multipart_archive(self, file_path: Path) -> List[Path]:
        """Collect all parts of a multi-part archive.
        
        Args:
            file_path: Path to first archive part
            
        Returns:
            List of paths to all archive parts
        """
        file_name = file_path.name.lower()
        if '.7z.' in file_name:
            base_name = file_name[:file_name.rindex('.7z.')]
        elif '.zip.' in file_name:
            base_name = file_name[:file_name.rindex('.zip.')]
        elif '.part' in file_name and file_name.endswith('.rar'):
            base_name = file_name[:file_name.index('.part')]
        else:
            base_name = file_name[:file_name.rindex('.part')]
            
        parent_dir = file_path.parent
        # Collect all parts of the multi-part archive using exact matching
        parts = [
            f for f in parent_dir.iterdir()
            if (f.name.lower().startswith(base_name + '.7z.') or
                f.name.lower().startswith(base_name + '.zip.') or
                f.name.lower().startswith(base_name + '.part') or
                (f.name.lower().startswith(base_name) and '.part' in f.name.lower()))
            and self._is_multipart_archive(f)
        ]
        parts.sort()
        logger.info(f'Found {len(parts)} parts for archive {base_name}')
        return parts

    def _process_unzip_temp_archives(self, _id: str) -> None:
        """Process archives in unzip_temp_dir recursively."""
        logger.info(f'Starting recursive archive processing for ID: {_id}')
        while True:
            # Get all files in unzip_temp_dir
            temp1_path = self.path_config.unzip_temp_dir / self._generate_folder_name(_id)
            if not temp1_path.exists() or not FileHelper.contains_files(temp1_path):
                logger.info(f'No more archives to process in {temp1_path}')
                break
            
            has_archives = False
            # Process each file in unzip_temp_dir
            for file_path in FileHelper.get_file_list(temp1_path):
                archive_type = self.archive_handler.detect_archive_type(file_path)
                if archive_type != ArchiveType.UNKNOWN:
                    has_archives = True
                    logger.info(f'Processing archive: {file_path} (type: {archive_type})')
                    
                    # Create archive info
                    archive = ArchiveInfo(
                        path=file_path,
                        type=archive_type,
                        parts=[file_path],
                        password=self.pieces_dao.query_pieces_unzip_key(_id) or self.default_password
                    )
                    
                    # If this is part of a multi-part archive, collect all parts
                    if self._is_multipart_archive(file_path):
                        logger.info(f'Collecting parts for multi-part archive: {file_path}')
                        archive.parts = self._collect_multipart_archive(file_path)
                    
                    # Try to extract the archive
                    temp_path = self.path_config.target_temp_dir / self._generate_folder_name(_id)
                    logger.info(f'Creating temp directory: {temp_path}')
                    temp_path.mkdir(parents=True, exist_ok=True)
                    
                    try:
                        logger.info(f'Extracting archive {file_path} to {temp_path}')
                        self.archive_handler.extract_archive(archive, temp_path)
                        logger.info(f'Successfully extracted archive: {file_path}')
                        
                        # Remove the original archive file(s)
                        for part in archive.parts:
                            logger.info(f'Removing archive part: {part}')
                            part.unlink()
                            logger.info(f'Successfully removed archive part: {part}')
                        
                        # Move any new archives back to unzip_temp_dir
                        self._move_archives_to_zip_temp(temp_path, _id)
                            
                    except Exception as e:
                        logger.error(f"Failed to extract archive {file_path}: {e}")
                        continue
            
            # Break if no archives were found in this iteration
            if not has_archives:
                logger.info('No more archives found to process')
                break

    def _move_to_target(self, temp3_path: Path, _id: str, source_path: Path) -> None:
        """Move processed files from temp3 to target directory."""
        if not FileHelper.contains_files(temp3_path):
            logger.info(f'No files to move from {temp3_path}')
            return
        
        dst_path = None
        skip_levels = self.path_config.move_config.skip_parent_levels
        if skip_levels > 0:
            dst_path = self.path_config.target_dir
        else:
            dst_path = self.path_config.target_dir / self._generate_folder_name(_id)
        logger.info(f'Moving files to target directory: {dst_path}')
        
        if dst_path.exists():
            logger.info(f'Removing existing target directory: {dst_path}')
            shutil.rmtree(dst_path)
        
        logger.info(f'Creating target directory: {dst_path}')
        dst_path.mkdir(parents=True, exist_ok=True)
        
        # Move all files and directories maintaining structure
        for item in temp3_path.iterdir():
            # Skip specified number of parent levels if configured
            skip_levels = self.path_config.move_config.skip_parent_levels
            if skip_levels > 0:
                # Calculate the path after skipping levels
                rel_path = item.relative_to(temp3_path)
                parts = list(rel_path.parts)
                
                # If we have enough levels to skip
                if len(parts) > skip_levels:
                    # Skip the specified number of parent folders
                    rel_path = Path(*parts[skip_levels:])
                    target_item = dst_path / rel_path
                else:
                    # Not enough levels to skip, use the item name only
                    target_item = dst_path / item.name
            else:
                # No levels to skip, maintain original structure
                target_item = dst_path / item.relative_to(temp3_path)
            
            if target_item.exists():
                logger.info(f'Removing existing target item: {target_item}')
                shutil.rmtree(target_item)
            
            logger.info(f'Creating parent directory: {target_item.parent}')
            target_item.parent.mkdir(parents=True, exist_ok=True)
            
            try:
                logger.info(f'Moving item {item} to {target_item}')
                shutil.move(item, target_item)
                logger.info(f'Successfully moved: {item.relative_to(temp3_path)} to {target_item}')
            except Exception as e:
                logger.error(f'Failed to move {item} to {target_item}: {e}')
                raise
        
        # Save history after all files are moved
        self.history_dao.save_history(_id, source_path, dst_path)
        logger.info(f'Completed moving files to target directory: {dst_path}')

    def _process_single_id(self, _id: str, files: List[Path], source_path: Path) -> None:
        """Process archives for a single ID.
        
        Args:
            _id: Content ID
            files: List of files to process
            source_path: Original source path
        """
        try:
            # Prepare archives
            archives = self._prepare_archive_info(_id, files)
            
            # Process each archive
            for basename, archive in archives.items():
                # Prepare temporary path
                temp_path = self.path_config.target_temp_dir / f"{self._generate_folder_name(_id)}"
                FileHelper.make_directory(temp_path)
                
                # Extract archive to temp3
                self._extract_to_target_temp(archive, temp_path)
                
                # Move archives to temp1 if any
                if FileHelper.contains_files(temp_path):
                    self._move_archives_to_zip_temp(temp_path, _id)
            
            # Process archives in temp1 recursively
            self._process_unzip_temp_archives(_id)
            
            # Move processed files to target
            temp3_path = self.path_config.target_temp_dir / self._generate_folder_name(_id)
            self._move_to_target(temp3_path, _id, source_path)
            
        except Exception as e:
            logger.error(f"Failed to process archives for ID {_id}: {e}")
            raise

    def cleanup(self) -> None:
        """Clean up processed files and temporary directories."""
        logger.info('Starting cleanup process')
        
        # Remove processed source files
        for item in self.path_config.source_dir.iterdir():
            _id = self._get_path_id(item)
            if (self.history_dao.query_history_by_id(_id) and 
                self._is_all_processed(_id)):
                logger.info(f'Removing processed source item: {item}')
                if item.is_dir():
                    shutil.rmtree(item)
                    logger.info(f'Removed processed source directory: {item}')
                else:
                    item.unlink()
                    logger.info(f'Removed processed source file: {item}')

        # Remove invalid target files
        for item in self.path_config.target_dir.iterdir():
            if '[' not in item.name and ']' not in item.name:
                logger.info(f'Removing invalid target item: {item}')
                if item.is_dir():
                    shutil.rmtree(item)
                    logger.info(f'Removed invalid target directory: {item}')
                else:
                    item.unlink()
                    logger.info(f'Removed invalid target file: {item}')
        
        logger.info('Cleanup process completed')
