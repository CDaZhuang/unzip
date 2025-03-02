"""
Data access objects for unzip operations.
"""
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Union

from .base import MongoDBClient, DatabaseType

class UnzipHistoryDao(MongoDBClient):
    """Data access object for unzip history."""
    
    def __init__(self, db_type: DatabaseType) -> None:
        """Initialize unzip history DAO.
        
        Args:
            db_type: Type of database to connect to
        """
        super().__init__(db_type)
        self.collection = self.get_collection("UnzipHistory")

    def save_history(self, _id: Union[int, str], src_path: Path, dst_path: Path) -> None:
        """Save unzip history.
        
        Args:
            _id: Record ID
            src_path: Source file path
            dst_path: Destination path
        """
        self.collection.insert_one({
            'id': _id,
            'src_path': str(src_path),
            'dst_path': str(dst_path),
            'date': datetime.now()
        })

    def update_history_status(self, _id: Union[int, str], status: str) -> None:
        """Update unzip history status.
        
        Args:
            _id: Record ID
            status: New status
        """
        self.collection.update_one({'id': _id}, {'$set': {'status': status}})

    def query_history_by_id(self, _id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Query unzip history by ID.
        
        Args:
            _id: Record ID
            
        Returns:
            History record if found, None otherwise
        """
        return self.collection.find_one({'id': _id})

    def query_history_by_src_path(self, src_path: Path) -> Optional[Dict[str, Any]]:
        """Query unzip history by source path.
        
        Args:
            src_path: Source file path
            
        Returns:
            History record if found, None otherwise
        """
        return self.collection.find_one({'src_path': str(src_path)})


class PiecesDao(MongoDBClient):
    """Data access object for file pieces."""
    
    def __init__(self, db_type: DatabaseType) -> None:
        """Initialize pieces DAO.
        
        Args:
            db_type: Type of database to connect to
        """
        super().__init__(db_type)
        self.collection = self.get_collection("Pieces")

    def query_pieces_by_id(self, _id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Query pieces by ID.
        
        Args:
            _id: Record ID
            
        Returns:
            Pieces record if found, None otherwise
        """
        return self.collection.find_one({'id': _id})

    def query_pieces_unzip_key(self, _id: Union[int, str]) -> str:
        """Query unzip key for pieces.
        
        Args:
            _id: Record ID
            
        Returns:
            Unzip key if found, empty string otherwise
        """
        pieces = self.query_pieces_by_id(_id)
        if not pieces:
            return ""
            
        # Try both possible key fields
        return pieces.get('unzip_key', pieces.get('open_key', "")) 