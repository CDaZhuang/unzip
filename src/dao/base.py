"""
Base database access module.
"""
from enum import IntEnum
from typing import Optional, Dict, Any

import pymongo
import pymongo.collection

from config.config import MONGO_CONFIG

class DatabaseType(IntEnum):
    """Database type enumeration."""
    LOIBUS = 0
    SEMAO = 1
    VAMVIDEO = 2
    XLIXLI = 3

class MongoDBClient:
    """Base MongoDB client."""
    
    def __init__(self, db_type: DatabaseType) -> None:
        """Initialize MongoDB client.
        
        Args:
            db_type: Type of database to connect to
        """
        self.client = pymongo.MongoClient(MONGO_CONFIG.uri)
        self.db_name = self._get_db_name(db_type)
        self.db = self.client[self.db_name]
    
    def _get_db_name(self, db_type: DatabaseType) -> str:
        """Get database name from type.
        
        Args:
            db_type: Database type
            
        Returns:
            Database name
        """
        db_map = {
            DatabaseType.LOIBUS: "Loibus",
            DatabaseType.SEMAO: "Semao",
            DatabaseType.VAMVIDEO: "VamVideo",
            DatabaseType.XLIXLI: "Xlixli"
        }
        return db_map.get(db_type, "UnknownDB")
    
    def get_collection(self, collection_name: str) -> pymongo.collection.Collection:
        """Get MongoDB collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            MongoDB collection
        """
        return self.db[collection_name] 