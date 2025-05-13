from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from enum import Enum


class DataNodeStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DECOMMISSIONED = "decommissioned"


class DataNodeRegistration(BaseModel):
    node_id: Optional[str] = None
    hostname: str
    port: int
    storage_capacity: int  # in bytes
    available_space: int  # in bytes


class DataNodeInfo(BaseModel):
    node_id: str
    hostname: str
    port: int
    status: DataNodeStatus
    storage_capacity: int  # in bytes
    available_space: int  # in bytes
    last_heartbeat: Optional[datetime] = None
    blocks_stored: int = 0


class BlockLocation(BaseModel):
    block_id: str
    datanode_id: str
    is_leader: bool = False


class BlockInfo(BaseModel):
    block_id: str
    file_id: str
    size: int  # in bytes
    locations: List[BlockLocation]
    checksum: Optional[str] = None


class FileType(str, Enum):
    FILE = "file"
    DIRECTORY = "directory"


class FileMetadata(BaseModel):
    file_id: Optional[str] = None
    name: str
    path: str
    type: FileType
    size: int = 0  # in bytes, 0 for directories
    blocks: List[str] = []  # list of block IDs
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    owner: Optional[str] = None


class DirectoryListing(BaseModel):
    path: str
    contents: List[FileMetadata]


class ErrorResponse(BaseModel):
    error: str
    details: Optional[str] = None
