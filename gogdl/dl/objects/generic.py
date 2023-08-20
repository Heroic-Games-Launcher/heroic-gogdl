from dataclasses import dataclass
from enum import Flag, auto
from typing import Optional


class BaseDiff:
    def __init__(self):
        self.deleted = []
        self.new = []
        self.changed = []
        self.redist = []
        self.removed_redist = []

    def __str__(self):
        return f"Deleted: {len(self.deleted)} New: {len(self.new)} Changed: {len(self.changed)}"

class TaskFlag(Flag):
    NONE = 0
    OPEN_FILE = auto()
    CLOSE_FILE = auto()
    CREATE_FILE = auto()
    RENAME_FILE = auto()
    DELETE_FILE = auto()
    OFFLOAD_TO_CACHE = auto()
    MAKE_EXE = auto()
    RELEASE_MEM = auto()

@dataclass
class MemorySegment:
    offset: int
    end: int

    @property
    def size(self):
        return self.end - self.offset

@dataclass
class ChunkTask:
    product: str
    index: int

    compressed_md5: str
    md5: str
    size: int
    download_size: int
    
    cleanup: bool = False
    offload_to_cache: bool = False
    old_offset: Optional[int] = None
    old_file: Optional[str] = None

@dataclass
class FileTask:
    path: str
    flags: TaskFlag

    old_file: Optional[str] = None


@dataclass
class TerminateWorker:
    pass
