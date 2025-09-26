# fits in cachedb/cachedb/__init__.py
from .core import CacheDB
from .decorators import quickCache

__all__ = ["CacheDB", "quickCache"]
