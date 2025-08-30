from typing import Callable, Optional

from .core import CacheDB


def quickCache(
    db: CacheDB,
    *,
    objectType: Optional[str] = None,
    objectName: Optional[str] = None,
    dataType: Optional[str] = None,
    ttlSeconds: Optional[int] = None,
    keyFn: Optional[Callable[..., str]] = None,
):
    """
    Module-level convenience when you don't want a method-bound decorator.
    Usage:
        @quickCache(db, objectType="model", objectName="prices")
        def compute(x): ...
    """
    return db.quickCache(
        objectType=objectType,
        objectName=objectName,
        dataType=dataType,
        ttlSeconds=ttlSeconds,
        keyFn=keyFn,
    )
