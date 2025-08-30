from __future__ import annotations

import atexit
import os
import signal
import sqlite3
import threading
from functools import wraps
from types import FrameType, TracebackType
from typing import (
    Any,
    Callable,
    List,
    Optional,
    ParamSpec,
    TypeVar,
    cast,
)

try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec

from . import utils
from .schema import SCHEMA_SQL


def _safeFuncName(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__name__", fn.__class__.__name__)


P = ParamSpec("P")
R = TypeVar("R")


class CacheDB:
    def __init__(
        self,
        path: str,
        temporary: bool = False,
        defaultObjectType: str = "default_objecttype",
        defaultDataType: str = "default_datatype",
    ):
        """
        path: directory or file path. If directory and temporary True, a temp file is created inside it.
        temporary: if True, DB file is deleted on interpreter exit or SIGINT/SIGTERM.
        """

        self._lock: threading.RLock = threading.RLock()
        self.temporary: bool = temporary
        if os.path.isdir(path):
            dbFile = os.path.join(path, "cachedb.sqlite")
        else:
            dbFile = path

        os.makedirs(os.path.dirname(os.path.abspath(dbFile)), exist_ok=True)

        self.dbPath: str = dbFile
        self._conn: sqlite3.Connection = sqlite3.connect(
            self.dbPath, check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._applySchema()

        self.defaultObjectType: str = defaultObjectType
        self.defaultDataType: str = defaultDataType

        if self.temporary:
            self._registerTempDeletion()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            finally:
                if self.temporary:
                    self._safeUnlink()

    def _applySchema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)

    def _registerTempDeletion(self) -> None:
        atexit.register(self._safeUnlink)

        def _sig_handler(signum: int, frame: FrameType | None) -> None:
            try:
                self._conn.close()
            finally:
                self._safeUnlink()
            default = signal.getsignal(signum)
            if callable(default):
                default(signum, frame)

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _sig_handler)
            except Exception:
                pass

    def _safeUnlink(self) -> None:
        try:
            if os.path.exists(self.dbPath):
                os.remove(self.dbPath)
        except Exception:
            pass

    def __enter__(self) -> "CacheDB":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False

    def _ensureObjectType(self, name: Optional[str]) -> int:
        name = name or self.defaultObjectType
        with self._lock:
            cur = self._conn.execute("SELECT id FROM object_type WHERE name=?", (name,))
            row = cur.fetchone()
            if row:
                return row[0]
            self._conn.execute("INSERT INTO object_type(name) VALUES (?)", (name,))
            return self._conn.execute(
                "SELECT id FROM object_type WHERE name=?", (name,)
            ).fetchone()[0]

    def _ensureDataType(self, objectTypeId: int, name: Optional[str]) -> int:
        name = name or self.defaultDataType
        with self._lock:
            cur = self._conn.execute(
                "SELECT id FROM data_type WHERE object_type_id=? AND name=?",
                (objectTypeId, name),
            )
            row = cur.fetchone()
            if row:
                return row[0]
            self._conn.execute(
                "INSERT INTO data_type(object_type_id, name) VALUES (?, ?)",
                (objectTypeId, name),
            )
            return self._conn.execute(
                "SELECT id FROM data_type WHERE object_type_id=? AND name=?",
                (objectTypeId, name),
            ).fetchone()[0]

    def _ensureObject(self, objectTypeId: int, name: Optional[str]) -> int:
        name = name or "default_object"
        with self._lock:
            cur = self._conn.execute(
                "SELECT id FROM object WHERE object_type_id=? AND name=?",
                (objectTypeId, name),
            )
            row = cur.fetchone()
            if row:
                return row[0]
            self._conn.execute(
                "INSERT INTO object(object_type_id, name) VALUES (?, ?)",
                (objectTypeId, name),
            )
            return self._conn.execute(
                "SELECT id FROM object WHERE object_type_id=? AND name=?",
                (objectTypeId, name),
            ).fetchone()[0]

    def _lookupObjectTypeId(self, name: str) -> Optional[int]:
        cur = self._conn.execute("SELECT id FROM object_type WHERE name=?", (name,))
        row = cur.fetchone()
        return row[0] if row else None

    def _lookupDataTypeId(self, objectTypeId: int, name: str) -> Optional[int]:
        cur = self._conn.execute(
            "SELECT id FROM data_type WHERE object_type_id=? AND name=?",
            (objectTypeId, name),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def _lookupObjectId(self, objectTypeId: int, name: str) -> Optional[int]:
        cur = self._conn.execute(
            "SELECT id FROM object WHERE object_type_id=? AND name=?",
            (objectTypeId, name),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def setData(
        self,
        data: Any,
        *,
        objectType: Optional[str] = None,
        objectName: Optional[str] = None,
        dataType: Optional[str] = None,
        key: Optional[str] = None,
        ttlSeconds: Optional[int] = None,
    ) -> None:
        otId = self._ensureObjectType(objectType)
        dtId = self._ensureDataType(otId, dataType)
        objId = self._ensureObject(otId, objectName)

        keyHash = key or "singleton"
        now = utils.utcNow()
        expires = None if ttlSeconds is None else now + int(ttlSeconds)
        blob = utils.pickleDump(data)

        with self._lock:
            self._conn.execute(
                """
                INSERT INTO data(object_id, data_type_id, key_hash, value_blob, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(object_id, data_type_id, key_hash)
                DO UPDATE SET value_blob=excluded.value_blob, created_at=excluded.created_at, expires_at=excluded.expires_at
                """,
                (objId, dtId, keyHash, sqlite3.Binary(blob), now, expires),
            )

    def getData(
        self,
        *,
        objectType: Optional[str] = None,
        objectName: Optional[str] = None,
        dataType: Optional[str] = None,
        key: Optional[str] = None,
        includeExpired: bool = False,
    ) -> Any | None:
        otId = self._ensureObjectType(objectType)
        dtId = self._ensureDataType(otId, dataType)
        objId = self._ensureObject(otId, objectName)
        keyHash = key or "singleton"
        now = utils.utcNow()

        with self._lock:
            if includeExpired:
                row = self._conn.execute(
                    "SELECT value_blob, expires_at FROM data WHERE object_id=? AND data_type_id=? AND key_hash=?",
                    (objId, dtId, keyHash),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT value_blob, expires_at FROM data WHERE object_id=? AND data_type_id=? AND key_hash=? AND (expires_at IS NULL OR expires_at >= ?)",
                    (objId, dtId, keyHash, now),
                ).fetchone()

        if not row:
            return None
        blob, _expires = row
        return utils.pickleLoad(blob)

    def deleteObjectType(self, objectType: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM object_type WHERE name=?", (objectType,))

    def deleteObject(self, objectType: str, objectName: str) -> None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT o.id FROM object o
                JOIN object_type ot ON ot.id = o.object_type_id
                WHERE ot.name=? AND o.name=?
            """,
                (objectType, objectName),
            ).fetchone()
            if row:
                self._conn.execute("DELETE FROM object WHERE id=?", (row[0],))

    def deleteDataType(self, objectType: str, dataType: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                DELETE FROM data_type
                WHERE name=? AND object_type_id = (SELECT id FROM object_type WHERE name=?)
            """,
                (dataType, objectType),
            )

    def purgeExpired(self) -> int:
        now = utils.utcNow()
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM data WHERE expires_at IS NOT NULL AND expires_at < ?",
                (now,),
            )
            return cur.rowcount

    def quickCache(
        self,
        *,
        objectType: Optional[str] = None,
        objectName: Optional[str] = None,
        dataType: Optional[str] = None,
        ttlSeconds: Optional[int] = None,
        keyFn: Optional[Callable[..., str]] = None,
    ) -> Callable[[Callable[..., R]], Callable[..., R]]:
        """
        Decorator that caches function result as pickled blob keyed by args/kwargs.
        If ttlSeconds is set, expired entries are recomputed.
        """

        def decorator(func: Callable[..., R]) -> Callable[..., R]:
            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                keyHash = (
                    keyFn(*args, **kwargs) if keyFn else utils.hashInputs(args, kwargs)
                )
                dtype = dataType or _safeFuncName(func)
                cached = self.getData(
                    objectType=objectType,
                    objectName=objectName,
                    dataType=dtype,
                    key=keyHash,
                    includeExpired=False,
                )
                if cached is not None:
                    return cast(R, cached)
                result = func(*args, **kwargs)
                self.setData(
                    result,
                    objectType=objectType,
                    objectName=objectName,
                    dataType=dtype,
                    key=keyHash,
                    ttlSeconds=ttlSeconds,
                )
                return result

            return wrapper

        return decorator

    def listObjects(self, objectType: str) -> list[str]:
        """
        Return list of object names for a given objectType.
        """
        with self._lock:
            otId = self._lookupObjectTypeId(objectType)
            if otId is None:
                return []
            rows = self._conn.execute(
                "SELECT name FROM object WHERE object_type_id=? ORDER BY name", (otId,)
            ).fetchall()
            return [r[0] for r in rows]

    def listDataTypes(self, objectType: str) -> list[str]:
        """
        Return list of dataType names for a given objectType.
        """
        with self._lock:
            otId = self._lookupObjectTypeId(objectType)
            if otId is None:
                return []
            rows = self._conn.execute(
                "SELECT name FROM data_type WHERE object_type_id=? ORDER BY name",
                (otId,),
            ).fetchall()
            return [r[0] for r in rows]

    def listKeys(self, *, objectType: str, objectName: str, dataType: str) -> List[str]:
        """
        Return list of keys (key_hash) under a specific (objectType, objectName, dataType).
        """
        with self._lock:
            otId = self._lookupObjectTypeId(objectType)
            if otId is None:
                return []
            objId = self._lookupObjectId(otId, objectName)
            if objId is None:
                return []
            dtId = self._lookupDataTypeId(otId, dataType)
            if dtId is None:
                return []
            rows = self._conn.execute(
                "SELECT key_hash FROM data WHERE object_id=? AND data_type_id=? ORDER BY key_hash",
                (objId, dtId),
            ).fetchall()
            return [r[0] for r in rows]

    def getAllDataForObjectType(
        self,
        objectType: str,
        *,
        includeExpired: bool = False,
        withMeta: bool = True,
        withBlobs: bool = False,
    ) -> list[tuple]:
        """
        Return all rows under an objectType.
        If withBlobs is False, returns metadata only.
        Tuple shape:
        when withBlobs: (object_name, data_type, key_hash, value_blob, created_at, expires_at)
        else:           (object_name, data_type, key_hash, created_at, expires_at)
        """
        with self._lock:
            otId = self._lookupObjectTypeId(objectType)
            if otId is None:
                return []
            time_clause = (
                ""
                if includeExpired
                else "AND (d.expires_at IS NULL OR d.expires_at >= ?)"
            )
            params = (otId,) if includeExpired else (otId, utils.utcNow())
            cols = (
                "o.name, dt.name, d.key_hash, d.value_blob, d.created_at, d.expires_at"
                if withBlobs
                else "o.name, dt.name, d.key_hash, d.created_at, d.expires_at"
            )
            rows = self._conn.execute(
                f"""
                SELECT {cols}
                FROM data d
                JOIN object o ON o.id = d.object_id
                JOIN data_type dt ON dt.id = d.data_type_id
                WHERE o.object_type_id=?
                {time_clause}
                ORDER BY o.name, dt.name, d.key_hash
                """,
                params,
            ).fetchall()
            if withBlobs:
                return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
            else:
                return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    def getAllDataOfTypeForObject(
        self,
        *,
        objectType: str,
        objectName: str,
        dataType: str,
        includeExpired: bool = False,
        deserialize: bool = True,
    ) -> list[tuple]:
        """
        Return all rows for a specific (objectType, objectName, dataType).
        Tuple shape:
        when deserialize: (key_hash, value_obj, created_at, expires_at)
        else:             (key_hash, value_blob, created_at, expires_at)
        """
        with self._lock:
            otId = self._lookupObjectTypeId(objectType)
            if otId is None:
                return []
            objId = self._lookupObjectId(otId, objectName)
            if objId is None:
                return []
            dtId = self._lookupDataTypeId(otId, dataType)
            if dtId is None:
                return []
            time_clause = (
                "" if includeExpired else "AND (expires_at IS NULL OR expires_at >= ?)"
            )
            params = (objId, dtId) if includeExpired else (objId, dtId, utils.utcNow())
            rows = self._conn.execute(
                f"""
                SELECT key_hash, value_blob, created_at, expires_at
                FROM data
                WHERE object_id=? AND data_type_id=?
                {time_clause}
                ORDER BY key_hash
                """,
                params,
            ).fetchall()
            if deserialize:
                out = []
                for k, b, c, e in rows:
                    out.append((k, utils.pickleLoad(b), c, e))
                return out
            else:
                return rows
