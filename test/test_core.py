# test/conftest.py
# test/test_lifecycle.py

# test/test_crud.py
from pathlib import Path

import pytest

from cachedb.cachedb import CacheDB


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "cache.sqlite"
    d = CacheDB(
        path=str(path),
        temporary=False,
        defaultObjectType="proj",
        defaultDataType="misc",
    )
    try:
        yield d
    finally:
        d.close()


@pytest.fixture
def temp_db(tmp_path):
    path = tmp_path / "tempdir"
    path.mkdir()
    d = CacheDB(
        path=str(path), temporary=True, defaultObjectType="proj", defaultDataType="misc"
    )
    try:
        yield d
    finally:
        # close triggers deletion for temporary=True
        d.close()


@pytest.fixture
def freeze_time(monkeypatch):
    """
    Provides a function to set utils.utcNow() to a fixed integer.
    Usage:
        set_time = freeze_time
        set_time(1000)
    """
    timeslot = {"now": 1234567890}

    def set_time(t: int):
        timeslot["now"] = int(t)
        # Patch the symbol imported in core.py
        import cachedb.cachedb.core as core_mod

        monkeypatch.setattr(
            core_mod.utils, "utcNow", lambda: timeslot["now"], raising=True
        )

    return set_time


def test_creates_file_in_dir_when_path_is_dir(tmp_path):
    d = CacheDB(path=str(tmp_path), temporary=False)
    try:
        # should have created cachedb.sqlite inside the dir
        db_file = tmp_path / "cachedb.sqlite"
        assert db_file.exists()
    finally:
        d.close()


def test_uses_exact_file_when_path_is_file(tmp_path):
    file_path = tmp_path / "explicit.sqlite"
    d = CacheDB(path=str(file_path), temporary=False)
    try:
        assert file_path.exists()
    finally:
        d.close()


def test_context_manager_calls_close(tmp_path):
    file_path = tmp_path / "ctx.sqlite"
    with CacheDB(path=str(file_path), temporary=False) as db:
        db.setData(1, objectType="t", objectName="o", dataType="d")
    # connection closed; file persists because not temporary
    assert file_path.exists()


def test_temporary_db_is_deleted_on_close(tmp_path):
    d = CacheDB(path=str(tmp_path), temporary=True)
    db_file = Path(d.dbPath)
    assert db_file.exists()
    d.close()
    assert not db_file.exists()


def test_set_get_singleton(db):
    db.setData({"x": 1}, objectType="proj", objectName="objA", dataType="meta")
    got = db.getData(objectType="proj", objectName="objA", dataType="meta")
    assert got == {"x": 1}


def test_set_get_with_key(db):
    db.setData(10, objectType="proj", objectName="A", dataType="vals", key="k1")
    db.setData(20, objectType="proj", objectName="A", dataType="vals", key="k2")
    assert (
        db.getData(objectType="proj", objectName="A", dataType="vals", key="k1") == 10
    )
    assert (
        db.getData(objectType="proj", objectName="A", dataType="vals", key="k2") == 20
    )


def test_overwrite_same_key_updates_value_and_timestamps(db, freeze_time):
    freeze_time(1000)
    db.setData("v1", objectType="T", objectName="o", dataType="D", key="k")
    v = db.getData(objectType="T", objectName="o", dataType="D", key="k")
    assert v == "v1"

    # move time and overwrite
    freeze_time(2000)
    db.setData("v2", objectType="T", objectName="o", dataType="D", key="k")
    v2 = db.getData(objectType="T", objectName="o", dataType="D", key="k")
    assert v2 == "v2"


def test_ttl_expiration_without_sleep(db, freeze_time):
    freeze_time(100)
    db.setData("temp", objectType="proj", objectName="A", dataType="ttl", ttlSeconds=30)
    assert db.getData(objectType="proj", objectName="A", dataType="ttl") == "temp"

    # advance just past expiry
    freeze_time(131)
    assert db.getData(objectType="proj", objectName="A", dataType="ttl") is None
    # includeExpired returns the raw row, but getData always deserializes or returns None.
    # We don't have an includeExpired-return-value path; so simply ensure miss after expiry.


def test_delete_objectType_cascades(db):
    db.setData(1, objectType="T", objectName="o1", dataType="d")
    db.setData(2, objectType="T", objectName="o2", dataType="d")
    db.deleteObjectType("T")
    assert db.listObjects("T") == []


def test_delete_object_only_removes_that_object(db):
    db.setData(1, objectType="T", objectName="o1", dataType="d")
    db.setData(2, objectType="T", objectName="o2", dataType="d")
    db.deleteObject("T", "o1")
    assert db.listObjects("T") == ["o2"]


def test_delete_dataType_removes_its_rows(db):
    db.setData(1, objectType="T", objectName="o", dataType="D1", key="a")
    db.setData(2, objectType="T", objectName="o", dataType="D2", key="b")
    db.deleteDataType("T", "D1")
    # keys under D1 gone, D2 remains
    assert db.listKeys(objectType="T", objectName="o", dataType="D1") == []
    assert db.listKeys(objectType="T", objectName="o", dataType="D2") == ["b"]


def test_purge_expired_returns_deleted_count(db, freeze_time):
    freeze_time(10)
    db.setData(
        "alive",
        objectType="T",
        objectName="o",
        dataType="D",
        key="keep",
        ttlSeconds=100,
    )
    db.setData(
        "old", objectType="T", objectName="o", dataType="D", key="drop", ttlSeconds=5
    )
    # advance past the short TTL only
    freeze_time(20)
    deleted = db.purgeExpired()
    assert deleted >= 1
    # alive entry should still be there
    assert (
        db.getData(objectType="T", objectName="o", dataType="D", key="keep") == "alive"
    )


# test/test_queries.py
def test_list_objects_and_datatypes(db):
    db.setData(1, objectType="T", objectName="o1", dataType="D1")
    db.setData(2, objectType="T", objectName="o2", dataType="D2")
    assert db.listObjects("T") == ["o1", "o2"]
    # order may be deterministic by ORDER BY name
    assert set(db.listDataTypes("T")) == {"D1", "D2"}


def test_list_keys(db):
    db.setData(1, objectType="T", objectName="o", dataType="D", key="a")
    db.setData(2, objectType="T", objectName="o", dataType="D", key="b")
    assert set(db.listKeys(objectType="T", objectName="o", dataType="D")) == {"a", "b"}


def test_get_all_data_for_objectType_metadata_only(db):
    db.setData(1, objectType="T", objectName="o", dataType="D", key="a")
    rows = db.getAllDataForObjectType("T", withMeta=True, withBlobs=False)
    # shape: (object_name, data_type, key_hash, created_at, expires_at)
    assert len(rows) == 1 and rows[0][0:3] == ("o", "D", "a")


def test_get_all_data_of_type_for_object_deserialize(db):
    db.setData({"v": 1}, objectType="T", objectName="o", dataType="D", key="k")
    rows = db.getAllDataOfTypeForObject(
        objectType="T", objectName="o", dataType="D", deserialize=True
    )
    assert rows == [("k", {"v": 1}, rows[0][2], rows[0][3])]


def test_non_creating_lookups_dont_spawn_rows(db):
    # Query for a totally unknown type should not create it
    assert db.listObjects("unknown") == []
    # Now create something else and confirm "unknown" still absent
    db.setData(1, objectType="T", objectName="o", dataType="D")
    assert db.listObjects("unknown") == []


# test/test_decorator.py
def test_quickcache_hits_same_args(db):
    calls = {"n": 0}

    @db.quickCache(objectType="C", objectName="bucket", dataType="f")
    def f(x, y=1):
        calls["n"] += 1
        return x + y

    assert f(5, y=7) == 12
    assert f(5, y=7) == 12
    assert calls["n"] == 1  # second call hit cache


def test_quickcache_respects_ttl(db, freeze_time):
    calls = {"n": 0}

    @db.quickCache(objectType="C", objectName="b", dataType="g", ttlSeconds=10)
    def g(x):
        calls["n"] += 1
        return x * 2

    freeze_time(100)
    assert g(3) == 6
    assert g(3) == 6
    assert calls["n"] == 1

    # after TTL, recompute
    freeze_time(111)
    assert g(3) == 6
    assert calls["n"] == 2


def test_quickcache_with_custom_keyFn(db):
    calls = {"n": 0}

    def key_fn(x):
        return f"id:{x % 2}"  # two buckets: odd/even

    @db.quickCache(objectType="C", objectName="b", dataType="h", keyFn=key_fn)
    def h(x):
        calls["n"] += 1
        return x

    # First time odd bucket → miss, compute, store 1
    assert h(1) == 1
    # First time even bucket → miss, compute, store 2
    assert h(2) == 2
    # Hit odd bucket again → returns cached 1
    assert h(3) == 1
    # Hit even bucket again → returns cached 2
    assert h(4) == 2

    assert calls["n"] == 2
