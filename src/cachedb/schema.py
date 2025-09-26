# fits in cachedb/cachedb/schema.py
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS object_type (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS data_type (
  id INTEGER PRIMARY KEY,
  object_type_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  UNIQUE(object_type_id, name),
  FOREIGN KEY(object_type_id) REFERENCES object_type(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS object (
  id INTEGER PRIMARY KEY,
  object_type_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  UNIQUE(object_type_id, name),
  FOREIGN KEY(object_type_id) REFERENCES object_type(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS data (
  id INTEGER PRIMARY KEY,
  object_id INTEGER NOT NULL,
  data_type_id INTEGER NOT NULL,
  key_hash TEXT NOT NULL,
  value_blob BLOB NOT NULL,
  created_at INTEGER NOT NULL,
  expires_at INTEGER,
  UNIQUE(object_id, data_type_id, key_hash),
  FOREIGN KEY(object_id) REFERENCES object(id) ON DELETE CASCADE,
  FOREIGN KEY(data_type_id) REFERENCES data_type(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_data_lookup
  ON data(object_id, data_type_id, key_hash);

-- Light housekeeping
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
"""
