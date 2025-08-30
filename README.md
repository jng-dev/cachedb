# CacheDB

A high-performance, thread-safe SQLite-based caching system for Python that stores Python objects as pickled blobs with decorator support.

## Features

- **Simple Decorator API** - Cache function results with a single decorator
- **Thread-safe** - Built-in locking for concurrent access
- **Automatic Serialization** - Seamlessly store and retrieve Python objects
- **Temporary Storage** - Optional in-memory or file-based temporary storage
- **Type Hints** - Full type support for better IDE integration

## Installation

Using `uv` (recommended):

```bash
uv install --editable 'path/to/cloned/repo/root'
```

## Quick Start

```python
from cachedb import CacheDB

# Basic usage
db = CacheDB("my_cache.db")

# Store any Python object
db.set("user:1", {"name": "Alice", "age": 30})

# Retrieve the object
user = db.get("user:1")
```

## Development

### Setup

1. Clone the repository
2. run 'uv sync' in root

the package itself is installed as a dev dependency so tests can import it as if it was an external package

### Code Style

This project uses:

- uv for python package management
- ruff default as formatter and linter
- pytest for testing

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

_Note: Test coverage requirements are intentionally flexible. Feel free to adjust them according to your project's needs._
