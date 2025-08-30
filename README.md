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
uv pip install cachedb
```

Or with pip:

```bash
pip install cachedb
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

## Usage

### Using the Decorator

```python
from cachedb import cached

@cached("function_cache.db")
def expensive_operation(x, y):
    # Your expensive computation here
    return x * y

# First call - computes and caches
result1 = expensive_operation(10, 20)

# Subsequent calls with same arguments use cache
result2 = expensive_operation(10, 20)  # Returns cached result
```

### Advanced Usage

```python
# With custom cache configuration
db = CacheDB(
    "advanced_cache.db",
    temporary=True,  # Delete on program exit
    defaultObjectType="my_objects"
)

# Store with expiration
db.set("temporary_data", "This will expire", ttl=3600)  # Expires in 1 hour

# Batch operations
with db.batch() as batch:
    for i in range(100):
        batch.set(f"item_{i}", {"data": i * 2})
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
