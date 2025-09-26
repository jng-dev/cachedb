
import pickle
import hashlib
import time

def utcNow() -> int:
    return int(time.time())

def pickleDump(obj) -> bytes:
    return pickle.dumps(obj, protocol=4)

def pickleLoad(b: bytes):
    return pickle.loads(b)

def hashInputs(args: tuple, kwargs: dict) -> str:
    payload = (args, tuple(sorted(kwargs.items(), key=lambda x: x[0])))
    pb = pickleDump(payload)
    h = hashlib.blake2b(pb, digest_size=16)  
    return h.hexdigest()
