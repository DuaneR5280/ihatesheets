import redis
import msgspec
from datetime import datetime
from dateutil import tz
from models import SheetPost

class RedisCache:
    def __init__(self, host="localhost", port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db)

    def get(self, key, type=SheetPost):
        """
        Retrieve a cached value.

        Args:
            key: The key to retrieve from the cache.
            type: The type of the value to retrieve. Defaults to SheetPost.

        Returns:
            The cached value if it exists, otherwise None.
        """
        return msgspec.msgpack.decode(self.r.get(key), type=type)

        
    def set(self, key, value):
        """
        Store a value in the cache.

        Args:
            key: The key to store the value under.
            value: The value to store.

        Returns:
            None
        """
        self.r.set(key, msgspec.msgpack.encode(value))
    
    def exists(self, key):
        """
        Check if a key exists in the cache.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.
        """
        return bool(self.r.exists(key))
    
    def hset(self, hash, key, value: SheetPost):
        """
        Set a field in a hash.

        Args:
            hash (str): The name of the hash.
            key (str): The key of the field to set.
            value (SheetPost): The value of the field to set.

        Returns:
            None

        Example:
            hset("sheets", "sheet:jxlbk6qYK5aHEaGGYip2ITWistiJZtloqh56fS2USMhTD", SheetPost)
            hset(hash="sheets", key=f"sheet:{item.sheet_id}", value=item)
        """
        encoded_data = msgspec.msgpack.encode(value)
        self.r.hset(hash, key, encoded_data)
    
    
    def hget(self, hash, key, type=SheetPost):
        """
        Get the value of a field in a hash.

        Args:
            hash (str): The name of the hash.
            key (str): The key of the field to get.
            type (Type): The type of the field to get.

        Returns:
            The value of the field.

        Example:
            hget("sheets", "sheet:jxlbk6qYK5aHEaGGYip2ITWistiJZtloqh56fS2USMhTD", type=SheetPost)
            hget("sheets", "sheet:jxlbk6qYK5aHEaGGYip2ITWistiJZtloqh56fS2USMhTD")
        """
        return msgspec.msgpack.decode(self.r.hget(hash, key), type=type)
    
    def hexists(self, hash, key):
        return self.r.hexists(hash, key)


    def delete(self, key):
        self.r.delete(key)
    
    def get_all_hash(self):
        data = []
        cursor = '0'
        while cursor != 0:
            cursor, items = self.r.hscan('sheets', cursor=cursor, match='sheet:*', count=20)
            for value in items.values():
                item = msgspec.msgpack.decode(value, type=SheetPost)
                item.sheet_raw = msgspec.json.decode(item.sheet_raw)
                data.append(item)
        return data
