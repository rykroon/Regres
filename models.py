from datetime import datetime 
import hashlib
import json 
import pickle
import uuid


import redis
from .sql import InsertQuery, UpdateQuery, DeleteQuery

class SerializableObject:
    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if type(o) == datetime:
                return o.isoformat()
            return super().default(o)

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    @classmethod
    def from_dict(cls, d):
        return cls(**d) 
        
    @classmethod
    def from_json(cls, j):
        d = json.loads(j)
        return cls.from_dict(d)

    @classmethod
    def from_pickle(cls, p):
        instance = pickle.loads(p)
        if type(instance) != cls:
            raise TypeError("Object is not of type {}".format(cls.__name__))
        return instance

    def to_dict(self):
        return vars(self)

    def to_json(self, cls=JSONEncoder, **kwargs):
        return json.dumps(self.to_dict(), cls=cls, **kwargs) 

    def to_pickle(self):
        return pickle.dumps(self) 


class RedisModel(SerializableObject):

    conn = None
    expire = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._uuid = str(uuid.uuid4())

    @classmethod
    def retrieve(cls, id):
        instance = cls.conn.get(id) 
        if instance:
            instance = cls.from_pickle(instance)
        return instance

    def remove(self):
        return self.__class__.conn.delete(self._uuid)

    def store(self, expire=None):
        expire = expire or self.__class__.expire
        return self.__class__.conn.set(self._uuid, self.to_pickle(), ex=expire)


class Model(SerializableObject):

    table = None

    def __init__(self, **kwargs):
        self.__dict__ = dict.fromkeys(self.__class__.table.column_names)
        self.__dict__.update(kwargs)

    def __getitem__(self, item):
        """
            @param item: the name of a column.
        """
        if item in self.__class__.table:
            return getattr(self, item)

        raise KeyError("")

    @classmethod
    def get(cls, id):
        query = """
            SELECT *
                FROM {table_name}
                WHERE {condition}
        """.format(
            table_name=cls.table,
            condition="{} = %s".format(cls.table.primary_key.qualified_name)
        )
        
        with cls.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (id,))
                values = cur.fetchone()
        
        keys = cls.table.column_names
        d = dict(zip(keys, values))
        return cls(**d)

    @property
    def pk(self):
        return self[self.__class__.table.primary_key.name]

    def delete(self):
        query = """
            DELETE FROM {table_name} 
                WHERE {condition}
        """.format(
            table_name=self.__class__.table,
            condition="{} = %s".format(self.__class__.table.primary_key.qualified_name)
        )

        with self.__class__.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (self.pk,))
                return True 

        return False

    def save(self):
        if self.pk is None:
            query, vars = self._insert()
        else:
            query, vars = self._update()

        with self.__class__.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, vars)
                results = cur.fetchone()
                d = dict(zip(self.__class__.table.column_names, results))
                self.__dict__.update(d)
                return True
        return False


    def _insert(self):
        query = """
            INSERT INTO {table_name} ({column_names}) 
                VALUES({values}) 
                RETURNING *
        """.format(
            table_name=self.__class__.table,
            column_names=', '.join([str(col) for col in self.__class__.table if self[col.name] is not None]),
            values=', '.join(['%s' for col in self.__class__.table if self[col.name] is not None])
        )

        vars = tuple([self[col.name] for col in self.__class__.table if self[col.name] is not None])

        return query, vars

    def _update(self):
        query = """
            UPDATE {table_name} 
                SET {assignments} 
                WHERE {condition} 
                RETURNING *
        """.format(
            table_name=self.__class__.table,
            assignments=', '.join(["{} = %s".format(col) for col in self.__class__.table]),
            condition="{} = %s".format(self.__class__.table.primary_key.qualified_name)
        )

        vars = [self[col.name] for col in self.__class__.table]
        vars.append(self.pk)
        vars = tuple(vars)

        return query, vars



class HybridModel(Model, RedisModel):

    conn = None
    expire = None
    table = None

    @classmethod
    def get(cls, id):
        instance = cls.retrieve(id)
        if instance is None:
            instance = cls.get(id)
            if instance is not None:
                instance.store()
        return instance

    @property
    def _uuid(self):
        """
            @returns: A UUID based on the MD5 hash of the table name and primary key.
        """
        key = "{}:{}".format(self._table.name, self.pk)
        md5 = hashlib.md5(key.encode())
        return str(uuid.UUID(md5.hexdigest()))
    
    def delete(self):
        success = super().delete()
        if success:
            self.remove()
        return success

    def save(self, expire=None):
        success = super().save()
        if success:
            self.store(expire)
        return success
        

