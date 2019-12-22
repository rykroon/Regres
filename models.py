from datetime import datetime as dt
import hashlib
import json 
import pickle
import uuid


import redis
from .sql import InsertQuery, UpdateQuery, DeleteQuery

class SerializableObject:
    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if type(0) == dt:
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
        return self.__dict__.copy() 

    def to_json(self, cls=JSONEncoder, **kwargs):
        return json.dumps(self.to_dict(), cls=cls, **kwargs) 

    def to_pickle(self):
        return pickle.dumps(self) 


class RedisModel(SerializableObject):

    conn = None
    expire = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        #make private? change value to key?
        self._cache_id = str(uuid.uuid4())

    @property
    def conn(self):
        return self.__class__.conn

    @classmethod
    def retrieve(cls, id):
        instance = cls.conn.get(id) 
        if instance:
            instance = cls.from_pickle(instance)
        return instance

    def remove(self):
        return self.conn.delete(self._cache_id)

    def store(self, expire=None):
        expire = expire or self.__class__.expire
        return self.conn.set(self._cache_id, self.to_pickle(), ex=expire)


class Model(SerializableObject):

    table = None

    def __init__(self, **kwargs):
        self.__dict__ = dict.fromkeys(self.table.column_names)
        self.__dict__.update(kwargs)

    def __getitem__(self, item):
        if item in self.table.column_names:
            return getattr(self, item)

        raise KeyError("")

    @classmethod
    def get(cls, id):
        keys = cls.table.column_names
        values = cls.table.query().where(cls.table.primary_key == id).one()
        d = dict(zip(keys, values))
        return cls(**d)

    @property
    def column_values(self):
        return {k:v for k, v in self.to_dict().items() if k in self.table}

    @property
    def pk(self):
        return self[self.table.primary_key.name]
    
    @property
    def table(self):
        # key = "_{}__{}".format(self.__class__, 'table')
        # return self.__class__.__dict__[key]
        return self.__class__.table

    def delete(self):
        q = DeleteQuery(self.table)
        q = dq.where(self.table.primary_key == self.pk)
        return q.execute()

    def save(self):
        if self.pk is None:
            q = InsertQuery(self.table)

            values = {col: self[col] for col in self.table.column_names if self[col] is not None}
            q = q.values(**values).returning(*self.table.columns)

            d = dict(zip(self.table.column_names, q.one()))
            self.__dict__.update(d)

        else:
            q = UpdateQuery(self.table)
            q = q.set(**self.column_values).where(self.table.primary_key == self.pk).returning(*self.table.columns)

            d = dict(zip(self.table.column_names, q.one()))
            self.__dict__.update(d)


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
    def _cache_id(self):
        key = "{}:{}".format(self.table.name, self.pk)
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
        

