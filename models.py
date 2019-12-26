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
        self.__dict__ = dict.fromkeys(self.table.column_names)
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
        keys = cls.table.column_names
        values = cls.table.query().where(cls.table.primary_key == id).one()
        d = dict(zip(keys, values))
        return cls(**d)

    @property
    def pk(self):
        return self[self.__class__.table.primary_key.name]

    def delete(self):
        q = DeleteQuery(self.__class__.table)
        id_filter = self.__class__.table.primary_key == self.pk
        q = q.where(id_filter)
        return q.execute()

    def save(self):
        if self.pk is None:
            q = InsertQuery(self.__class__.table)
            values = {col: self[col] for col in self.__class__.table.column_names if self[col] is not None}
            q = q.values(**values).returning(*self.__class__.table.columns)

        else:
            q = UpdateQuery(self.__class__.table)
            values = {col.name: self[col.name] for col in self.__class__.table}
            id_filter = self.__class__.table.primary_key == self.pk
            q = q.set(**values).where(id_filter).returning(*self.__class__.table.columns)

        d = dict(zip(self.__class__.table.column_names, q.one()))
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
    def _uuid(self):
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
        

