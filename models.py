from datetime import datetime as dt
import json 
import pickle
import uuid

import redis
from .queries import UpdateQuery, DeleteQuery

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
        self.id = str(uuid.uuid4())

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
        return self.conn.delete(self.id)

    def store(self, expire=None):
        expire = expire or self.__class__.expire
        return self.conn.set(self.id, self.to_pickle(), ex=expire)


class Model(SerializableObject):

    table = None

    def __init__(self, **kwargs):
        self.__dict__ = dict.fromkeys(self.table.columns)
        self.__dict__.update(kwargs)

    @classmethod
    def get(cls, id):
        keys = cls.table.columns
        values = cls.table.query.where(id=id).one()
        d = dict(zip(keys, values))
        return cls(**d)

    @property
    def pk(self):
        return getattr(self, self.table.primary_key)
    
    @property
    def table(self):
        return self.__class__.table

    def delete(self):
        dq = DeleteQuery(self.table)

        dq = dq.where(**{self.table.primary_key:self.p})
        return dq.execute()

    def save(self):
        if self.pk is None:
            cols = [col for col in self.table.columns if getattr(self, col) is not None]
            values = [getattr(self, col) for col in self.table.columns if getattr(self, col) is not None]

            sql = """
                INSERT INTO {}
                ({}) VALUES ({})
                RETURNING *
            """.format(self.table.table_name, to_sql(cols), to_sql(values))

            with self.table.pool.getconn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    result = cur.fetchone()
                    conn.commit()
        else:
            d = {k:v for k, v in self.to_dict().items() if k in self.table.columns}
            q = UpdateQuery(self.table)
            q = q.set(**d).where(**{self.table.primary_key:self.pk}).returning()

            keys = self.table.columns
            values = q.one()
            d = dict(zip(keys, values))
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
        

