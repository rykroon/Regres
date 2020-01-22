from datetime import datetime, date, time, timedelta
from decimal import Decimal
from functools import reduce
import hashlib
import json 
import pickle
import uuid


class ObjectDoesNotExist(Exception):
    pass


class MultipleObjectsReturned(Exception):
    pass


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if type(o) in (datetime, date, time):
            return o.isoformat()

        elif type(o) == timedelta:
            return o.total_seconds()

        elif type(o) == Decimal:
            return float(o)

        return super().default(o)


class SerializableObject:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return vars(self)

    def to_json(self, cls=JSONEncoder, **kwargs):
        return json.dumps(self.to_dict(), cls=cls, **kwargs) 

    def to_pickle(self):
        return pickle.dumps(self) 

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


class RedisModel(SerializableObject):

    conn = None
    expire = None

    """
        Magic Methods
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.id:
            self.id = str(uuid.uuid4())

    def __hash__(self):
        return hash((self.__class__.__name__, self.id))

    """
        Properties
    """

    @property
    def _conn(self):
        return self.__class__.conn

    @property
    def _expire(self):
        return self.__class__.expire

    """
        Instance Methods
    """

    def delete(self):
        return self._delete_from_redis()

    def save(self, expire=None):
        return self._save_to_redis(expire)

    def _delete_from_redis(self):
        return self._conn.delete(hash(self))

    def _save_to_redis(self, expire=None):
        expire = expire or self._expire
        return self._conn.set(hash(self), self.to_pickle(), ex=expire)

    """
        Class Methods
    """

    @classmethod
    def get(cls, id):
        return cls._get_from_redis(id)

    @classmethod
    def _get_from_redis(cls, id):
        key = hash((cls.__name__, id))
        instance = cls.conn.get(key)
        if instance:
            instance = cls.from_pickle(instance)
        return instance


class Model(SerializableObject):

    table = None

    """
        Magic Methods
    """

    def __init__(self, **kwargs):
        self.__dict__ = dict.fromkeys(self._table.column_names)
        self.__dict__.update(kwargs)

    def __getitem__(self, column):
        if column in self._table:
            return getattr(self, column.name)

        raise KeyError("")

    """
        Properties
    """
    @property
    def pk(self):
        return self[self._table.primary_key]

    @property
    def _table(self):
        return self.__class__.table

    """
        Instance Methods
    """

    def delete(self):
        return self._delete_from_postgres()

    def save(self):
        return self._save_to_postgres()

    def _delete_from_postgres(self):
        query = """
            DELETE FROM %s 
                WHERE %s
        """
        condition = self._table.primary_key == self.pk
        args = (self._table, condition)

        try:
            self._table._pool.execute(query, args)
            return True
        except:
            return False

    def _save_to_postgres(self):
        if self.pk is None:
            query, vars = self._insert()
        else:
            query, vars = self._update()

        try:
            values = self._table._pool.fetchone(query, vars)
            d = dict(zip(self._table.column_names, values))
            self.__dict__.update(d)
            return True 
        except:
            return False

    def _insert(self):
        query = """
            INSERT INTO %s %s
                VALUES %s
                RETURNING *
        """

        column_names = tuple([col for col in self._table if self[col] is not None])
        values = tuple(self[col] for col in self._table.columns)

        args = (self._table, column_names, values)
        return query, args

    def _update(self):
        assignments = [col.assign(self[col]) for col in self._table]
        add = lambda x, y : x + y
        assignments = reduce(add, assignments)
        condition = self._table.primary_key == self.pk

        query = """
            UPDATE {table_name} 
                SET {assignments} 
                WHERE {condition} 
                RETURNING *
        """.format(
            table_name=self._table,
            assignments=assignments,
            condition=condition
        )

        args = assignments.args + condition.args
        return query, args
    
    """
        Class Methods
    """

    @classmethod
    def get(cls, id):
        return cls._get_from_postgres(id)

    @classmethod
    def get_many(cls, **kwargs):
        pass

    @classmethod
    def _get_from_postgres(cls, id):
        query = """
            SELECT *
                FROM %s
                WHERE %s
        """
        condition = cls.table.primary_key == id
        args = (cls.table, condition)
        
        values = cls.table._pool.fetchall(query, args)

        if len(values) == 0:
            raise ObjectDoesNotExist

        elif len(values) > 1:
            raise MultipleObjectsReturned

        keys = cls.table.column_names
        values = values[0]
        d = dict(zip(keys, values))
        return cls(**d)


class HybridModel(Model, RedisModel):

    conn = None
    expire = None
    table = None

    def __hash__(self):
        return hash((self.__class__.__name__, self.pk))

    def delete(self):
        success = self._delete_from_postgres()
        if success:
            self._delete_from_redis()
        return success

    def save(self, expire=None):
        success = self._save_to_postgres()
        if success:
            self._save_to_redis(expire)
        return success

    @classmethod
    def get(cls, id):
        instance = cls._get_from_redis(id)
        if instance is None:
            instance = cls._get_from_postgres(id)
            if instance is not None:
                instance._save_to_redis()
        return instance