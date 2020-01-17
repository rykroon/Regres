from datetime import datetime 
import hashlib
import json 
import pickle
import uuid


class ObjectDoesNotExist(Exception):
    pass

class SerializableObject:
    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if type(o) == datetime:
                return o.isoformat()
            return super().default(o)

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

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def to_dict(self):
        return vars(self)

    def to_json(self, cls=JSONEncoder, **kwargs):
        return json.dumps(self.to_dict(), cls=cls, **kwargs) 

    def to_pickle(self):
        return pickle.dumps(self) 


class RedisModel(SerializableObject):

    conn = None
    expire = None

    """
        Class Methods
    """

    @classmethod
    def __get(cls, id):
        instance = cls.conn.get(id) 
        if instance:
            instance = cls.from_pickle(instance)
        return instance

    @classmethod
    def get(cls, id):
        return cls.__get(id)

    """
        Magic Methods
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._uuid = str(uuid.uuid4())

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

    def __delete(self):
        return self._conn.delete(self._uuid)

    def __save(self, expire=None):
        expire = expire or self._expire
        return self._conn.set(self._uuid, self.to_pickle(), ex=expire)

    def delete(self):
        return self.__delete()

    def save(self, expire=None):
        return self.__save(expire)


class Model(SerializableObject):

    table = None

    """
        Class Methods
    """

    @classmethod
    def __get(cls, id):
        query = """
            SELECT *
                FROM {table_name}
                WHERE {condition}
        """.format(
            table_name=cls.table,
            condition="{} = %s".format(cls.table.primary_key.qualified_name)
        )
        
        values = cls.table.pool.fetchone(query, (id, ))

        if values is None:
            raise ObjectDoesNotExist
        
        keys = cls.table.column_names
        d = dict(zip(keys, values))
        return cls(**d)

    @classmethod
    def get(cls, id):
        return cls.__get(id)

    @classmethod
    def get_many(cls, **kwargs):
        pass

    """
        Magic Methods
    """

    def __init__(self, **kwargs):
        self.__dict__ = dict.fromkeys(self._table.column_names)
        self.__dict__.update(kwargs)

    def __getitem__(self, item):
        """
            @param item: the name of a column.
        """
        if item in self._table:
            return getattr(self, item)

        raise KeyError("")

    """
        Properties
    """

    @property
    def _table(self):
        return self.__class__.table

    @property
    def pk(self):
        return self[self._table.primary_key.name]

    """
        Instance Methods
    """

    def __delete(self):
        query = """
            DELETE FROM {table_name} 
                WHERE {condition}
        """.format(
            table_name=self._table,
            condition="{} = %s".format(self._table.primary_key.qualified_name)
        )
        try:
            self._table.pool.execute(query, (self.pk,))
            return True
        except:
            return False

    def __save(self):
        if self.pk is None:
            query, vars = self._insert()
        else:
            query, vars = self._update()

        try:
            values = self._table.pool.fetchone(query, vars)
            d = dict(zip(self._table.column_names, values))
            self.__dict__.update(d)
            return True 
        except:
            return False

    def _insert(self):
        query = """
            INSERT INTO {table_name} ({column_names}) 
                VALUES({values}) 
                RETURNING *
        """.format(
            table_name=self._table,
            column_names=', '.join([str(col) for col in self._table if self[col.name] is not None]),
            values=', '.join(['%s' for col in self._table if self[col.name] is not None])
        )

        vars = tuple([self[col.name] for col in self._table if self[col.name] is not None])

        return query, vars

    def _update(self):
        query = """
            UPDATE {table_name} 
                SET {assignments} 
                WHERE {condition} 
                RETURNING *
        """.format(
            table_name=self._table,
            assignments=', '.join(["{} = %s".format(col) for col in self._table]),
            condition="{} = %s".format(self._table.primary_key.qualified_name)
        )

        vars = [self[col.name] for col in self._table]
        vars.append(self.pk)
        vars = tuple(vars)

        return query, vars

    def delete(self):
        return self.__delete()

    def save(self):
        return self.__save()


class HybridModel(Model, RedisModel):

    conn = None
    expire = None
    table = None

    @classmethod
    def get(cls, id):
        instance = cls._RedisModel__get(id)
        if instance is None:
            instance = cls._Model__get(id)
            if instance is not None:
                instance._RedisModel__save()
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
        success = self._Model__delete()
        if success:
            self._RedisModel__delete()
        return success

    def save(self, expire=None):
        success = self._Model__save()
        if success:
            self._RedisModel__save(expire)
        return success