from contextlib import contextmanager
import copy
from datetime import datetime as dt 
import json
import pickle
import uuid

from psycopg2.pool import SimpleConnectionPool as SCP
import redis


def to_sql(val):
    if type(val) in (list, tuple):
        val = [to_sql(i) for i in val]
        return ', '.join(val)

    if type(val) == str:
        return "'{}'".format(val)

    if type(val) == type(None):
        return 'NULL'

    return str(val)


class SimpleConnectionPool(SCP):

    def __init__(self, minconn, maxconn, database='postgres', user='postgres', host='localhost', *args, **kwargs):
        super().__init__(minconn, maxconn, database=database, user=user, host=host, *args, **kwargs)
    
    @contextmanager
    def getconn(self):
        conn = super().getconn()
        try:
            yield conn
        finally:
            self.putconn(conn)        


class Column(str):
    pass        


class Table:

    def __init__(self, table_name, pool):
        self.table_name = table_name
        self.pool = pool

        with self.pool.getconn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT a.column_name, c.constraint_type 
                        FROM information_schema.columns AS a
                            LEFT JOIN information_schema.key_column_usage AS b
                                ON a.table_schema = b.table_schema 
                                AND a.table_name = b.table_name
                                AND a.column_name = b.column_name
                            LEFT JOIN information_schema.table_constraints AS c
                                ON b.table_schema = c.table_schema
                                AND b.table_name = c.table_name
                                AND b.constraint_name = c.constraint_name
                        WHERE a.table_name = '{}'
                        ORDER BY a.ordinal_position
                """.format(self.table_name)
                cur.execute(sql)
                rows = cur.fetchall()
                
                if rows:
                    self.columns = tuple([Column(row[0]) for row in rows])
                    self._primary_key = [Column(row[0]) for row in rows if row[1]=='PRIMARY KEY'][0]

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def query(self):
        return Query(self)


class Query:

    def __init__(self, table):
        self.table = table
        self._select = None
        self._where = []
        self._order_by = None
        self._limit = None
        self._offset = None

    @property
    def sql(self):
        sql = ''

        if self._select:
            for idx, s in enumerate(self._select):
                if idx == 0: sql += "SELECT {}".format(s)
                else: sql += ", {}".format(s)
        else:
            sql += 'SELECT *'

        sql += " FROM {}".format(self.table.table_name)

        if self._where:
            for idx, val in enumerate(self._where):
                if idx == 0: sql += " WHERE"
                else: sql += " AND"
                sql += " {} {} {}".format(val[0], val[1], to_sql(val[3]))

        if self._order_by:
            for idx, o in enumerate(self._order_by):
                if idx == 0: sql += " ORDER BY {}".format(o)
                else: sql += ", {}".format(o)

        if self._limit:
            sql += " LIMIT {}".format(self._limit)

        if self._offset:
            sql += " OFFSET {}".format(self._offset)

        return sql

    def all(self):
        """
            Fetch all rows
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return cur.fetchall()

    def limit(self, value):
        if type(value) != int:
            raise TypeError("Limit must be of type int")
        self._limit = value

    def offset(self, value):
        if type(value) != int:
            raise TypeError("Offset must be of type int")
        self._offset = value

    def one(self):
        """
            Fetch one row
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return cur.fetchone()
                
    def order_by(self, *args):
        q = copy.copy(self)
        q._order_by = args
        return q

    def select(self, *args):
        q = copy.copy(self)
        q._select = args
        return q
             
    def where(self, **kwargs):
        q = copy.copy(self)

        for col, val in kwargs.items():
            if '__' in col:
                col, op = col.split('__')
                op_conversions = dict(eq='=', ne='!=', lt='<', le='<=', gt='>', ge='>=')
                op = op_conversions.get(op, op)

            else:
                op = '='

            q._where.append((col, op, val))
                
        return q


class Model:

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
        sql = """
            DELETE
            FROM {}
            WHERE {} = {}
        """.format(self.table.table_name, self.table.primary_key, to_sql(self.pk))

        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.commit()

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
            sql = """
                UPDATE {}
                SET {}
                WHERE {} = {}
            """.format(self.table.table_name, x, self.table.primary_key, )


 
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
        super().__init__()
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



class HybridModel(Model, RedisModel):

