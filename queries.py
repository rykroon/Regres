import copy

from .clauses import *

class Query():
    def __init__(self, table):
        self.table = table 
        self.clause_order = tuple()
        self.clause_mapping = dict()
        
    @property
    def sql(self):
        clauses = [self.clauses[c].to_sql() for c in self.clause_order if self.clauses.get(c) is not None]
        return ' '.join(clauses)


    def all(self):
        """
            Fetch all rows
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return cur.fetchall()

    def copy(self):
        return copy.copy(self)

    def execute(self):
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return conn.commit()

    def one(self):
        """
            Fetch one row
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return cur.fetchone()


class SelectQuery(Query):
    def __init__(self, table):
        self.table = table
        self.clause_order = ('SELECT', 'FROM', 'WHERE', 'ORDER BY', 'LIMIT', 'OFFSET')
        self.clauses = {
            'SELECT' : SelectClause(), 
            'FROM' : FromClause(self.table), 
        }

    def select(self, *args):
        q = self.copy()
        q.clauses['SELECT'] = SelectClause(*args)
        return q

    def where(self, **kwargs):
        q = self.copy()
        q.clauses['WHERE'] = WhereClause(**kwargs)
        return q

    def order_by(self, *args):
        q = self.copy()
        q.clauses['ORDER BY'] = OrderByClause(*args)
        return q

    def limit(self, count):
        q = self.copy()
        q.clauses['LIMIT'] = LimitClause(count)
        return q

    def offset(self, start):
        q = self.copy()
        q.clauses['OFFSET'] = OffsetClause(start)
        return q 


class InsertQuery(Query):
    def __init__(self, table):
        self.table = table 
        self.clause_order = ('INSERT', 'COLUMNS', 'VALUES', 'RETURNING')
        self.clauses = {
            'INSERT' : InsertClause(self.table),
        }

    def columns(self, *args):
        q = self.copy()
        q.clauses['COLUMNS'] = ColumnsClause(*args)
        return q

    def columns_and_values(self, **kwargs):
        columns = list(kwargs.keys())
        values = list(kwarg.values())
        return self.columns(*columns).values(*values)

    def values(self, *args):
        q = self.copy()
        q.clauses['VALUES'] = ValuesClause(*args)
        return q

    def returning(seld, *args):
        q = self.copy()
        q.clauses['RETURNING'] = ReturningClause(*args)
        return q


class UpdateQuery(Query):
    def __init__(self, table):
        self.table = table
        self.clause_order = ('UPDATE', 'SET', 'WHERE', 'RETURNING')
        self.clauses = {
            'UPDATE' : UpdateClause(self.table),
        }

    def set(self, **kwargs):
        q = copy.copy(self)
        q.clauses['SET'] = SetClause(**kwargs)
        return q 

    def where(self, **kwargs):
        q = copy.copy(self)
        q.clauses['WHERE'] = WhereClause(**kwargs)
        return q 

    def returning(seld, *args):
        q = copy.copy(self)
        q.clauses['RETURNING'] = ReturningClause()
        return q


class DeleteQuery(Query):
    def __init__(self, table):
        self.table = table 
        self.clause_order = ('DELETE', 'FROM', 'WHERE')
        self.clauses = {
            'DELETE' : DeleteClause(),
            'FROM' : FromClause(self.table),
        }

    def where(self, **kwargs):
        q = copy.copy(self)
        q.clauses['WHERE'] = WhereClause(**kwargs)
        return q
