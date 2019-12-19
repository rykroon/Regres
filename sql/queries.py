import copy
from types import MethodType
from clauses import *


class Query:
    """
        Base Query
    """

    def __init__(self, table):
        self.table = table 
        self.clause_order = tuple()
        self.clauses = dict()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, list(self.clauses.values()))

    def __str__(self):
        return ' '.join([str(self.clauses[c]) for c in self.clause_order if self.clauses.get(c) is not None])

    def _resolve_key(self, key):
        result = None

        for idx, val in enumerate(key.split('__')):
            if idx == 0:
                result = result.get_column(val)
            else:
                result = result.getattr(val)

        return result or key

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

    def returning(*args):
        class_ = self.__class__
        if class_ not in (InsertQuery, UpdateQuery, DeleteQuery):
            raise AttributeError("'{}' object has no attribute '{}'".format(class_.__name__, 'returning'))

        q = self.copy()
        q.clauses['RETURNING'] = ReturningClause(*args)
        return q


    def where(self, *args, **kwargs):
        """
            *args: Must be of type Condition
            **kwargs: Used to create Condition objects
        """

        class_ = self.__class__
        if class_ not in (SelectQuery, UpdateQuery, DeleteQuery):
            raise AttributeError("'{}' object has no attribute '{}'".format(class_.__name__, 'where'))

        conditions = list(args)
        for key, val in kwargs.items():
            x = self._resolve_key(key)
            if type(x) == Column:
                conditions.append(x == val)
            elif type(x) == types.MethodType:
                conditions.append(x(val))
            else:
                conditions.append(x)

        q = self.copy()
        q.clauses['WHERE'] = WhereClause(*conditions)
        return q


class SelectQuery(Query):
    """
        SELECT Query
    """

    def __init__(self, table):
        self.table = table
        self.clause_order = ('SELECT', 'FROM', 'WHERE', 'ORDER BY', 'LIMIT', 'OFFSET')
        self.clauses = {
            'SELECT' : SelectClause(), 
            'FROM' : FromClause(self.table), 
        }

    def __contains__(self, value):
        pass

    def __iter__(self):
        pass

    def __next__(self):
        pass

    def __len__(self):
        pass

    def select(self, *args):
        q = self.copy()
        q.clauses['SELECT'] = SelectClause(*args)
        return q

    def order_by(self, *args):
        exprs = list()

        """
            I am not 100% satisfied with this
            Is there any useful way to add **kwargs?
        """

        for arg in args:
            if type(arg) == str:
                split = arg.split('__')
                col = self.get_colum(split[0])
                if len(split) > 1:
                    expr = col.getattr(split[1])()
                else:
                    expr = col.asc()
            
            if type(arg) == OrderByExpression:
                expr = arg

            exprs.append(expr)

        q = self.copy()
        q.clauses['ORDER BY'] = OrderByClause(*exprs)
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
    """
        Insert Query
    """

    def __init__(self, table):
        self.table = table 
        self.clause_order = ('INSERT', 'COLUMNS', 'VALUES', 'RETURNING')
        self.clauses = {
            'INSERT' : InsertClause(self.table),
        }

    def values(self, **kwargs):
        """
            Keys are columns
            Values are values
        """
        columns = list(kwargs.keys())
        values = [Value(v) for v in kwargs.values()]            

        q = self.copy()
        q.clauses['COLUMNS'] = ColumnsClause(columns)
        q.clauses['VALUES'] = ValuesClause(values)
        return q


class UpdateQuery(Query):
    """
        UPDATE Query
    """

    def __init__(self, table):
        self.table = table
        self.clause_order = ('UPDATE', 'SET', 'WHERE', 'RETURNING')
        self.clauses = {
            'UPDATE' : UpdateClause(self.table),
        }

    def set(self, *args, **kwargs):
        """
            *args: must be of type Assignment
            **kwargs: are used to create Assignment objects
        """

        args = list(args)
        for k, v in kwargs.items():
            col = self.get_column(k)
            expr = col.assign(v)
            args.append(expr)   
         
        q = copy.copy(self)
        q.clauses['SET'] = SetClause(*args)
        return q 


class DeleteQuery(Query):
    """
        DELETE Query
    """

    def __init__(self, table):
        self.table = table 
        self.clause_order = ('DELETE', 'FROM', 'WHERE', 'RETURNING')
        self.clauses = {
            'DELETE' : DeleteClause(),
            'FROM' : FromClause(self.table),
        }

