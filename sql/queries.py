import copy
from .clauses import *
from .columns import Column


def resolve_arg(table, arg, default):
    """
        ex: 'id__asc'
        table['id']['asc']
    """
    attrs = arg.split('__')
    column = attrs[0]
    method = attrs[1] if len(attrs) > 1 else default 
    return table[column][method]()


def resolve_kwarg(table, key, val, default):
    """
        ex: {'id__ge':5}
        table['id']['ge'](5)
    """
    attrs = key.split('__')
    column = attrs[0]
    method = attrs[1] if len(attrs) > 1 else default 
    return table[column][method](val)


"""
    Guidlines for writing Queries.
    - queries should be focused on doing conversions so that it passes 
        the underlying clauses' strict type checking.
"""


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

    def all(self):
        """
            Fetch all rows
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(str(self))
                return cur.fetchall()

    def copy(self):
        return copy.copy(self)

    def execute(self):
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(str(self))

    def one(self):
        """
            Fetch one row
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(str(self))
                return cur.fetchone()

    def returning(self, *args):
        class_ = self.__class__
        if class_ not in (InsertQuery, UpdateQuery, DeleteQuery):
            raise AttributeError("'{}' object has no attribute '{}'".format(class_.__name__, 'returning'))

        output_exprs = list()
        for arg in args:
            if type(arg) == str:
                expr = resolve_arg(self.table, arg, 'as')
                output_exprs.append(expr)

            elif type(arg) == Column:
                expr = arg.as_()
                output_exprs.append(expr)

            else:
                 output_exprs.append(arg)
        
        q = self.copy()
        q.clauses['RETURNING'] = ReturningClause(*output_exprs)
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
            condition = resolve_kwarg(self.table, key, val, 'eq')
            conditions.append(condition)

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

    def count(self, *args):
        count = Count(*args)
        return self.select(count)

    def distinct(self, *args):
        distinct = Distinct(*args)
        return self.select(distinct)

    def select(self, *args):
        q = self.copy()
        q.clauses['SELECT'] = SelectClause(*args)
        return q

    def order_by(self, *args):
        exprs = list()

        for arg in args:
            if type(arg) == str:
                expr = resolve_arg(self.table, arg, 'asc')
                exprs.append(expr)

            elif type(arg) == Column:
                expr = arg.asc()
                exprs.append(expr)
            else:
                exprs.append(arg)

        q = self.copy()
        q.clauses['ORDER BY'] = OrderByClause(*exprs)
        return q

    def limit(self, count):
        q = self.copy()
        q.clauses['LIMIT'] = LimitClause(int(count))
        return q

    def offset(self, start):
        q = self.copy()
        q.clauses['OFFSET'] = OffsetClause(int(start))
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
        columns = [self.table[col] for col in kwargs.keys()]
        values = [Value(v) for v in kwargs.values()]            

        q = self.copy()
        q.clauses['COLUMNS'] = ColumnsClause(*columns)
        q.clauses['VALUES'] = ValuesClause(*values)
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
            col = self.table[k]
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

