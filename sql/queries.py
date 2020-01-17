import copy
from functools import reduce

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
        @param table: The table object associated with the query.
        @param key: The key of a kwarg.
        @param value: The value of a kwarg
        @param default: The default method to call if one is not provided.
        
        ex: {'id__ge':5}
        table['id']['ge'](5)
        result = "table"."id" >= 5
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
        self._clause_order = tuple()
        self._clauses = dict()
        self.results = None

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, list(self._clauses.values()))

    def __str__(self):
        return ' '.join([str(self._clauses[c]) for c in self._clause_order if self._clauses.get(c) is not None])

    @property
    def vars(self):
        result = list()
        for clause in self._clause_order:
            clause = self._clauses.get(clause)
            if clause is not None:
                result.extend(clause.vars)
        return tuple(result)

    def all(self):
        return self.table.pool.fetchall(str(self), self.vars)

    def copy(self):
        return copy.copy(self)

    def execute(self):
        self.table.pool.execute(str(self), self.vars)

    def one(self):
        return self.table.pool.fetchone(str(self), self.vars)

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
        q._clauses['RETURNING'] = ReturningClause(*output_exprs)
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

        condition = reduce(lambda x,y: x & y, conditions)

        q = self.copy()
        q._clauses['WHERE'] = WhereClause(condition)
        return q


class SelectQuery(Query):
    """
        SELECT Query
    """

    def __init__(self, table):
        self.table = table
        self._clause_order = ('SELECT', 'FROM', 'WHERE', 'ORDER BY', 'LIMIT', 'OFFSET')
        self._clauses = {
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
        return self.count()

    def count(self):
        count = Count()
        q = self.select(count)
        q.fetchone()
        return q.result #???

    def select(self, *args):
        q = self.copy()
        q._clauses['SELECT'] = SelectClause(*args)
        return q

    def order_by(self, *args):
        exprs = list()

        for arg in args:
            if type(arg) == str:
                expr = resolve_arg(self.table, arg, 'asc')
                exprs.append(expr)

            elif type(arg) == Column:
                exprs.append(+arg)
            else:
                exprs.append(arg)

        q = self.copy()
        q._clauses['ORDER BY'] = OrderByClause(*exprs)
        return q

    def limit(self, count):
        q = self.copy()
        q._clauses['LIMIT'] = LimitClause(int(count))
        return q

    def offset(self, start):
        q = self.copy()
        q._clauses['OFFSET'] = OffsetClause(int(start))
        return q 

