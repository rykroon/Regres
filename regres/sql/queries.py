from copy import copy
from functools import reduce
from .expressions import Condition, Expression, SortExpression


class Query:
    
    def __init__(self, table):
        self.table = table
        self._args = ()
        self._fields = []
        self._condition = None
        self._sort_expressions = [] 
        self._count = 0
        self._start = 0

    def __contains__(self, key):
        pass

    def __getitem__(self, item):
        if type(item) != slice:
            raise TypeError("")

        q = self.offset(item.start)
        q = q.limit(item.stop - q._start)
        return q

    def __iter__(self):
        pass

    def __len__(self):
        return self.count()

    def __next__(self):
        pass

    def __repr__(self):
        return "{}(table={})".format(self.__class__.__name__, repr(self.table._name))

    def __str__(self):
        return self.query

    @property
    def query(self):
        add = lambda x, y: x + y
        self._args = []

        if self._fields:
            fields = reduce(add, self.fields)
            query = "SELECT %s"
            self._args = [fields]
        else:
            query = 'SELECT *'
            self._args = []

        query = "{} FROM %s".format(query)
        self._args.append(self.table)

        if self._condition:
            query = "{} WHERE %s".format(query)
            self._args.append(self.condition)

        if self._sort_expressions:
            expression = reduce(add, self.sort_expressions)
            query = "{} ORDER BY %s".format(query)
            self._args.append(expression)

        if self._count:
            query = "{} LIMIT %s".format(query)
            self._args.append(self._count)

        if self._start:
            query = "{} OFFSET %s".format(query)
            self._args.append(self._start)

        self._args = tuple(self._args)

        return query

    def select(self, *args):
        #args can be of type str, Column, or Expression ?
        pass

    def where(self, *args, **kwargs):
        #add logic for clearing condition

        if args:
            q = self._filter_by_args(*args)

        if kwargs:
            q = self._filter_by_kwargs(**kwargs)

        return q

    def order_by(self, *args):
        #make sure args are of type SortExpression, or str
        q = self.copy()
        q._sort_expressions.extend(args)
        return q 
        
    def limit(self, count):
        q = self.copy()
        q._count = 0 if not count or count < 0 else count
        return q

    def offset(self, start):
        q = self.copy()
        q._start = 0 if not start or start < 0 else start 
        return q

    def all(self):
        rows = self.table._pool.fetchall(self.query, self._args)
        return rows

    def one(self):
        row = self.table._pool.fetchone(self.query, self._args)
        return row

    def count(self):
        count = Expression('COUNT(*)')
        q = self.select(count)
        row = self.one()
        return row[0]

    def _filter_by_args(self, *args):
        if not all([type(arg) == Condition for arg in args]):
            raise TypeError("args must be of type '{}'".format(Condition.__name__))

        and_ = lambda x, y : x & y
        condition = reduce(and_, args)

        q = self.copy()

        if q._condition:
            q._condition = q._condition & condition
        else:
            q._condition = condition

        return q

    def _filter_by_kwargs(**kwargs):
        pass