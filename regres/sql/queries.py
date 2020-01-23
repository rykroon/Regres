from copy import copy
from functools import reduce
from .expressions import Condition, Expression, SortExpression


class Query:
    
    def __init__(self, table):
        self.table = table
        self.args = ()
        self.fields = []
        self.condition = None
        self.sort_expressions = [] 
        self.limit = 0
        self.offset = 0

    def __getitem__(self, item):
        if type(item) != slice:
            raise TypeError("")

        if item.start < 0 or item.stop < 0 or item.step is not None:
            raise ValueError("")

        q = self.copy()
        q.offset = item.start 
        q.limit = item.stop - item.start
        return q

    def __len__(self):
        return self.count()

    def __repr__(self):
        return "{}(table='{}')".format(self.__class__.__name__, self.table._name)

    def __str__(self):
        return self.query

    @property
    def query(self):
        add = lambda x, y: x + y
        self.args = []

        if self.fields:
            fields = reduce(add, self.fields)
            query = "SELECT %s"
            self.args = [fields]
        else:
            query = 'SELECT *'
            self.args = []

        query = "{} FROM %s".format(query)
        self.args.append(self.table)

        if self.condition:
            query = "{} WHERE %s".format(query)
            self.args.append(self.condition)

        if self.sort_expressions:
            expression = reduce(add, self.sort_expressions)
            query = "{} ORDER BY %s".format(query)
            self.args.append(expression)

        if self.limit:
            query = "{} LIMIT %s".format(query)
            self.args.append(self.limit)

        if self.offset:
            query = "{} OFFSET %s".format(query)
            self.args.append(self.offset)

        self.args = tuple(self.args)

        return query


    def where(self, *args, **kwargs):
        #add logic for clearing condition

        if args:
            q = self._filter_by_args(*args)

        if kwargs:
            q = self._filter_by_kwargs(**kwargs)

        return q
        
    def limit(self, count):
        q = self.copy()
        q.limit = count or 0
        return q

    def offset(self, start):
        q = self.copy()
        q.offset = start or 0
        return q

    def order_by(self, *args):
        #make sure args are of type SortExpression, or str
        q = self.copy()
        q.sort_expressions.extend(args)
        return q 

    def all(self):
        rows = self.table._pool.fetchall(self.query, self.args)
        return rows

    def one(self):
        row = self.table._pool.fetchone(self.query, self.args)
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

        if q.condition:
            q.condition = q.condition & condition
        else:
            q.condition = condition

        return q

    def _filter_by_kwargs(**kwargs):
        pass