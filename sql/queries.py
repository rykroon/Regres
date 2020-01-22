from copy import copy
from functools import reduce
from .expressions import Condition


class Query:
    def __init__(self, table):
        self.table = table

    def _build_sql(self):
        pass

    def all(self):
        pass

    def copy(self):
        return copy(self)


class SelectQuery(Query):
    
    def __init__(self):
        self.condition = None
        self.order_by = [] 
        self.limit = 0
        self.offset = 0

    def _build_sql(self):
        # !!! idea !!!
        # add logic so that psycopg2 can automagically convert Expression objects into sql

        if self.order_by:
            pass

        if self.limit:
            limit = Expression('LIMIT %s', self.limit)

        if self.offset:
            offset = Expression('OFFSET %s', self.offset)

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

    def _filter_by_kwargs(self, **kwargs):
        field_lookups = {
            'eq'    : '=',
            'le'    : '<=',
            'lt'    : '<',
            'ge'    : '>=',
            'gt'    : '>',
            'ne'    : '!=',
            'in'    : 'IN',
            'is'    : 'IS',
            'isnot' : 'IS NOT',
            'like'  : 'LIKE',
        }

        #potentially add fieldlookups.py
        #have functions for each lookup?
        #add the lookups here as well as in columns.py?

        for key, val in kwargs.items():
            split = key.split('__')

            if len(split) == 1:
                field, lookup = split[0], 'eq'
            elif len(split) == 2:
                field, lookup = split
            else:
                raise Exception
            
            operator = field_lookups.get(lookup)
            expr = '"{}" {} %s'.format(field, operator)
            condition = Conditon(expr, val)

    def limit(self, count):
        self.limit = count

    def offset(self, start):
        self.offset = start

    def order_by(self, *args):
        #check type of args
        self.order_by.extend(args)