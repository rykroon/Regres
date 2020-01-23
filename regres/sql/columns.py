from psycopg2.extensions import AsIs, adapt, register_adapter
from psycopg2.extensions import Column as BaseColumn
from .expressions import *


class Column:
    """
        SQL Column
    """
    def __init__(self, name, table):
        self.name = name 
        self.table = table
        self._attr_name = self.name.replace(' ', '_').lower()

    def __eq__(self, value):
        expr = "{} = %s".format(self.qualified_name)
        return Condition(expr, value) 

    def __ge__(self, value):
        expr = "{} >= %s".format(self.qualified_name)
        return Condition(expr, value) 

    def __getitem__(self, item):
        """
            @param item: the name of an sql operator
            @return: the method associated with that operator
        """

        #possible remove asc and desc so that this is used only for lookups

        return {
            'asc':      self.__pos__,
            'between':  self.between,
            'desc':     self.__neg__,
            'eq':       self.__eq__,
            'ge':       self.__ge__,
            'gt':       self.__gt__,
            'le':       self.__le__,
            'lt':       self.__lt__,
            'ne':       self.__ne__,
            'in':       self.in_,
            'is':       self.is_,
            'isnot':    self.isnot,
            'like':     self.like
        }[item]

    def __gt__(self, value):
        expr = "{} > %s".format(self.qualified_name)
        return Condition(expr, value) 

    def __le__(self, value):
        expr = "{} <= %s".format(self.qualified_name)
        return Condition(expr, value)  

    def __lt__(self, value):
        expr = "{} < %s".format(self.qualified_name)
        return Condition(expr, value) 

    def __ne__(self, value):
        expr = "{} != %s".format(self.qualified_name)
        return Condition(expr, value) 

    def __neg__(self):
        expr = "{} DESC".format(self.qualified_name)
        return SortExpression(expr) 

    def __pos__(self):
        expr = "{} ASC".format(self.qualified_name)
        return SortExpression(expr)

    def __repr__(self):
        return "{}(name={})".format(self.__class__.__name__, repr(self.name))

    def __str__(self):
        return '"{}"'.format(self.name)

    @property
    def qualified_name(self):
        return '"{}"."{}"'.format(self.table._name, self.name)

    def asc(self):
        return self.__pos__()

    def assign(self, value):
        expr = "{} = %s".format(self)
        return Expression(expr, value) 

    def between(self, x, y):
        expr = "{} BETWEEN %s AND %s".format(self.qualified_name)
        return Condition(expr, x, y)

    def desc(self):
        return self.__neg__()

    def in_(self, value):
        expr = "{} IN %s".format(self.qualified_name)
        return Condition(expr, value) 

    def is_(self, value):
        expr = "{} IS %s".format(self.qualified_name)
        return Condition(expr)

    def isnot(self, value):
        expr = "{} IS NOT %s".format(self.qualified_name)
        return Condition(expr)

    def like(self, value):
        expr = "{} LIKE %s".format(self.qualified_name)
        return Condition(expr, value) 


def adapt_column(column):
    return AsIs(str(column))


register_adapter(Column, adapt_column)