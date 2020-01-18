from .expressions import *


class Column:
    """
        SQL Column
    """
    def __init__(self, name, table):
        self.name = name 
        self.table = table

    def __add__(self, other):
        return "{}, {}".format(self, other)

    def __radd__(self, other):
        return "{}, {}".format(other, self)

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

        item = item.lower()

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
        return Condition(expr) 

    def __pos__(self):
        expr = "{} ASC".format(self.qualified_name)
        return Expression(expr)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def __str__(self):
        return '"{}"'.format(self.name)

    @property
    def qualified_name(self):
        return '"{}"."{}"'.format(self.table._name, self.name)

    def assign(self, value):
        expr = "{} = %s".format(self)
        return Expression(expr, value) 

    def between(self, x, y):
        expr = "{} BETWEEN %s AND %s".format(self.qualified_name)
        return Condition(expr, x, y)

    def in_(self, value):
        expr = "{} IN %s".format(self.qualified_name)
        return Condition(expr, value) 

    def like(self, value):
        expr = "{} LIKE %s".format(self.qualified_name)
        return Condition(expr, value) 