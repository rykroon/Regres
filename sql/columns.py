from .expressions import *


class Column:
    """
        SQL Column
    """
    def __init__(self, name, table):
        self.name = name 
        self.table = table

    def __eq__(self, value):
        return Condition(self.qualified_name, '=', Value(value)) 

    def __ge__(self, value):
        return Condition(self.qualified_name, '>=', Value(value)) 

    def __getitem__(self, item):
        """
            @param item: the name of an sql operator
            @return: the method associated with that operator
        """

        item = item.lower()

        return {
            'as':   self.as_,
            'asc':  self.__pos__,
            'desc': self.__neg__,
            'eq':   self.__eq__,
            'ge':   self.__ge__,
            'gt':   self.__gt__,
            'le':   self.__le__,
            'lt':   self.__lt__,
            'ne':   self.__ne__,
            'in':   self.in_,
            'like': self.like
        }[item]

    def __gt__(self, value):
        return Condition(self.qualified_name, '>', Value(value)) 

    def __le__(self, value):
        return Condition(self.qualified_name, '<=', Value(value)) 

    def __lt__(self, value):
        return Condition(self.qualified_name, '<', Value(value)) 

    def __ne__(self, value):
        return Condition(self.qualified_name, '!=', Value(value)) 

    def __neg__(self):
        return Expression(self.qualified_name, 'DESC')

    def __pos__(self):
        return Expression(self.qualified_name, 'ASC')

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def __str__(self):
        return '"{}"'.format(self.name)

    @property
    def qualified_name(self):
        table_name = self.table.alias or self.table.name
        return '"{}"."{}"'.format(table_name, self.name)

    def as_(self, output_name=None):
        return OutputExpression(self.qualified_name, output_name)

    def assign(self, value):
        return Expression(self, '=', Value(value))

    def in_(self, value):
        return Condition(self.qualified_name, 'IN', Value(value))

    def like(self, value):
        return Condition(self.qualified_name, 'LIKE', Value(value))


