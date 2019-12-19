from datetime import datetime as dt
from decimal import Decimal

ASTERISK = Expression('*')

class Expression:
    """
        Expressions are made of args.
        The args are joined with spaces to create an expression
    """
    def __init__(self, *args):
        self.args = list(args)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.args)

    def __str__(self):
        return ' '.join([str(arg) for arg in self.args])


class Assignment(Expression):
    def __init__(self, column, expression):
        self.column_name = '"{}"'.format(column.name)
        self.expression = expression
        super().__init__(column, '=', expression)


class Condition(Expression):
    """
        
    """
    def __init__(self, left, operator, right):
        super().__init__(left, operator, right)

    def __and__(self, val):
        return Condition(str(self), 'AND', str(val))

    def __neg__(self):
        return Condition('NOT', str(self))

    def __or__(self, val):
        return Condition(str(self), 'OR', str(val))


class Value():
    """
        SQL Value
    """
    def __init__(self, value):
        """
            deprecate dict ?
        """
        valid_types = (bool, int, float, str, list, tuple, dict, dt, Decimal, Value)
        if type(value) not in valid_types:
            raise TypeError("Invalid Type")
        
        if type(value) == Value:
            self.value = value.value
        else:
            self.value = value

    # potentially add parent class that contains all logical operators
    # eq, lt, gt, etc. 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.value)

    def __str__(self):
        if type(self.value) in (list, tuple):
            values = tuple([str(val) for val in self.value])
            return str(values)

        if type(self.value) == dict:
            return ', '.join(["{} = {}".format(k, str(v)) for k,v in self.value.items()])

        if type(self.value) in (str, dt):
            return "'{}'".format(self.value)
        
        if type(self.value) == type(None):
            return 'NULL'

        return str(self.value)

