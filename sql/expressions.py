from datetime import datetime as dt
from decimal import Decimal


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


ASTERISK = Expression('*')


class Function(Expression):
    def __init__(self, func_name, *args):
        self.name = func_name 
        self.args = args

    def __str__(self):
        args = ', '.join([str(arg) for arg in self.args])
        return "{}({})".format(self.name, args)


class Count(Function):
    def __init__(self, *args):
        args = args or [ASTERISK]
        super().__init__('COUNT', *args)


class Distinct(Function):
    def __init__(self, *args):
        super().__init__('DISTINCT', *args)


class Assignment(Expression):
    """
        An Expression that represents the assignment of an expression to a column
        Used in the SET clause of an UPDATE query
    """
    def __init__(self, column, expression):
        self.column = column
        self.expression = expression
        super().__init__(self.column_name, '=', expression)

    @property
    def column_name(self):
        return '"{}"'.format(self.column.name)
    

class Condition(Expression):
    """
        
    """
    def __init__(self, left, operator, right):
        super().__init__(left, operator, right)

    def __and__(self, value):
        return Condition(self, 'AND', value)

    def __getitem__(self, item):
        return {
            'and': self.__and__,
            'or': self.__or__,
            'not': self.__neg__
        }[item]

    def __neg__(self):
        return Condition('NOT', str(self))

    def __or__(self, value):
        return Condition(self, 'OR', val)


class OutputExpression(Expression):
    def __init__(self, column, output_name=None):
        if output_name:
            output_name = '"{}"'.format(output_name)
            super().__init__(column, 'AS', output_name)
        else:
            super().__init__(column)


class Value():
    """
        SQL Value

        !! Since it is apparently best practice to use the '%s' syntax and passing the args
        separately for values I may just deprecate this class and add logic in the Expression class
        that will look out for any python types and act accordingly.
    """
    def __init__(self, value):
        valid_types = (bool, float, int, str, type(None), list, tuple, dt, Decimal)
        if type(value) not in valid_types:
            print(type(value))
            raise TypeError("Invalid Type")
        
        self.value = value

    # potentially add parent class that contains all logical operators
    # eq, lt, gt, etc. 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.value)

    def __str__(self):
        if type(self.value) in (list, tuple):
            values = "({})".format(', '.join([str(val) for val in self.value]))
            return str(values)

        if type(self.value) in (str, dt):
            return "'{}'".format(self.value)
        
        if type(self.value) == type(None):
            return 'NULL'

        return str(self.value)

