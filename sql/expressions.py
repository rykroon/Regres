from datetime import date, time, datetime, timedelta
from decimal import Decimal


class Value:
    def __init__(self, value):
        valid_types = (
            type(None), 
            bool, 
            float, 
            int,
            list, 
            str, 
            tuple, 
            date, 
            time, 
            datetime, 
            timedelta, 
            Decimal
        )

        if type(value) not in valid_types:
            raise TypeError("Invalid Type")
        
        self.value = value

    # potentially add parent class that contains all logical operators
    # eq, lt, gt, etc. 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.value)

    def __str__(self):
        return "%s"


class Expression:
    """
        An Expression is an sql string that can contain vars (Values)
    """
    def __init__(self, *args):
        self.args = args
        self.vars = tuple([arg.value for arg in args if type(arg) == Value])

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, '...')

    def __str__(self):
        return ' '.join([str(arg) for arg in args])


ASTERISK = Expression('*')


class Condition(Expression):
    """
        A Condition is  an Expression that resolves to a Boolean
    """

    def __and__(self, value):
        return Condition(self, 'AND', value)

    def __getitem__(self, item):
        return {
            'and': self.__and__,
            'or': self.__or__,
            'not': self.__invert__
        }[item]

    def __invert__(self):
        return Condition('NOT', self)

    def __or__(self, value):
        return Condition(self, 'OR', value)


class OutputExpression(Expression):
    def __init__(self, column, output_name=None):
        self.column = column 
        self.output_name = output_name

    def __str__(self):
        if self.output_name:
            return '{} AS "{}"'.format(str(self.column), self.output_name)
        else:
            return str(self.column)


class Function(Expression):
    def __init__(self, name, *args):
        self.name = name 
        self.args = args 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, '...')

    def __str__(self):
        args = ', '.join([str(arg) for arg in self.args])
        return '{}({})'.format(self.name, args)


class Count(Function):
    def __init__(self, *args):
        args = args or [ASTERISK]
        super().__init__('COUNT', *args)



