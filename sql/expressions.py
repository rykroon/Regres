from datetime import date, time, datetime, timedelta
from decimal import Decimal


class Value():
    """
        SQL Value
    """
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
        Expressions are made of args.
        The args are joined with spaces to create an expression
    """
    def __init__(self, *args):
        self.vars = list()
        self.args = list()

        for arg in args:
            if type(arg) == Value:
                self.vars.append(arg.value)
            self.args.append(arg)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.args)

    def __str__(self):
        return ' '.join([str(arg) for arg in self.args])


ASTERISK = Expression('*')


class Function(Expression):
    def __init__(self, func_name, *args):
        self.name = func_name 
        super().__init__(args)

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
    def __init__(self, column, value):
        if type(value) != Value:
            raise TypeError("value must be of type 'Value'")

        self.column = column
        self.value = value
        super().__init__(self.column_name, '=', value)

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
            'not': self.__invert__
        }[item]

    def __invert__(self):
        #fix this later
        return Condition('NOT', self)

    def __or__(self, value):
        return Condition(self, 'OR', value)


class OutputExpression(Expression):
    def __init__(self, column, output_name=None):
        if output_name:
            output_name = '"{}"'.format(output_name)
            super().__init__(column, 'AS', output_name)
        else:
            super().__init__(column)




