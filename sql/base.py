

class SQL:
    def __init__(self, string):
        self.sql = str(string)

    def __repr__(self):
        return "{}('{}')".format(self.__class__.__name__, self.sql)

    def __str__(self):
        return self.sql


ASTERISK = SQL('*')


class ColumnName(SQL):
    pass 


class OutputExpression(SQL):
    pass 


class TableName(SQL):
    pass 


class Expression(SQL):
    """
        An Expression is an sql string that can contain vars (Values)
    """
    def __init__(self, *args):
        sql = ' '.join([str(arg) for arg in args])
        super().__init__(sql)
        self.vars = tuple([arg for arg in args if type(arg) == Value])


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
            'not': self.__not__
        }[item]

    def __invert__(self):
        return Condition('NOT', self)

    def __or__(self, value):
        return Condition(self, 'OR', value)


class Value(SQL):
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

