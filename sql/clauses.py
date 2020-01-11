from .columns import Column
from .expressions import *


class Clause:
    def __init__(self, name, delimiter, *args):
        """
            @param name: The name of the Clause
            @param delimiter: The delimiter for expressions
            @param *args: The expressions
        """
        self.name = name 
        self.delimiter = delimiter
        self.exprs = args

        self.vars = list()
        for expr in self.exprs:
            if issubclass(type(expr), Expression):
                self.vars.extend(expr.vars)
            elif type(expr) == Value:
                self.vars.append(expr.value)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, '...')

    def __str__(self):
        exprs = [str(expr) for expr in self.exprs]
        return self.format().format(*exprs)

    def format(self):
        substitutions = self.delimiter.join(['{}'] * len(self.exprs))
        return "{} {}".format(self.name, substitutions)


"""
    Guidelines for writing Clauses
    - Clauses are strict when it comes to typing.
    - Should only take a single arg or *args, never **kwargs
"""


class SelectClause(Clause):
    """
        SELECT Clause
    """
    def __init__(self, *args):
        args = args or [ASTERISK]
        type_check_args(args, Exrepssion)
        super().__init__('SELECT', ', ', *args)


class FromClause(Clause):
    """
        FROM Clause
    """
    def __init__(self, table):
        super().__init__('FROM', '', table)


class WhereClause(Clause):
    """
        WHERE Clause
    """
    def __init__(self, condition):        
        if type(condition) != Condition:
            raise TypeError("condition must be of type '{}'".format(Condition.__name__))
        super().__init__('WHERE', '', condition)


class OrderByClause(Clause):
    def __init__(self, *args):
        if not all([issubclass(type(arg), Expression) for arg in args]):
            raise TypeError("*args must be of type '{}'".format(Expression.__name__))

        type_check_args(args, valid_types=Expression)
        super().__init__('ORDER BY', ', ', *args)


class LimitClause(Clause):
    def __init__(self, count):
        if type(count) != int:
            raise TypeError("count must be of type '{}'".format(int.__name__))

        super().__init__('LIMIT', '', count)


class OffsetClause(Clause):
    def __init__(self, start):
        if type(count) != int:
            raise TypeError("start must be of type '{}'".format(int.__name__))
        super().__init__('OFFSET', '', start)


