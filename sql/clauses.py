from .expressions import *
from .columns import Column
#from .tables import Table


def type_check_args(args, valid_types):
    if type(valid_types) not in (list, tuple):
        valid_types = [valid_types]

    for arg in args:
        if type(arg) not in valid_types:
            raise TypeError("arg '{}' must be of type '{}'".format(arg, valid_types))


class Clause:
    def __init__(self, name, delimiter, *args):
        """
            @param name: The name of the Clause
            @param delimiter: The delimiter for expressions
            @param *args: The expressions
        """
        self.name = name 
        self.exprs = args
        self.delimiter = delimiter
        self.values = list()
        for expr in self.exprs:
            if issubclass(type(expr), Expression):
                self.values.extend(expr.values)
            elif type(expr) == Value:
                self.values.append(expr.value)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.exprs)

    def __str__(self):
        exprs = [str(expr) for expr in self.exprs]
        return self.format().format(*exprs)

    def format(self):
        substitutions = self.delimiter.join(['{}'] * len(self.exprs))
        return "{} {}".format(self.name, substitutions)


"""
    Guidelines for writing Clauses
    - Clauses are strict when it comes to typing.
    - should only take a single arg or *args, never **kwargs
"""


class SelectClause(Clause):
    """
        SELECT Clause
    """
    def __init__(self, *args):
        args = args or [ASTERISK]
        super().__init__('SELECT', ', ', *args)


class FromClause(Clause):
    """
        FROM Clause
    """
    def __init__(self, table):
        #if type(table) != Table:
        #    raise TypeError("table must be of type 'Table'")
        super().__init__('FROM', '', table.name)


class WhereClause(Clause):
    """
        WHERE Clause
    """
    def __init__(self, *args):        
        type_check_args(args, valid_types=Condition)

        condition = args[0]
        for c in args[1:]:
            condition = condition & c

        super().__init__('WHERE', '', condition)


class OrderByClause(Clause):
    def __init__(self, *args):
        type_check_args(args, valid_types=Expression)

        super().__init__('ORDER BY', ', ', *args)


class LimitClause(Clause):
    def __init__(self, count):
        type_check_args((count), valid_types=int)
        super().__init__('LIMIT', '', count)


class OffsetClause(Clause):
    def __init__(self, start):
        type_check_args((start), valid_types=int)
        super().__init__('OFFSET', '', start)


class InsertClause(Clause):
    def __init__(self, table):
        #if type(table) != Table:
        #    raise TypeError("table must be of type 'Table'")
        super().__init__('INSERT INTO', '', table.name)


class ColumnsClause(Clause):
    def __init__(self, *args):        
        type_check_args(args, valid_types=Column)

        columns = ['"{}"'.format(col.name) for col in args]
        super().__init__('COLUMNS', ', ', *columns)

    def format(self):
        substitutions = self.delimiter.join(['{}'] * len(self.exprs))
        return "({})".format(substitutions)


class ValuesClause(Clause):
    def __init__(self, *args):
        type_check_args(args, valid_types=Value)
        super().__init__('VALUES', ', ', *args)

    def format(self):
        substitutions = self.delimiter.join(['{}'] * len(self.exprs))
        return "{}({})".format(self.name, substitutions)


class UpdateClause(Clause):
    def __init__(self, table):
        #if type(table) != Table:
        #    raise TypeError("table must be of type 'Table'")
        super().__init__('UPDATE', '', table.name)


class SetClause(Clause):
    def __init__(self, *args):
        type_check_args(args, valid_types=Assignment)
        super().__init__('SET', ', ', *args)


class ReturningClause(Clause):
    def __init__(self, *args):
        if args:
            type_check_args(args, valid_types=OutputExpression)
        else:
            args = [ASTERISK]
        super().__init__('RETURNING', ', ', *args)


class DeleteClause(Clause):
    def __init__(self):
        super().__init__('DELETE', '')

