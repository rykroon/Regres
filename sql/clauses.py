from .expressions import *
from .columns import Column
#from .tables import Table

class Clause:
    def __init__(self, clause, delimiter, *args):
        self.exprs = args
        self.clause = clause 
        self.delimiter = delimiter
        self.format = ''

        if self.exprs:
            self.format = self.clause + ' ' + self.delimiter.join(['{}'] * len(self.exprs))

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.exprs)

    def __str__(self):
        exprs = [str(expr) for expr in self.exprs]
        return self.format.format(*exprs)


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
        all_args_are_conditions = all([type(arg) == Condition for arg in args])
        if not all_args_are_conditions:
            raise TypeError("args must be of type 'Condition'")

        super().__init__('WHERE', ' AND ', *args)


class OrderByClause(Clause):
    def __init__(self, *args):
        all_args_are_expressions = all([type(arg) == Expression for arg in args])
        if not all_args_are_expressions:
            raise TypeError("args must be of type 'Expression'")

        super().__init__('ORDER BY', ', ', *args)


class LimitClause(Clause):
    def __init__(self, count):
        if type(count) != int:
            raise TypeError("count must be of type 'int'")
        super().__init__('LIMIT', '', count)


class OffsetClause(Clause):
    def __init__(self, start):
        if type(start) != int:
            raise TypeError("start must be of type 'int'")
        super().__init__('OFFSET', '', start)


class InsertClause(Clause):
    def __init__(self, table):
        #if type(table) != Table:
        #    raise TypeError("table must be of type 'Table'")
        super().__init__('INSERT INTO', '', table.name)


class ColumnsClause(Clause):
    def __init__(self, *args):        
        all_args_are_columns = all([type(arg) == Column for arg in args])
        if not all_args_are_columns:
            raise TypeError("args must be of type 'Column'")

        columns = ['"{}"'.format(col.name) for col in args]
        # Not unpacking the args on purpose
        super().__init__('', '', Value(columns))


class ValuesClause(Clause):
    def __init__(self, *args):

        # Not unpacking the args on purpose
        super().__init__('VALUES', '', Value(args))


class UpdateClause(Clause):
    def __init__(self, table):
        #if type(table) != Table:
        #    raise TypeError("table must be of type 'Table'")
        super().__init__('UPDATE', '', table.name)


class SetClause(Clause):
    def __init__(self, *args):
        all_args_are_assignments = all([type(arg) == Assignment for arg in args])
        if not all_args_are_assignments:
            raise TypeError("args must be of type 'Assignment'")

        super().__init__('SET', ', ', *args)


class ReturningClause(Clause):
    def __init__(self, *args):
        if args:
            all_args_are_output_expressions = all([type(arg) == OutputExpression for arg in args])
            if not all_args_are_output_expressions:
                raise TypeError("args must be of type 'OutputExpression'")
        else:
            args = [ASTERISK]
        super().__init__('RETURNING', ', ', *args)


class DeleteClause(Clause):
    def __init__(self):
        super().__init__('DELETE', '')

