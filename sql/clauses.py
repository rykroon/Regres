from expressions import *

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
        expres = [str(expr) for expr in self.exprs]
        return self.format.format(*exprs)


"""
    Guidelines for writing Clauses
    - Clauses should be very short. Typically just a call to super().__init__()
    - should only take a single arg or *args, never **kwargs

    ! - maybe add type checking to the clauses since ultimately it is the resposibility of 
    ! the clause to make sure it has the correct expressions.
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
    def __init__(self, table_name):
        super().__init__('FROM', '', table_name)


class WhereClause(Clause):
    """
        WHERE Clause
    """
    def __init__(self, *args):        
        all_args_are_conditions = all([type(arg) == Condition for arg in args])
        if not all_arg_are_conditions:
            raise TypeError("Args must be of type Condition")

        super().__init__('WHERE', ' AND ', *args)


class OrderByClause(Clause):
    def __init__(self, *args):
        all_args_are_expressions = all([type(arg) == Expression for arg in args])
        if not all_args_are_expressions:
            raise TypeError("Args must be of type OrderByExpression")

        super().__init__('ORDER BY', ', ', *args)


class LimitClause(Clause):
    def __init__(self, count):
        super().__init__('LIMIT', '', count)


class OffsetClause(Clause):
    def __init__(self, start):
        super().__init__('OFFSET', '', start)


class InsertClause(Clause):
    def __init__(self, table_name):
        super().__init__('INSERT INTO', '', table_name)


class ColumnsClause(Clause):
    def __init__(self, *args):        
        # Not unpacking the args on purpose
        super().__init__('', '', Value(args))


class ValuesClause(Clause):
    def __init__(self, *args):
        # Not unpacking the args on purpose
        super().__init__('VALUES', '', Value(args))


class UpdateClause(Clause):
    def __init__(self, table_name):
        super().__init__('UPDATE', '', table_name)


class SetClause(Clause):
    def __init__(self, *args):
        all_args_are_assignments = all([type(arg) == Assignment for arg in args])
        if not all_args_are_assignments:
            raise TypeError("args must be of type 'Assignment'")

        super().__init__('SET', ', ', *args)


class ReturningClause(Clause):
    def __init__(self, *args):
        args = args or [ASTERISK]
        super().__init__('RETURNING', ', ', *args)


class DeleteClause(Clause):
    def __init__(self):
        super().__init__('DELETE', '')

