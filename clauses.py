from .expressions import *

def to_sql(val):
    if type(val) == list:
        # Treat a Python list as  an SQL list.
        # Comma separated values in between parenthesis.
        val = [to_sql(i) for i in val]
        return "({})".format(', '.join(val))

    if type(val) == tuple:
        # Each element in a tuple is treated like a piece of an
        # expression to be separated by a whitespace.
        val = [to_sql(arg) for arg in val]
        return ' '.join(val)

    if type(val) == dict:
        # Each key:value pair is considered an expression.
        # The key is the left hand side of the expression.
        # The value is the right hand side of the expression.
        # The operator is assumed to be '='.

        expressions = ["{} = {}".format(k, to_sql(v)) for k,v in val.items()]

        return ', '.join(expressions)

    if type(val) == str:
        return "'{}'".format(val)

    if type(val) == type(None):
        return 'NULL'

    return str(val)


class Clause:
    def __init__(self, clause, delimiter, *args):
        self.args = args
        self.clause = clause 
        self.delimiter = delimiter
        self.format = ''

        if self.args:
            self.format = self.clause + ' ' + self.delimiter.join(['{}'] * len(self.args))

    def to_sql(self):
        args = [to_sql(arg) for arg in self.args]
        return self.format.format(*args)


class SelectClause(Clause):
    def __init__(self, *args):
        args = args or [Asterisk()]
        super().__init__('SELECT', ', ', *args)


class FromClause(Clause):
    def __init__(self, table_name):
        super().__init__('FROM', '', table_name)


class WhereClause(Clause):
    def __init__(self, **kwargs):
        args = list()
        for k, v in kwargs.items():
            if '__' not in k:
                args.append({k:v})
            else:
                left, op = k.split('__')
                op_conv = dict(eq='=', ne='!=', lt='<', le='<=', gt='>', ge='>=')
                op = op_conv.get(op, op).upper()
                args.append((left, op, v))
                
        super().__init__('WHERE', ' AND ', *args)


class OrderByClause(Clause):
    def __init__(self, *args):
        new_args = list()
        for arg in args:
            if '__' in arg:
                field, order = arg.split('__')
                new_args.append((field, order.upper()))
            else:
                new_args.append(arg)

        super().__init__('ORDER BY', ', ', *new_args)


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
        # Since the columns are enclosed in parenthesis I am not 
        # unpacking the args in the call to super().__init__()
        super().__init__('', '', args)


class ValuesClause(Clause):
    def __init__(self, *args):
        # Since the values are enclosed in parenthesis I am not 
        # unpacking the args in the call to super().__init__()
        super().__init__('VALUES', '', args)


class UpdateClause(Clause):
    def __init__(self, table_name):
        super().__init__('UPDATE', '', table_name)


class SetClause(Clause):
    def __init__(self, **kwargs):
        # Convert kwargs into a list of dictionaries
        args = [{k:v} for k,v in kwargs.items()]
        super().__init__('SET', ', ', *args)


class ReturningClause(Clause):
    def __init__(self, *args):
        args = args or [Asterisk()]
        super().__init__('RETURNING', ', ', *args)


class DeleteClause(Clause):
    def __init__(self):
        super().__init__('DELETE', '')