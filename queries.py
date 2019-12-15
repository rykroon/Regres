import copy

def to_sql(val):
    if type(val) in (list, tuple):
        val = [to_sql(i) for i in val]
        return "({})".format(', '.join(val))

    if type(val) == dict:
        # Each key:value pair is considered an expression.
        # The key is the left hand side of the expression.
        # The value is the right hand side of the expression.
        # The operator is assumed to be '='.
        # If the key is a tuple then the first position is the left hand side of the expression. 
        # If the key is a tuple then the second position is the operator.

        expressions = list()

        for key, val in val.items():
            left = key
            op = '='
            right = to_sql(val)

            if type(key) in (list, tuple):
                left = key[0]

                try:
                    op = key[1]
                    op_conv = dict(eq='=', ne='!=', lt='<', le='<=', gt='>', ge='>=')
                    op = op_conv.get(op, op)
                except IndexError as e:
                    pass
                    
            expr = "{} {} {}".format(left, op, right)
            expressions.append(expr)

        return ', '.join(expressions)

    if type(val) == str:
        return "'{}'".format(val)

    if type(val) == type(None):
        return 'NULL'

    return str(val)


class Asterisk(str):
    pass 


ASTERISK = Asterisk('*')


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
        args = args or [ASTERISK]
        super().__init__('SELECT', ', ', *args)


class FromClause(Clause):
    def __init__(self, table_name):
        super().__init__('FROM', '', table_name)


class WhereClause(Clause):
    def __init__(self, **kwargs):
        args = [{tuple(k.split('__')) : v} for k,v in kwargs.items()]
        super().__init__('WHERE', ' AND ', *args)


class OrderByClause(Clause):
    def __init__(self, *args):
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
        # Convert kwargs into a tuple of dictionaries since
        # the parent class does not take **kwargs
        args = [{k:v} for k,v in kwargs.items()]
        super().__init__('SET', ', ', *args)


class ReturningClause(Clause):
    def __init__(self, *args):
        args = args or [ASTERISK]
        super().__init__('RETURNING', ', ', *args)


class DeleteClause(Clause):
    def __init__(self):
        super().__init__('DELETE', '')


class Query():
    def __init__(self, table):
        self.table = table 
        self.clause_order = tuple()
        self.clause_mapping = dict()
        
    @property
    def sql(self):
        clauses = [self.clauses[c].to_sql() for c in self.clause_order if self.clauses.get(c) is not None]
        return ' '.join(clauses)


    def all(self):
        """
            Fetch all rows
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return cur.fetchall()

    def execute(self):
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return conn.commit()

    def one(self):
        """
            Fetch one row
        """
        with self.table.pool.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql)
                return cur.fetchone()


class SelectQuery(Query):
    def __init__(self, table):
        self.table = table
        self.clause_order = ('SELECT', 'FROM', 'WHERE', 'ORDER BY', 'LIMIT', 'OFFSET')
        self.clauses = {
            'SELECT' : SelectClause(), 
            'FROM' : FromClause(self.table), 
        }

    def select(self, *args):
        q = copy.copy(self)
        q.clauses['SELECT'] = SelectClause(*args)
        return q

    def where(self, **kwargs):
        q = copy.copy(self)
        q.clauses['WHERE'] = WhereClause(**kwargs)
        return q

    def order_by(self, *args):
        q = copy.copy(self)
        q.clauses['ORDER BY'] = OrderByClause(*args)
        return q

    def limit(self, count):
        q = copy.copy(self)
        q.clauses['LIMIT'] = LimitClause(count)
        return q

    def offset(self, start):
        q = copy.copy(self)
        q.clauses['OFFSET'] = OffsetClause(start)
        return q 


class InsertQuery(Query):
    def __init__(self, table):
        self.table = table 
        self.clause_order = ('INSERT', 'COLUMNS', 'VALUES', 'RETURNING')
        self.clauses = {
            'INSERT' : InsertClause(self.table),
        }

    def columns(self, *args):
        q = copy.copy(self)
        q.clauses['COLUMNS'] = ColumnsClause(*args)
        return q

    def columns_and_values(self, **kwargs):
        columns = list(kwargs.keys())
        values = list(kwarg.values())
        return self.columns(*columns).values(*values)

    def values(self, *args):
        q = copy.copy(self)
        q.clauses['VALUES'] = ValuesClause(*args)
        return q

    def returning(seld, *args):
        q = copy.copy(self)
        q.clauses['RETURNING'] = ReturningClause(*args)
        return q


class UpdateQuery(Query):
    def __init__(self, table):
        self.table = table
        self.clause_order = ('UPDATE', 'SET', 'WHERE', 'RETURNING')
        self.clauses = {
            'UPDATE' : UpdateClause(self.table),
        }

    def set(self, **kwargs):
        q = copy.copy(self)
        q.clauses['SET'] = SetClause(**kwargs)
        return q 

    def where(self, **kwargs):
        q = copy.copy(self)
        q.clauses['WHERE'] = WhereClause(**kwargs)
        return q 

    def returning(seld, *args):
        q = copy.copy(self)
        q.clauses['RETURNING'] = ReturningClause()
        return q


class DeleteQuery(Query):
    def __init__(self, table):
        self.table = table 
        self.clause_order = ('DELETE', 'FROM', 'WHERE')
        self.clauses = {
            'DELETE' : DeleteClause(),
            'FROM' : FromClause(self.table),
        }

    def where(self, **kwargs):
        q = copy.copy(self)
        q.clauses['WHERE'] = WhereClause(**kwargs)
        return q
