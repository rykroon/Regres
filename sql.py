import copy


class Column:
    """
        SQL Column
    """
    def __init__(self, name, table):
        self.name = name 
        self.table = table

    def __eq__(self, value):
        return Expression(self, '=', to_sql(value)) 

    def __ge__(self, value):
        return Expression(self, '>=', to_sql(value)) 

    def __gt__(self, value):
        return Expression(self, '>', to_sql(value)) 

    def __le__(self, value):
        return Expression(self, '<=', to_sql(value)) 

    def __lt__(self, value):
        return Expression(self, '<', to_sql(value)) 

    def __ne__(self, value):
        return Expression(self, '!=', to_sql(value)) 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self.name))

    def __str__(self):
        return '"{}"."{}"'.format(self.table.name, self.name)

    def asc(self):
        return Expression(self, 'ASC')

    def desc(self):
        return Expression(self, 'DESC')
        
        
class Table:
    """
        SQL Table
    """

    def __init__(self, name, pool, schema='public'):
        self.schema = schema
        self.name = name
        self.pool = pool

        with self.pool.getconn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT a.column_name, c.constraint_type 
                        FROM information_schema.columns AS a
                            LEFT JOIN information_schema.key_column_usage AS b
                                ON a.table_schema = b.table_schema 
                                AND a.table_name = b.table_name
                                AND a.column_name = b.column_name
                            LEFT JOIN information_schema.table_constraints AS c
                                ON b.table_schema = c.table_schema
                                AND b.table_name = c.table_name
                                AND b.constraint_name = c.constraint_name
                        WHERE a.table_schema = '{}' AND a.table_name = '{}'
                        ORDER BY a.ordinal_position
                """.format(self.schema, self.name)
                cur.execute(sql)
                rows = cur.fetchall()
                
                if rows:
                    self._columns = list()
                    for row in rows:
                        col_name = row[0]
                        col = Column(col_name, self)
                        
                        setattr(self, col_name, col)
                        self._columns.append(col)

                        if row[1] == 'PRIMARY KEY':
                            self._primary_key = col

                    self._columns = tuple(self._columns)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self.name))

    def __str__(self):
        return '"{}"."{}"'.format(self.schema, self.name)

    @property
    def columns(self):
        return self._columns 

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def query(self):
        return SelectQuery(self)


"""
    -- Queries --
"""


class Query:
    """
        Base Query
    """

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

    def copy(self):
        return copy.copy(self)

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
    """
        SELECT Query
    """

    def __init__(self, table):
        self.table = table
        self.clause_order = ('SELECT', 'FROM', 'WHERE', 'ORDER BY', 'LIMIT', 'OFFSET')
        self.clauses = {
            'SELECT' : SelectClause(), 
            'FROM' : FromClause(self.table), 
        }

    def select(self, *args):
        q = self.copy()
        q.clauses['SELECT'] = SelectClause(*args)
        return q

    def where(self, **kwargs):
        q = self.copy()
        q.clauses['WHERE'] = WhereClause(**kwargs)
        return q

    def order_by(self, *args):
        q = self.copy()
        q.clauses['ORDER BY'] = OrderByClause(*args)
        return q

    def limit(self, count):
        q = self.copy()
        q.clauses['LIMIT'] = LimitClause(count)
        return q

    def offset(self, start):
        q = self.copy()
        q.clauses['OFFSET'] = OffsetClause(start)
        return q 


class InsertQuery(Query):
    """
        Insert Query
    """

    def __init__(self, table):
        self.table = table 
        self.clause_order = ('INSERT', 'COLUMNS', 'VALUES', 'RETURNING')
        self.clauses = {
            'INSERT' : InsertClause(self.table),
        }

    def columns(self, *args):
        q = self.copy()
        q.clauses['COLUMNS'] = ColumnsClause(*args)
        return q

    def columns_and_values(self, **kwargs):
        columns = list(kwargs.keys())
        values = list(kwarg.values())
        return self.columns(*columns).values(*values)

    def values(self, *args):
        q = self.copy()
        q.clauses['VALUES'] = ValuesClause(*args)
        return q

    def returning(seld, *args):
        q = self.copy()
        q.clauses['RETURNING'] = ReturningClause(*args)
        return q


class UpdateQuery(Query):
    """
        UPDATE Query
    """

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
    """
        DELETE Query
    """

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


"""
    -- sql --
"""


def to_sql(obj):
    if type(obj) in (list, tuple):
        # Treat a Python list as  an SQL list.
        # Comma separated values in between parenthesis.
        values = [to_sql(val) for val in obj]
        return "({})".format(', '.join(values))

    # if type(val) == tuple:
    #     # Each element in a tuple is treated like a piece of an
    #     # expression to be separated by a whitespace.
    #     val = [to_sql(arg) for arg in val]
    #     return ' '.join(val)

    if type(obj) == dict:
        # Each key:value pair is considered an expression.
        # The key is the left hand side of the expression.
        # The value is the right hand side of the expression.
        # The operator is assumed to be '='.

        expressions = ["{} = {}".format(k, to_sql(v)) for k,v in obj.items()]

        return ', '.join(expressions)

    if type(obj) == str:
        return "'{}'".format(obj)

    if type(obj) == type(None):
        return 'NULL'

    return str(obj)


"""
    -- Misc. --
"""


class Asterisk:
    def __str__(self):
        return '*'


class Operator(str):
    def __str__(self):
        conv = dict(eq='=', ne='!=', lt='<', le='<=', gt='>', ge='>=')
        return conv.get(self, self.upper())


class Expression:
    def __init__(self, *args):
        self.args = args

    def __repr__(self):
        expr = ' '.join([repr(arg) for arg in self.args])
        return "{}({})".format(self.__class__.__name__, expr)

    def __str__(self):
        return ' '.join([str(arg) for arg in self.args])


class WhereExpression(Expression):
    pass 


class OrderByExpression(Expression):
    pass


"""
    -- Clauses --
"""


class Clause:
    def __init__(self, clause, delimiter, *args):
        self.args = args
        self.clause = clause 
        self.delimiter = delimiter
        self.format = ''

        if self.args:
            self.format = self.clause + ' ' + self.delimiter.join(['{}'] * len(self.args))

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self.clause))

    def to_sql(self):
        args = [to_sql(arg) for arg in self.args]
        return self.format.format(*args)


class SelectClause(Clause):
    """
        SELECT Clause
    """
    def __init__(self, *args):
        args = [Expression(arg) if type(arg) == str else arg for arg in args]
        args = args or [Asterisk()]
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
    def __init__(self, **kwargs):
        """
            ideas:
                ! add *args, all args must be a (Where)Expression, else raise type error

        """
        args = list()
        for k, v in kwargs.items():
            if '__' not in k:
                args.append({k:v})
            else:
                left, op = k.split('__')
                expr = Expression(left, Operator(op), to_sql(v))
                args.append(expr)
                
        super().__init__('WHERE', ' AND ', *args)


class OrderByClause(Clause):
    def __init__(self, *args):
        """
            ! Add type checking so that args must be a Column or an OrderByExpression

        """
        new_args = list()
        for arg in args:
            if type(arg) == Expression:
                new_args.append(arg)
            else:
                new_args.append(Expression(*arg.split('__')))

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

        # Being picky with the type checking here because the  
        # Column clause an ONLY have columns.
        all_columns = all([True if type(arg) == Column else False for arg in args])
        if not all_columns:
            raise TypeError("All args must be of type Column")

        # Need to do this because in an INSERT statement it is invalid syntax to
        # include the qualified table name ex: "table"."column", so I just need the names 
        # of the column
        args = [col.name for col in args]

        # Not unpacking the args on purpose
        super().__init__('', '', args)


class ValuesClause(Clause):
    def __init__(self, *args):
        # Not unpacking the args on purpose
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
        args = [Expression(arg) for arg in args]
        args = args or [Asterisk()]
        super().__init__('RETURNING', ', ', *args)


class DeleteClause(Clause):
    def __init__(self):
        super().__init__('DELETE', '')
