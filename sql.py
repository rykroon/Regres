import copy
from datetime import datetime as dt 
from decimal import Decimal

class Column:
    """
        SQL Column
    """
    def __init__(self, name, table):
        self.name = name 
        self.table = table

    def __eq__(self, value):
        return Condition(self, '=', Value(value)) 

    def __ge__(self, value):
        return Condition(self, '>=', Value(value)) 

    def __gt__(self, value):
        return Condition(self, '>', Value(value)) 

    def __le__(self, value):
        return Condition(self, '<=', Value(value)) 

    def __lt__(self, value):
        return Condition(self, '<', Value(value)) 

    def __ne__(self, value):
        return Condition(self, '!=', Value(value)) 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self.name))

    def __str__(self):
        return '"{}"."{}"'.format(self.table.name, self.name)

    def asc(self):
        return Expression(self, 'ASC')

    def desc(self):
        return Expression(self, 'DESC')

    def eq(self, value):
        return self == value 

    def ge(self, value):
        return self >= value

    def gt(self, value):
        return self > value 

    def le(self, value):
        return self <= value 

    def like(self, value):
        return Condition(self, 'LIKE', Value(value))

    def lt(self, value):
        return self < value

    def ne(self, value):
        return self != value 

    def call_method(self, method_name, value):
        method = getattr(self, method_name)
        return method(value)
        
        
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

    def query(self):
        return SelectQuery(self)

    def get_column(self, column_name):
        try:
            return getattr(self, column_name)
        except AttributeError:
            return None

    def get_columns(self, *args):
        return [self.get_column(arg) for arg in args]


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
        self.clauses = dict()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, list(self.clauses.values()))

    def __str__(self):
        return ' '.join([str(self.clauses[c]) for c in self.clause_order if self.clauses.get(c) is not None])

    def _resolve_string(self, string):
        """
            
        """
        result = None

        for idx, val in enumerate(string.split('__')):
            if idx == 0: #check if there is a column with this name
                result = self.table.get_column(val)
                if result is None: break
            else:
                result = getattr(result, val)        

        return result or string


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

    def returning(*args):
        if self.__class__ not in (InsertQuery, UpdateQuery, DeleteQuery):
            raise NotImplementedError

        #

        q = self.copy()
        q.clauses['RETURNING'] = ReturningClause()


    def where(self, *args, **kwargs):
        if self.__class__ not in (SelectQuery, UpdateQuery, DeleteQuery):
            raise NotImplementedError

        conditions = list(args)
        for key, val in kwargs.items()
            x = self._resolve_string(key)
            if type(x) == Column:
                conditions.append(x == v)
            else:
                conditions.append(x)

        q = self.copy()
        q.clauses['WHERE'] = WhereClause(*conditions)
        return q



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
        exprs = list()

        for k, v in kwargs.items():
            if '__' in k:
                col, method = k.split('__')
                expr = self.table.get_column(col).call_method(method, v)
                exprs.append(expr)
            else:
                exprs.append(Expression(k, '=', Value(v)))

        q = self.copy()
        q.clauses['WHERE'] = WhereClause(*exprs)
        return q

    def order_by(self, *args):
        exprs = list()

        for arg in args:
            if type(arg) == str and '__' in arg:
                col, method = arg.split('__')
                exprs.append(self.table.get_column(col).call_method(method))
            exprs.append(arg)

        q = self.copy()
        q.clauses['ORDER BY'] = OrderByClause(*exprs)
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

    def values(self, *args, **kwargs):
        """
            ! add logic for kwargs
        """
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


class DeleteQuery(Query):
    """
        DELETE Query
    """

    def __init__(self, table):
        self.table = table 
        self.clause_order = ('DELETE', 'FROM', 'WHERE', 'RETURNING')
        self.clauses = {
            'DELETE' : DeleteClause(),
            'FROM' : FromClause(self.table),
        }


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
        return "{}({})".format(self.__class__.__name__, self.args)

    def __str__(self):
        args = [str(arg) for arg in self.args]
        return self.format.format(*args)


class SelectClause(Clause):
    """
        SELECT Clause
    """
    def __init__(self, *args):
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
    def __init__(self, *args):                
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
        # Need to do this because in an INSERT statement it is invalid syntax to
        # include the qualified table name ex: "table"."column", so I just need the names
        args = ['"{}"'.format(arg.name) if type(arg) == Column else arg for arg in args]
        
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
    def __init__(self, **kwargs):
        # Convert kwargs into a list of dictionaries
        args = [{k:Value(v)} for k,v in kwargs.items()]
        super().__init__('SET', ', ', *args)


class ReturningClause(Clause):
    def __init__(self, *args):
        args = args or [Asterisk()]
        super().__init__('RETURNING', ', ', *args)


class DeleteClause(Clause):
    def __init__(self):
        super().__init__('DELETE', '')


"""
    Expressions
"""


def to_sql(obj):
    """
        Deprecate
    """

    if type(obj) in (list, tuple):
        # Treat a Python list as  an SQL list.
        # Comma separated values in between parenthesis.
        values = [to_sql(val) for val in obj]
        return "({})".format(', '.join(values))

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
    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, '*')

    def __str__(self):
        return '*'

class Expression:
    def __init__(self, *args):
        self.args = args

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.args)

    def __str__(self):
        return ' '.join([str(arg) for arg in self.args])


class Condition(Expression):
    """
        An expression that resolves to a boolean
        Essentially a WhereCondition
    """

    def __and__(self, value):
        pass

    def __or__(self, value):
        pass


class WhereExpression(Expression):
    pass 


class OrderByExpression(Expression):
    pass


class Value():
    """
        SQL Value
    """
    def __init__(self, value):
        valid_types = (bool, int, float, str, list, tuple, dict, dt, Decimal, Value)
        if type(value) not in valid_types:
            raise TypeError("Invalid Type")
        
        if type(value) == Value:
            self.value = value.value
        else:
            self.value = value

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self.value))

    def __str__(self):
        if type(self.value) in (list, tuple):
            values = tuple([str(val) for val in self])
            return str(values)

        if type(self.value) == dict:
            return ', '.join(["{} = {}".format(k, v) for k,v in self.items()])

        if type(self.value) in (str, dt):
            return "'{}'".format(self.value)
        
        if type(self.value) == type(None):
            return 'NULL'


        return str(self.value)


