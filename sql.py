import copy
from datetime import datetime as dt 
from decimal import Decimal
from types import MethodType


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
        return "{}({})".format(self.__class__.__name__, self.name)

    def __str__(self):
        return '"{}"."{}"'.format(self.table.name, self.name)

    def asc(self):
        return Expression(self, 'ASC')

    def assign(self, value):
        col = '"{}"'.format(self.name)
        return Assignment(col, '=', Value(value))

    def desc(self):
        return Expression(self, 'DESC')

    def eq(self, value):
        return self == value 

    def ge(self, value):
        return self >= value

    def getattr(self, attr):
        try:
            return getattr(self, attr)
        except AttributeError:
            return None

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

    def getattr(self, attr):
        try:
            return getattr(self, attr)
        except AttributeError:
            return None

    def get_column(self, column_name):
        col = self.getattr(column_name)
        if col in self.columns:
            return col 
        return None

    def get_columns(self, *args):
        return [self.get_column(arg) for arg in args]

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
        self.clauses = dict()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, list(self.clauses.values()))

    def __str__(self):
        return ' '.join([str(self.clauses[c]) for c in self.clause_order if self.clauses.get(c) is not None])

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

        q = self.copy()
        q.clauses['RETURNING'] = ReturningClause(*args)
        return q


    def where(self, *args, **kwargs):
        """
            *args: Must be of type Condition
            **kwargs: Used to create Condition objects
        """
        
        if self.__class__ not in (SelectQuery, UpdateQuery, DeleteQuery):
            raise NotImplementedError

        all_args_are_conditions = all([type(arg) == Condition for arg in args])
        if not all_arg_are_conditions:
            raise TypeError("Args must be of type Condition")

        conditions = list(args)
        for key, val in kwargs.items():
            split = key.split('__')
            col = self.get_column(split[0])

            if len(split) > 1:
                method = col.getattr(split[1])
                conditions.append(method(val))
            else:
                conditions.append(col == val)

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

    def order_by(self, *args):
        exprs = list()

        """
            I am not 100% satisfied with this
            Is there any useful way to add **kwargs?
        """

        for arg in args:
            if type(arg) == str:
                split = arg.split('__')
                col = self.get_colum(split[0])
                if len(split) > 1:
                    expr = col.getattr(split[1])()
                else:
                    expr = col.asc()
            
            if type(arg) == OrderByExpression:
                expr = arg

            exprs.append(expr)

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

    def values(self, **kwargs):
        """
            Keys are columns
            Values are values
        """
        columns = list(kwargs.keys())
        values = [Value(v) for v in kwargs.values()]            

        q = self.copy()
        q.clauses['COLUMNS'] = ColumnsClause(columns)
        q.clauses['VALUES'] = ValuesClause(values)
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

    def set(self, *args, **kwargs):
        """
            *args: must be of type Assignment
            **kwargs: are used to create Assignment objects
        """
        all_args_are_assignments = all([type(arg) == Assignment for arg in args])
        if not all_args_are_assignments:
            raise TypeError("Args must be of type Assignment")

        args = list(args)
        for k, v in kwargs.items():
            col = self.get_column(k)
            expr = col.assign(v)
            args.append(expr)   
         
        q = copy.copy(self)
        q.clauses['SET'] = SetClause(*args)
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


"""
    Guidelines for writing Clauses
    - Clauses should be very short. Typically just a call to super().__init__()
    - should only take a single arg or *args, never **kwargs
"""


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


class Assignment(Expression):
    pass


class Condition(Expression):
    """
        An expression that resolves to a boolean
        Essentially a WhereCondition
    """

    def __and__(self, value):
        if type(value) == bool:
            value = Value(value)

        elif type(value) == Value:
            if type(value.value) != bool:
                value = Value(bool(value))

        return Condition(self, 'AND', value)


    def __or__(self, value):

        #...

        return Condition(self, 'OR', value)


class OrderByExpression(Expression):
    def nulls_first(self):
        pass

    def nulls_last(self):
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
            return ', '.join(["{} = {}".format(k, str(v)) for k,v in self.items()])

        if type(self.value) in (str, dt):
            return "'{}'".format(self.value)
        
        if type(self.value) == type(None):
            return 'NULL'

        return str(self.value)


