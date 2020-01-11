import copy

from .columns import Column
from .queries import SelectQuery

class Table:
    """
        SQL Table
    """

    def __init__(self, name, pool, schema='public'):
        self._schema = schema
        self._name = name
        self.pool = pool
        self.alias = None

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
                        ORDER BY a.ordinal_position ASC
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

    def __contains__(self, key):
        """
            @param key: The name of a column
            @returns: True if the column is in the table
        """
        return key in self.column_names

    def __getitem__(self, item):
        """
            @param item: The name of a column.
            @returns: The column.
        """
        if item in self:
            return getattr(self, item)

        raise KeyError("column '{}' not found.".format(item))

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        return len(self.columns)

    def __next__(self):
        return next(self.column)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def __str__(self):
        table_name = '"{}"."{}"'.format(self.schema, self.name)
        if self.alias:
            table_name = '{} AS "{}"'.format(table_name, self.alias)
        return table_name

    @property
    def columns(self):
        return self._columns 
    
    @property
    def column_names(self): 
        return tuple([col.name for col in self])

    @property
    def qualified_column_names(self):
        return tuple([col.qualified_name for col in self])

    @property
    def name(self):
        return self._name

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def schema(self):
        return self._schema

    def as_(self, alias):
        """
            @param alias: An alias for the table.
            @returns: A copy of the table. The new table has the alias.
        """
        t = self.copy()
        t.alias = alias 
        return t

    def copy(self):
        return copy.copy(self)

    def query(self):
        return SelectQuery(self)


