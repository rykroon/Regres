import copy

from psycopg2.extensions import AsIs, adapt, register_adapter

from .columns import Column
from .queries import Query

class Table:
    def __init__(self, name, pool, schema='public'):
        self._schema = schema #maybe change to table_schema
        self._name = name # maybe change to table_name
        self._pool = pool

        query = """
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
                WHERE a.table_schema = %s AND a.table_name = %s
                ORDER BY a.ordinal_position ASC
        """

        rows = self._pool.fetchall(query, (self._schema, self._name))

        if not rows:
            raise Exception("Table '{}' does not exist.".format(self._name))
        
        if rows:
            self._columns = list()
            for row in rows:
                col_name = row[0]
                col = Column(col_name, self)
                
                setattr(self, col._attr_name, col)
                self._columns.append(col)

                if row[1] == 'PRIMARY KEY':
                    self._primary_key = col

            self._columns = tuple(self._columns)

    def __contains__(self, item):
        return item in self.columns

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        return len(self.columns)

    def __next__(self):
        return next(self.columns)

    def __repr__(self):
        return "{}(name={})".format(self.__class__.__name__, repr(self._name))

    def __str__(self):
        return '"{}"."{}"'.format(self._schema, self._name)

    @property
    def columns(self):
        return self._columns 
    
    @property
    def column_names(self): 
        return tuple([col.name for col in self])

    @property
    def primary_key(self):
        return self._primary_key

    def query(self):
        return Query(self)


def adapt_table(table):
    return AsIs(str(table))


register_adapter(Table, adapt_table)


