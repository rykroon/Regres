import copy
from .columns import Column
from .queries import SelectQuery

class Table:
    def __init__(self, name, pool, schema='public'):
        self._schema = schema
        self._name = name
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

    def __contains__(self, column):
        return column in self.columns

    def __getitem__(self, column_name):
        return getattr(self, column_name)

    def __iter__(self):
        return iter(self.columns)

    def __len__(self):
        return len(self.columns)

    def __next__(self):
        return next(self.columns)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._name)

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
        return SelectQuery(self)


