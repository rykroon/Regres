from .queries import SelectQuery


class Column:
    def __init__(self, name):
        self.name = name 

    def __str__(self):
        return '"{}"'.format(self.name)
        
        
class Table:

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
                    self.columns = tuple([Column(row[0]) for row in rows])
                    self._primary_key = [Column(row[0]) for row in rows if row[1]=='PRIMARY KEY'][0]

    def __str__(self):
        return '"{}"."{}"'.format(self.schema, self.name)

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def query(self):
        return SelectQuery(self)
