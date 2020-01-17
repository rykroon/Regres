from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool as SCP
from psycopg2.pool import ThreadedConnectionPool as TCP


class SimpleConnectionPool(SCP):

    def __init__(self, minconn, maxconn, database='postgres', user='postgres', host='localhost', *args, **kwargs):
        super().__init__(minconn, maxconn, database=database, user=user, host=host, *args, **kwargs)
    
    @contextmanager
    def getconn(self):
        try:
            conn = super().getconn()
            yield conn
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            self.putconn(conn)  

    @contextmanager
    def cursor(self):
        with self.getconn() as conn:
            try:
                cur = conn.cursor()
                yield cur 
            except:
                raise
            finally:
                cur.close()

    def execute(self, query, vars=None):
        with self.cursor() as cur:
            cur.execute(query, vars)

    def fetchall(self, query, vars=None):
        with self.cursor() as cur:
            cur.execute(query, vars)
            return cur.fetchall()


class ThreadedConnectionPool(TCP):
    def __init__(self, minconn, maxconn, database='postgres', user='postgres', host='localhost', *args, **kwargs):
        super().__init__(minconn, maxconn, database=database, user=user, host=host, *args, **kwargs)
    
    @contextmanager
    def getconn(self):
        try:
            conn = super().getconn()
            yield conn
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            self.putconn(conn)  

    @contextmanager
    def cursor(self):
        with self.getconn() as conn:
            try:
                cur = conn.cursor()
                yield cur 
            except:
                raise
            finally:
                cur.close()

    def execute(self, query, vars=None):
        with self.cursor() as cur:
            cur.execute(query, vars)

    def fetchall(self, query, vars=None):
        with self.cursor() as cur:
            cur.execute(query, vars)
            return cur.fetchall()
