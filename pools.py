from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool as SCP
from psycopg2.pool import ThreadedConnectionPool as TCP


class SimpleConnectionPool(SCP):

    def __init__(self, minconn, maxconn, database='postgres', user='postgres', host='localhost', *args, **kwargs):
        super().__init__(minconn, maxconn, database=database, user=user, host=host, *args, **kwargs)
    
    @contextmanager
    def getconn(self):
        conn = super().getconn()
        try:
            yield conn
        except:
            conn.rollback()
        finally:
            conn.commit()
            self.putconn(conn)  


class ThreadedConnectionPool(TCP):
    def __init__(self, minconn, maxconn, database='postgres', user='postgres', host='localhost', *args, **kwargs):
        super().__init__(minconn, maxconn, database=database, user=user, host=host, *args, **kwargs)
    
    @contextmanager
    def getconn(self):
        conn = super().getconn()
        try:
            yield conn
        except:
            conn.rollback()
        finally:
            conn.commit()
            self.putconn(conn)  
