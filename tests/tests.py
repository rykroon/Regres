"""
    Tests to write

    get, create, update, delete for RedisModel, Model, and HybridModel

    creating a model for a table that does not exist


"""

import redis 
from psycopg2 import connect

from regres import *

pool = SimpleConnectionPool(2,3)
redis_conn = redis.Redis()

class User(Model):
    table = Table('users', pool)

class Session(RedisModel):
    conn = redis_conn
    expire = 300

def create_user():
    user = User(name='Ryan')
    users.save()


if __name__ == '__main__':
    r = redis.Redis()
    conn = connect(dbname='postgres', user='postgres', host='localhost')