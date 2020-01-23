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


class Pet(RedisModel):
    conn = redis_conn
    expire = 300


if __name__ == '__main__':
    r = redis.Redis()
    conn = connect(dbname='postgres', user='postgres', host='localhost')
    cur = conn.cursor()

    cur.execute('DELETE FROM users')
    r.flushdb()

    #test User Model

    user = User(name='Ryan', age=27)
    result = user.save()
    assert result == True

    cur.execute("SELECT * FROM users WHERE name='Ryan'")
    rows = cur.fetchall()
    assert len(rows) == 1

    user.name = 'Kroon'
    user.save()

    cur.execute("SELECT * FROM users WHERE name='Kroon'")
    rows = cur.fetchall()
    assert len(rows) == 1

    id = user.pk
    del user 

    user = User.get(id)
    assert user.name == 'Kroon'

    user.delete()

    cur.execute("SELECT * FROM users WHERE name='Kroon'")
    rows = cur.fetchall()
    assert len(rows) == 0

    # Test Pet Redis Model

    pet = Pet(name='Leo', animal='Dog')
    pet.save()

    assert len(r.keys()) == 1

    id = pet.id
    del pet 

    pet = Pet.get(id)
    assert pet.name == 'Leo'

    pet.delete()
    assert len(r.keys()) == 0



