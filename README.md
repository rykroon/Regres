Regres is a Python ORM

### What makes it different?
- Lightweight
- Influenced by Django and SQLAlchemy
- Has Redis Built-in 
- SQL Friendly Attitude


### Lightweight

### Django + SQLAlchemy
- There are Django like methods such as save() and delete() 
- There are SQLAlchemy like methods such as get(), filter(), filter_by()

### Redis
- There is a RedisModel class for models that are not persisted to a database table
- There is a HybridModel class for Models that are stored in a database table, but are also cached in Redis.
### SQL Friendly
- Most ORMs are designed so that the programmer never has to write a single line of SQL because every single SQL feature is built into the ORM. 
- Because this ORM was designed to be lightweight and only have the most neccesary features for a CRUD API, all other functionality will require you to write your own queries.
- This ORM does not have functionality for GROUP BY and HAVING clauses. 
- There are no aggregate functions.
- There is no bulk create, update, or delete functions
- There is also no means of creating database tables or migrations. 
- This ORM loads it's models from already existing databases.
