

class Expression:
    def __init__(self, query, *args):
        self.query = query
        self.vars = list(args)

    def __add__(self, other):
        query = "{} {}".format(self.query, other.query)
        vars = self.vars + other.vars 
        return Expression(query, vars)

    def __radd__(self, other):
        query = "{} {}".format(other.query, self.query)
        vars = other.vars + self.vars 
        return Expression(query, vars)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.query)

    def __str__(self):
        return self.query 


class Condition(Expression):

    def __and__(self, other):
        query = "{} AND {}".format(self.query, other.query)
        vars = self.vars + other.vars
        return Condition(query, vars)

    def __invert__(self):
        query = "NOT {}".format(self.query)
        return Condition(query, self.vars)

    def __or__(self, other):
        query = "{} OR {}".format(self.query, other.query)
        vars = self.vars + other.vars
        return Condition(query, vars)

