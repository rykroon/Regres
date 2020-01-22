from psycopg2.extensions import AsIs, adapt, register_adapter


class Expression:
    def __init__(self, expr, *args):
        self.expr = expr
        self.args = args

    def __add__(self, other):
        expr = "{}, {}".format(self, other)
        args = self.args + other.args
        return Expression(expr, *args)

    def __radd__(self, other):
        expr = "{}, {}".format(other, self)
        args = other.args + self.args
        return Expression(expr, *args)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self)

    def __str__(self):
        return self.expr


class Condition(Expression):
    def __and__(self, other):
        expr = "{} AND {}".format(self, other)
        args = self.args + other.args
        return Condition(expr, *args)

    def __invert__(self):
        expr = "NOT {}".format(self)
        return Condition(expr, *self.args)

    def __or__(self, other):
        expr = "{} OR {}".format(self, other)
        args = self.args + other.args
        return Condition(expr, *args)


def adapt_expression(expr):
    args = tuple([adapt(arg).getquoted().decode() for arg in expr.args])
    return AsIs(str(expr) % args)
    

register_adapter(Expression, adapt_expression)