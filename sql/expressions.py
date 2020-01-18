

class Expression:
    def __init__(self, expr, *args):
        self.expr = expr
        self.args = args

    def __add__(self, other):
        expr = "{}, {}".format(self.expr, other.expr)
        args = self.args + other.args
        return Expression(expr, *args)

    def __radd__(self, other):
        expr = "{}, {}".format(other.expr, self.expr)
        args = other.args + self.args
        return Expression(expr, *args)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.expr)

    def __str__(self):
        return self.expr 


class Condition(Expression):

    def __and__(self, other):
        expr = "{} AND {}".format(self.expr, other.expr)
        args = self.args + other.args
        return Condition(expr, *args)

    def __invert__(self):
        expr = "NOT {}".format(self.expr)
        return Condition(expr, *self.args)

    def __or__(self, other):
        expr = "{} OR {}".format(self.expr, other.expr)
        args = self.args + other.args
        return Condition(expr, *args)