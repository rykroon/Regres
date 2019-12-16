class Asterisk:
    def __str__(self):
        return '*'


class WhereExpression:
    def __init__(self, left, op, right):
        self.left = left 
        self.op = op 
        self.right = right

    def __str__(self):
        return "{} {} {}".format(left, op, right)


class OrderByExpression:
    def __init__(self, field, ascending=True):
        self.field = field 
        self.ascending = ascending

    def __str__(self):
        if not self.ascending:
            return '"{}" DESC'.format(self.field)

        return '"{}"'.format(self.field)