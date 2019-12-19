from expressions import Assignment, Condition, Expression

class Column:
    """
        SQL Column
    """
    def __init__(self, name, table):
        self.name = name 
        self.table = table

    def __eq__(self, value):
        return Condition(self, '=', value) 

    def __ge__(self, value):
        return Condition(self, '>=', value) 

    def __getitem__(self, item):
        if type(item) == slice:
            raise TypeError("unhashable type: 'slice'")

        if item in ('eq','ge','gt','le','lt','ne'):
            item = "__{}__".format(item)

        elif item == 'in':
            item = 'in_'

        return getattr(self, item)

    def __gt__(self, value):
        return Condition(self, '>', value) 

    def __le__(self, value):
        return Condition(self, '<=', value) 

    def __lt__(self, value):
        return Condition(self, '<', value) 

    def __ne__(self, value):
        return Condition(self, '!=', value) 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def __str__(self):
        return '"{}"."{}"'.format(self.table.name, self.name)

    def asc(self):
        return Expression(self, 'ASC')

    def assign(self, value):
        return Assignment(self, value)

    def desc(self):
        return Expression(self, 'DESC')

    def in_(self, value):
        return Condition(self, 'IN', value)

    def like(self, value):
        return Condition(self, 'LIKE', value)