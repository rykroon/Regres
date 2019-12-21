from .expressions import Assignment, Condition, Expression, OutputExpression, Value

class Column:
    """
        SQL Column
    """
    def __init__(self, name, table):
        self.name = name 
        self.table = table

    def __eq__(self, value):
        return Condition(self, '=', Value(value)) 

    def __ge__(self, value):
        return Condition(self, '>=', Value(value)) 

    def __getitem__(self, item):
        """
            @param item: the name of an sql operator
            @return: the method associated with that operator
        """

        item = item.lower()

        return {
            'as':   self.as_,
            'asc':  self.asc,
            'desc': self.desc,
            'eq':   self.__eq__,
            'ge':   self.__ge__,
            'gt':   self.__gt__,
            'le':   self.__le__,
            'lt':   self.__lt__,
            'ne':   self.__ne__,
            'in':   self.in_,
            'like': self.like
        }[item]

    def __gt__(self, value):
        return Condition(self, '>', Value(value)) 

    def __le__(self, value):
        return Condition(self, '<=', Value(value)) 

    def __lt__(self, value):
        return Condition(self, '<', Value(value)) 

    def __ne__(self, value):
        return Condition(self, '!=', Value(value)) 

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def __str__(self):
        return '"{}"."{}"'.format(self.table.name, self.name)

    def as_(self, output_name=None):
        return OutputExpression(self, output_name)

    def asc(self):
        return Expression(self, 'ASC')

    def assign(self, value):
        return Assignment(self, Value(value))

    def desc(self):
        return Expression(self, 'DESC')

    def in_(self, value):
        return Condition(self, 'IN', Value(value))

    def like(self, value):
        return Condition(self, 'LIKE', Value(value))

