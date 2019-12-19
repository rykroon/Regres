
# logical operators not, and, or
# comparison operators <, <=, ==, !=, >, >=
# assignment operator =

class Comparable:
    def __eq__(self, val):
        return "{} = {}".format(str(self), str(val))

    def __ge__(self, val):
        return "{} >= {}".format(str(self), str(val))

    def __gt__(self, val):
        return "{} > {}".format(str(self), str(val))

    def __le__(self, val):
        return "{} <= {}".format(str(self), str(val))

    def __lt__(self, val):
        return "{} < {}".format(str(self), str(val))
         
    def __ne__(self, val):
        return "{} != {}".format(str(self), str(val))

    def __in(self, val):
        return "{} IN {}".format(str(self, str(val)))

    def __like(self, val):
        return "{} LIKE {}".format(str(self), str(val))


class Logicable:
    def __and__(self, val):
        return "{} AND {}".format(str(self), str(val))

    def __neg__(self):
        return "NOT {}".format(str(self))

    def __or__(self, val):
        return "{} OR {}".format(str(self), str(val))


class Assignable:
    def __assign(self, val):
        return "{} = {}".format(str(self), str(val))