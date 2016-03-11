# from .error import *


class ASTVisitor():

    def visit(self, astnode):
        'A read-only function which looks at a single AST node.'
        pass


class ASTNode(object):

    def __init__(self):
        self.parent = None
        self._children = []

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, children):
        self._children = children
        for child in children:
            child.parent = self

    def pprint(self, indent=''):
        '''Recursively prints a formatted string representation of the AST.
        Format of the print:
        parent
            child1
                child11 child12
            child2
                child21 child22
        '''
        # TODO
        line = indent + self.parent + '\n'
        for child in self.children:
            line += pprint(child, indent=indent+'\t')
        return ret

    def walk(self, visitor):
        '''Traverses an AST, calling visitor.visit() on every node.

        This is a depth-first, pre-order traversal. Parents will be visited before
        any children, children will be visited in order, and (by extension) a node's
        children will all be visited before its siblings.
        The visitor may modify attributes, but may not add or delete nodes.'''
        # TODO
        # Stop criterion
        if visitor is None:
            return
        # Visiting parent
        visitor.visit()
        # Walking from children in order
        for child in visitor.children:
            self.walk(child)


class ASTProgram(ASTNode):

    def __init__(self, statements):
        super().__init__()
        self.children = statements


class ASTImport(ASTNode):  # TODO

    def __init__(self, module):
        super().__init__()
        self.module = module


class ASTComponent(ASTNode):  # TODO

    def __init__(self, children):
        super().__init__()
        self.children = children

    @property
    def name(self):  # TODO return an element of self.children
        return self.children[0]

    @property
    def expressions(self):  # TODO return one or more children
        return self.children[1]


class ASTInputExpr(ASTNode):  # TODO

    def __init__(self, children=None):
        super().__init__()
        self.children = children


class ASTOutputExpr(ASTNode):  # TODO

    def __init__(self, children=None):
        super().__init__()
        self.children = children


class ASTAssignmentExpr(ASTNode):  # TODO

    def __init__(self, children):
        super().__init__()
        self.children = children

    @property
    def binding(self):  # TODO
        return self.children[0]

    @property
    def value(self):  # TODO
        return self.children[1]


class ASTEvalExpr(ASTNode):  # TODO

    def __init__(self, children):
        super().__init__()
        self.children = children

    @property
    def op(self):  # TODO
        return self.children[0]

    @property
    def args(self):  # TODO
        return self.children[1:]

# These are already complete.


class ASTID(ASTNode):

    def __init__(self, name, typedecl=None):
        super().__init__()
        self.name = name
        self.type = typedecl


class ASTLiteral(ASTNode):

    def __init__(self, value):
        super().__init__()
        self.value = value
        self.type = 'Scalar'
