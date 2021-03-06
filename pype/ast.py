class ASTVisitor():
    '''
    Wrapper class to walk througha tree and visit each Node.
    '''

    def visit(self, astnode):
        'A read-only function which looks at a single AST node.'
        pass

    def return_value(self):
        return None


class ASTModVisitor(ASTVisitor):
    '''
    A visitor class that can also construct a new, modified AST.

    Two methods are offered: the normal visit() method, which focuses on
    analyzing and/or modifying a single node; and the post_visit() method,
    which allows you to modify the child list of a node.
    The default implementation does nothing; it simply builds up itself,
    unmodified.'''

    def visit(self, astnode):
        # note: this overrides the super's implementation, because we need a
        # non-None return value.
        return astnode

    def post_visit(self, visit_value, child_values):
        '''
        A function which constructs a return value out of its children.

        This can be used to modify an AST by returning a different or modified
        ASTNode than the original. The top-level return value will then be the
        new AST.
        '''
        return visit_value


class ASTNode(object):
    '''
    Node of the AST tree.
    '''

    def __init__(self):
        self.parent = None
        self._children = []

    @property
    def children(self):
        return self._children

    @children.setter
    def children(self, children):
        self._children = children
        if children is not None:
            for child in children:
                child.parent = self

    def pprint(self, indent=''):
        '''
        Recursively prints a formatted string representation of the AST.
        Format of the print:
        parent
            child1
                child11 child12
            child2
                child21 child22
        '''
        print(indent + str(self))
        if self.children is not None:
            for child in self.children:
                child.pprint(indent=indent+'\t')

    def walk(self, visitor):
        '''
        Traverses an AST, calling node.visit(visitor) on every node.

        This is a depth-first, pre-order traversal. Parents will be visited
        before any children, children will be visited in order, and (by
        extension) a node's children will all be visited before its siblings.
        The visitor may modify attributes, but may not add or delete nodes.
        '''

        # visit self first
        visitor.visit(self)

        # then visit all children (if any)
        if self.children is not None:
            for child in self.children:
                child.walk(visitor)

        return visitor.return_value()

    def mod_walk(self, mod_visitor):
        '''
        Traverses an AST, building up a return value from visitor methods.

        Similar to walk(), but constructs a return value from the result of
        postvisit() calls. This can be used to modify an AST by building up the
        desired new AST with return values.
        '''

        selfval = mod_visitor.visit(self)
        child_values = [child.mod_walk(mod_visitor) for child in self.children]
        retval = mod_visitor.post_visit(self, selfval, child_values)
        return retval


class ASTProgram(ASTNode):
    '''
    Node storing a programm in the AST.
    '''

    def __init__(self, statements):
        super().__init__()
        self.children = statements


class ASTImport(ASTNode):
    '''
    Node storing an import statement in the AST.
    '''

    def __init__(self, module):
        super().__init__()
        self.module = module


class ASTComponent(ASTNode):
    '''
    Node storing a component in the AST, ie a group of instructions between
    brackets.
    '''

    def __init__(self, children):
        super().__init__()
        self.children = children

    @property
    def name(self):
        return self.children[0]

    @property
    def expressions(self):
        return self.children[1:]


class ASTInputExpr(ASTNode):
    '''
    Node storing an input expression in the AST.
    '''

    def __init__(self, children=None):
        super().__init__()
        self.children = children


class ASTOutputExpr(ASTNode):
    '''
    Node storing an output expression in the AST.
    '''

    def __init__(self, children=None):
        super().__init__()
        self.children = children


class ASTAssignmentExpr(ASTNode):
    '''
    Node storing an assignement expression in the AST.
    '''

    def __init__(self, children):
        super().__init__()
        self.children = children

    @property
    def binding(self):
        return self.children[0]

    @property
    def value(self):
        return self.children[1]


class ASTEvalExpr(ASTNode):
    '''
    Node storing an evaluation expression in the AST.
    '''

    def __init__(self, children):
        super().__init__()
        self.children = children

    @property
    def op(self):
        return self.children[0]

    @property
    def args(self):
        return self.children[1:]


class ASTID(ASTNode):
    '''
    Node storing an ID in the AST.
    '''

    def __init__(self, name, typedecl=None):
        super().__init__()
        self.name = name
        self.type = typedecl


class ASTLiteral(ASTNode):
    '''
    Node storing a Literal variable in the AST.
    '''

    def __init__(self, value):
        super().__init__()
        self.value = value
        self.type = 'Scalar'
