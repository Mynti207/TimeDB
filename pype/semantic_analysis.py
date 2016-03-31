from pype.ast import *
from pype.error import *

class PrettyPrint(ASTVisitor):

    def __init__(self):
        pass

    def visit(self, astnode):
        if astnode.children is not None:
            for child in astnode.children:
                self.visit(child)


class CheckSingleAssignment(ASTVisitor):

    def __init__(self):
        self.local_vars = []
        self.global_vars = []

    def visit(self, astnode):

        if isinstance(astnode, ASTProgram):
            if astnode.children is not None:
                for child in astnode.children:
                    self.visit(child)

        if isinstance(astnode, ASTComponent):

            # reset local assignments
            self.local_vars = []

            # add to global variables
            self.global_vars.append(astnode.name)

            if astnode.children is not None:
                for child in astnode.children:
                    if isinstance(child, ASTAssignmentExpr):
                        if child.binding in self.local_vars:
                            raise Exception(
                                'Double Assignement: {}'.format(child.binding))
                        else:
                            self.local_vars.append(child.binding)


class CheckSingleIOExpression(ASTVisitor):

    def __init__(self):
        self.component = None
        self.component_has_input = False
        self.component_has_output = False

    def visit(self, node):
        if isinstance(node, ASTComponent):
            self.component = node.name.name
            self.component_has_input = False
            self.component_has_output = False
        elif isinstance(node, ASTInputExpr):
            if self.component_has_input:
                raise PypeSyntaxError('Component ' + str(self.component) +
                                      ' has multiple input expressions')
            self.component_has_input = True
        elif isinstance(node, ASTOutputExpr):
            if self.component_has_output:
                raise PypeSyntaxError('Component ' + str(self.component) +
                                      ' has multiple output expressions')
            self.component_has_output = True


class CheckUndefinedVariables(ASTVisitor):

    def __init__(self, symtab):
        self.symtab = symtab
        self.scope = None

    def visit(self, node):
        if isinstance(node, ASTComponent):
            self.scope = node.name.name
        elif isinstance(node, ASTID):
            if self.symtab.lookupsym(node.name, scope=self.scope) is None:
                print (node.name)
                raise PypeSyntaxError('Undefined variable: ' + str(node.name))
