from .ast_copy import *


class PrettyPrint(ASTVisitor):

    def __init__(self):
        pass

    def visit(self, astnode):
        print(astnode.__class__.__name__)
        for child in astnode.children:
            self.visit(child)


class CheckSingleAssignment(ASTVisitor):

    def __init__(self):
        self.local_vars = []
        self.global_vars = []

    def visit(self, astnode):
        if isinstance(astnode, ASTProgram):
            for child in astnode.children:
                self.visit(child)

        elif isinstance(astnode, ASTComponent):
            self.local_vars = []  # reset local assignments
            self.global_vars.append(astnode.name)  # add to global variables

            for child in astnode.children:
                if isinstance(child, ASTAssignmentExpr):
                    if child.binding in self.local_vars:
                        raise Exception(
                            'Double Assignement: {}'.format(child.binding))
                    else:
                        self.local_vars.append(child.binding)
