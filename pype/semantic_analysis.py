from pype.ast import *


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
