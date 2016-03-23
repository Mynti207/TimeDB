from .ast import *

class PrettyPrint(ASTVisitor):
  def __init__(self):
    pass
  def visit(self, node):
    print(node.__class__.__name__)
    for child in node.children:
      self.visit(child)

class CheckSingleAssignment(ASTVisitor):
  def __init__(self):
    self.local_vars = []
    self.global_vars = []
  
  def visit(self, node):
    if isinstance(node, ASTProgram):
      for child in node.child:
        self.visit(child)

    if isinstance(node, ASTComponent):
      self.local_vars = [] # reset local assignments
      self.global_vars.append(node.name) # add to global variables

      for child in node.children:
        if isinstance(child, ASTAssignmentExpr):
          if child.binding in self.local_vars:
            raise Exception('Double Assignement: {}'.format(child.binding))
          else:
            self.local_vars.append(child.binding) 