from .ast import *
from .symtab import *
from .lib_import import LibraryImporter

class SymbolTableVisitor(ASTVisitor):
  def __init__(self):
    self.symbol_table = SymbolTable()

  def return_value(self):
    return self.symbol_table

  def visit(self, node):
    if isinstance(node, ASTImport):
      # Import statements make library functions available to PyPE
      imp = LibraryImporter(node.module)
      imp.add_symbols(self.symbol_table)

    elif isinstance(node, ASTProgram): 
      # Traverse the rest of the nodes in the tree
      for child in node.children:
        self.visit(child)

    # Add symbols for the following types of names: Inputs, Assignments, Components
    
    # Inputs: anything in an input expression
    # the SymbolType should be input, and the ref can be None
    # the scope should be the enclosing component
    elif isinstance(node, ASTInputExpr):
      for child in node.children:
        sym = Symbold(child.name, SympolType.input, None)
        scope = node.parent.name
        self.symbol_table.addsym(sym, scope=scope)
    
    # Assigned names: the bound name in an assignment expression
    # the SymbolType should be var, and the ref can be None
    # the scope should be the enclosing component
    elif isinstance(node, ASTAssignmentExpr):
      sym = Symbol(node.binding.name, SymbolType.var, None)
      scope = node.binding.name
      self.symbol_table.addsym(sym, scope=scope)
    
    # Components: the name of each component
    # the SymbolType should be component, and the ref can be None
    # the scope sould be *global*
    elif isinstance(node, ASTComponent):  
      sym = Symbol(node.name, SymbolType.component, None)
      self.symbol_table.addsym(sym)

      # traverse the rest of the nodes in the tree
      for child in node.children:
        self.visit(child)

    # Note, you'll need to track scopes again for some of these.
    # You may need to add class state to handle this.