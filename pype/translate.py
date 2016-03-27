from pype.ast import *
from pype.symtab import *
from pype.lib_import import LibraryImporter


class SymbolTableVisitor(ASTVisitor):
    '''
    Visitor subclass to walk through a SymbolTable
    '''

    def __init__(self):
        self.symbol_table = SymbolTable()

    def return_value(self):
        return self.symbol_table

    def visit(self, node):

        # import statements make library functions available to PYPE
        if isinstance(node, ASTImport):
            imp = LibraryImporter(node.module)
            imp.add_symbols(self.symbol_table)

        # traverse the rest of the nodes in the tree
        elif isinstance(node, ASTProgram):
            if node.children is not None:
                for child in node.children:
                    self.visit(child)

        # add symbols for inputs, i.e. anything in an expression_list
        # SymbolType: inputs
        # ref: None
        # scope: enclosing component
        elif isinstance(node, ASTInputExpr):
            if node.children is not None:
                for child in node.children:
                    sym = Symbol(child.name, SymbolType.input, None)
                    scope = node.parent.name.name
                    self.symbol_table.addsym(sym, scope=scope)

        # add symbols for assigned names, i.e. the bound name in an
        # assignment expression
        # SymbolType: var
        # ref: None
        # scope: enclosing component
        elif isinstance(node, ASTAssignmentExpr):
            sym = Symbol(node.binding.name, SymbolType.var, None)
            scope = node.parent.name.name
            self.symbol_table.addsym(sym, scope=scope)

        # add symbols for components, i.e. the name of each components
        # SymbolType: components
        # ref: None
        # scope: global
        elif isinstance(node, ASTComponent):
            sym = Symbol(node.name.name, SymbolType.component, None)
            self.symbol_table.addsym(sym)

            # traverse the rest of the nodes in the tree
            if node.children is not None:
                for child in node.children:
                    self.visit(child)
