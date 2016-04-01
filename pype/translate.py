from pype.ast import *
from pype.symtab import *
from pype.lib_import import LibraryImporter
from pype.fgir import FGNodeType, FGNode, Flowgraph, FGIR
from pype.error import *


class SymbolTableVisitor(ASTVisitor):

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


class LoweringVisitor(ASTModVisitor):
    'Produces FGIR from an AST.'

    def __init__(self, symtab):
        self.symtab = symtab
        self.ir = FGIR()
        self.current_component = None

    def visit(self, astnode):
        if isinstance(astnode, ASTComponent):
            name = astnode.name.name
            self.ir[name] = Flowgraph(name=name)
            self.current_component = name
        return astnode

    def post_visit(self, node, visit_value, child_values):

        if isinstance(node, ASTProgram):
            return self.ir

        elif isinstance(node, ASTInputExpr):
            fg = self.ir[self.current_component]
            for child_v in child_values:
                varname = child_v.name
                var_nodeid = fg.get_var(varname)
                if var_nodeid is None:  # No use yet, declare it.
                    var_nodeid = fg.new_node(FGNodeType.input).nodeid
                else:  # use before declaration
                    fg.nodes[var_nodeid].type = FGNodeType.input
                fg.set_var(varname, var_nodeid)
                fg.add_input(var_nodeid)
            return None

        elif isinstance(node, ASTOutputExpr):
            fg = self.ir[self.current_component]
            for child_v in child_values:
                n = fg.new_node(FGNodeType.output)
                varname = child_v.name
                var_nodeid = fg.get_var(varname)
                if var_nodeid is None:  # Use before declaration
                    # The "unknown" type will be replaced later
                    var_nodeid = fg.new_node(FGNodeType.unknown).nodeid
                    fg.set_var(varname, var_nodeid)
                # Already declared in an assignment or input expression
                n.inputs.append(var_nodeid)
                fg.add_output(n.nodeid)
            return None

        elif isinstance(node, ASTAssignmentExpr):
            fg = self.ir[self.current_component]
            # If a variable use precedes its declaration,
            # a stub will be in this table
            stub_nodeid = fg.get_var(node.binding.name)
            if stub_nodeid is not None:  # Modify the existing stub
                n = fg.nodes[stub_nodeid]
                n.type = FGNodeType.assignment
            else:  # Create a new node
                n = fg.new_node(FGNodeType.assignment)
            child_v = child_values[1]
            if isinstance(child_v, FGNode):  # subexpressions or literals
                n.inputs.append(child_v.nodeid)
            elif isinstance(child_v, ASTID):  # variable lookup
                varname = child_v.name
                var_nodeid = fg.get_var(varname)
                if var_nodeid is None:  # Use before declaration
                    # The "unknown" type will be replaced later
                    var_nodeid = fg.new_node(FGNodeType.unknown).nodeid
                    fg.set_var(varname, var_nodeid)
                # Already declared in an assignment or input expression
                n.inputs.append(var_nodeid)
            fg.set_var(node.binding.name, n.nodeid)
            return None

        elif isinstance(node, ASTEvalExpr):
            fg = self.ir[self.current_component]
            op = self.symtab.lookupsym(node.op.name,
                                       scope=self.current_component)
            if op is None:
                raise PypeSyntaxError('Undefined operator: '+str(node.op.name))
            if op.type == SymbolType.component:
                n = fg.new_node(FGNodeType.component, ref=op.name)
            elif op.type == SymbolType.libraryfunction:
                n = fg.new_node(FGNodeType.libraryfunction, ref=op.ref)
            elif op.type == SymbolType.librarymethod:
                n = fg.new_node(FGNodeType.librarymethod, ref=op.ref)
            else:
                raise PypeSyntaxError('Invalid operator of type "' +
                                      str(SymbolType)+'" in expression: ' +
                                      str(node.op.name))

            n.inputs = []
            for child_v in child_values[1:]:
                if isinstance(child_v, FGNode):  # subexpressions or literals
                    n.inputs.append(child_v.nodeid)
                elif isinstance(child_v, ASTID):  # variable lookup
                    varname = child_v.name
                    var_nodeid = fg.get_var(varname)
                    if var_nodeid is None:  # Use before declaration
                        # The "unknown" type will be replaced later
                        var_nodeid = fg.new_node(FGNodeType.unknown).nodeid
                        fg.set_var(varname, var_nodeid)
                    # Already declared in an assignment or input expression
                    n.inputs.append(var_nodeid)
            return n

        elif isinstance(node, ASTLiteral):
            fg = self.ir[self.current_component]
            n = fg.new_node(FGNodeType.literal, ref=node.value)
            return n

        else:
            return visit_value
