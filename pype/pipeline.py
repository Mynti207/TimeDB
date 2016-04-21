from .lexer import lexer
from .parser import parser
from .ast import *
from .semantic_analysis import (CheckSingleAssignment, CheckSingleIOExpression,
                                CheckUndefinedVariables)
from .translate import SymbolTableVisitor, LoweringVisitor
from .optimize import *
from .pcode import PCodeGenerator


class Pipeline(object):
    '''
    Main PYPE driver. Carries out the following steps:
        - Opens and load a PPL (pipeline) file
        - Carries out lexing, parsing and AST construction of tokens contained
        in PPL file
        - Creates symbol table for relevant scope(s)
        - Runs code optimization
        - Generates pcode
    '''

    def __init__(self, source):
        self.pcodes = {}
        with open(source) as f:
            self.compile(f)

    def compile(self, file):

        input = file.read()

        # lexing, parsing, AST construction
        ast = parser.parse(input, lexer=lexer)

        # semantic analysis
        ast.walk(CheckSingleAssignment())
        ast.walk(CheckSingleIOExpression())
        syms = ast.walk(SymbolTableVisitor())
        ast.walk(CheckUndefinedVariables(syms))

        # translation
        ir = ast.mod_walk(LoweringVisitor(syms))

        # optimization
        ir.flowgraph_pass(AssignmentEllision())
        ir.flowgraph_pass(DeadCodeElimination())
        ir.topological_flowgraph_pass(InlineComponents())

        # pcode generation
        pcodegen = PCodeGenerator()
        ir.flowgraph_pass(pcodegen)
        self.pcodes = pcodegen.pcodes

    def __getitem__(self, component_name):
        return self.pcodes[component_name]
