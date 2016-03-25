from pype.lexer import lexer
from pype.parser import parser
from pype.ast import *
from pype.semantic_analysis import CheckSingleAssignment
from pype.translate import SymbolTableVisitor


class Pipeline(object):
    '''
    Main PYPE driver. Carries out the following steps:
        - Opens and load a PPL (pipeline) file
        - Carries out lexing, parsing and AST construction of tokens contained
        in PPL file
        - Creates symbol table for relevant scope(s)
    '''

    def __init__(self, source):
        with open(source) as f:
            self.compile(f)

    def compile(self, file):

        input = file.read()

        # lexing, parsing, AST construction
        ast = parser.parse(input, lexer=lexer)
        ast.pprint()

        # semantic analysis
        ast.walk(CheckSingleAssignment())
        syms = ast.walk(SymbolTableVisitor())
        syms.pprint()
