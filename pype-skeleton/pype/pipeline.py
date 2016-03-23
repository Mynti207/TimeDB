from .lexer import lexer
from .parser import parser
from .ast_copy import *
from .semantic_analysis import CheckSingleAssignment
from .translate import SymbolTableVisitor

class Pipeline(object):

    def __init__(self, source):
        with open(source) as f:
            self.compile(f)

    def compile(self, file):
        input = file.read()
        # Lexing, parsing, AST construction
        ast = parser.parse(input, lexer=lexer)
        print_tree = ast.pprint()
        # Semantic analysis
        ast.walk( CheckSingleAssignment() )
        # Translation
        syms = ast.walk( SymbolTableVisitor() )
        print(syms.pprint())
        return syms