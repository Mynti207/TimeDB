from pype.lexer import lexer
from pype.parser import parser
from pype.ast import *
from pype.semantic_analysis import CheckSingleAssignment
from pype.translate import SymbolTableVisitor


class Pipeline(object):

    def __init__(self, source):
        with open(source) as f:
            self.compile(f)

    def compile(self, file):

        input = file.read()

        # Lexing, parsing, AST construction
        ast = parser.parse(input, lexer=lexer)
        ast.pprint()

        # Semantic analysis
        ast.walk(CheckSingleAssignment())
        # Translation
        syms = ast.walk(SymbolTableVisitor())
        print(syms.pprint())
