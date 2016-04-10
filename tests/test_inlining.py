import pytest
from pype import *

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


def test_inling():

    # Defining input with two uses of the same component
    program_input ='''
    (import timeseries)
{ mul (input x y) (:= z (* x y)) (output z) }
{ dist (input a b) (:= c (mul (mul a b) (mul b a))) (output c) }
    '''

    # lexing, parsing, AST construction
    ast = parser.parse(program_input, lexer=lexer)
    # ast.pprint()

    # semantic analysis
    ast.walk(CheckSingleAssignment())
    ast.walk(CheckSingleIOExpression())
    syms = ast.walk(SymbolTableVisitor())
    syms.pprint()
    ast.walk(CheckUndefinedVariables(syms))

    # translation
    ir = ast.mod_walk(LoweringVisitor(syms))

    # optimization
    ir.flowgraph_pass(AssignmentEllision())
    ir.flowgraph_pass(DeadCodeElimination())
    ir.topological_flowgraph_pass(InlineComponents())

    # Check there is no component nodes in the graph
    for name, g in ir.graphs.items():
        for tnode in g.nodes.values():
            assert(tnode.type != FGNodeType.component)

    # Check inputs are correct (right names)
    # NB: need to think a way to check for outputs also
    inputs = {'mul': ['x', 'y'], 'dist': ['a', 'b']}
    outputs = {'mul': ['z'], 'dist': ['c']}
    for name, g in ir.graphs.items():
        for kin in inputs[name]:
            print(kin)
            print(g.nodes[g.variables[kin]].type)
            assert(g.nodes[g.variables[kin]].type == FGNodeType.input)