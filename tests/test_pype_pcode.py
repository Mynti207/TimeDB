import pytest
from pype import *
from timeseries import *

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


def test_pcode():

    standardize_code = """
    (import timeseries)
    { standardize
      (input (TimeSeries t))
      (:= mu (mean t))
      (:= sig (std t))
      (:= new_t (/ (- t mu) sig))
      (output new_t)
    }
    """

    # generate time series data for testing
    times = [t for t in range(100)]
    values = [t - 50 for t in range(100)]
    ts = TimeSeries(times, values)

    ast = pype.parser.parse(standardize_code, lexer=pype.lexer)
    ast.walk(pype.CheckSingleAssignment())
    ast.walk(pype.CheckSingleIOExpression())
    syms = ast.walk(pype.SymbolTableVisitor())
    ast.walk(pype.CheckUndefinedVariables(syms))

    ir = ast.mod_walk(pype.LoweringVisitor(syms))
    ir.flowgraph_pass(pype.AssignmentEllision())
    ir.flowgraph_pass(pype.DeadCodeElimination())
    ir.topological_flowgraph_pass(pype.InlineComponents())

    pcodegen = pype.PCodeGenerator()
    ir.flowgraph_pass(pcodegen)
    pcodes = pcodegen.pcodes
    result = pcodes['standardize'].run(ts)

    print("Output should be 0, 1")
    print("Output:", result.mean(), result.std())
    assert(round(result.mean(), 10) == 0)
    assert(round(result.std(), 10) == 1)
