import pytest
from pype import *
from timeseries import *

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


def test_lexer():

    # sample data
    data = '''
    3 + 4 * 10
      + -20 *2
    '''

    # pass data to lexer and tokenize
    lexer.input(data)
    for tok in lexer:
        assert isinstance(tok, ply.lex.LexToken)

    # sample data
    data = '''
    # sample comment
    x := 3 + 42 * (s - t)
    '''

    # pass data to lexer and tokenize
    lexer.input(data)
    for tok in lexer:
        assert isinstance(tok, ply.lex.LexToken)

    # sample data
    data = '''
    # sample comment
    sample_string = "bla"
    '''

    # pass data to lexer and tokenize
    lexer.input(data)
    for tok in lexer:
        assert isinstance(tok, ply.lex.LexToken)


def test_files():

    # time series examples
    Pipeline(source='tests/samples/example0.ppl')
    Pipeline(source='tests/samples/example1.ppl')

    # syntax error
    with pytest.raises(PypeSyntaxError):
        Pipeline(source='tests/samples/syntaxerror1.ppl')

    with pytest.raises(PypeSyntaxError):
        Pipeline(source='tests/samples/syntaxerror2.ppl')

    # strings
    Pipeline(source='tests/samples/example2.ppl')

    # two (more complicated) functions
    Pipeline(source='tests/samples/six.ppl')

def test_final():

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
