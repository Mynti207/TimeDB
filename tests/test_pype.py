import pytest
from pype import *

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
    Pipeline(source='tests/samples/example2.ppl')

    # strings
    Pipeline(source='tests/samples/example3.ppl')

    # arithmetic operations
    Pipeline(source='tests/samples/example4.ppl')

    # two (more complicated) functions
    Pipeline(source='tests/samples/six.ppl')
