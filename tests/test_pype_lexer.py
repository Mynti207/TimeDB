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
