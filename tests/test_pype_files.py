import pytest
from pype import *
from timeseries import *

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


def test_files():

    # time series examples
    Pipeline(source='tests/samples_pype/example0.ppl')
    Pipeline(source='tests/samples_pype/example1.ppl')

    # syntax error
    with pytest.raises(PypeSyntaxError):
        Pipeline(source='tests/samples_pype/syntaxerror1.ppl')

    with pytest.raises(PypeSyntaxError):
        Pipeline(source='tests/samples_pype/syntaxerror2.ppl')

    # strings
    Pipeline(source='tests/samples_pype/example2.ppl')

    # two (more complicated) functions
    Pipeline(source='tests/samples_pype/six.ppl')
