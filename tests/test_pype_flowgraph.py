import pytest
from pype import *

__author__ = "Mynti207"
__copyright__ = "Mynti207"
__license__ = "mit"


def test_flowgraph():

    # create flowgraph and nodes
    FG = fgir.Flowgraph("standardize")
    FG.new_node(FGNodeType.unknown, None)
    FG.new_node(FGNodeType.unknown, 1)
    FG.new_node(FGNodeType.unknown, 2)
    FG.new_node(FGNodeType.unknown, 3)

    # add inputs and outputs
    FG.add_input('@N0')
    FG.add_output('@N3')

    # set variables
    FG.set_var('N0', '@N0')
    FG.set_var('N3', '@N3')

    # get variables
    assert FG.get_var('N0') == '@N0'
    assert FG.get_var('N3') == '@N3'

    # check topological sorts
    assert sorted(FG.topological_sort()) == ['@N0', '@N1', '@N2', '@N3']

    # check dotfile
    FG.dotfile() # note: order changes with each run due to hashing

    # create FGIR
    FGIR = fgir.FGIR()
    FGIR.__setitem__('FG', FG)
    assert isinstance(FGIR.__getitem__('FG'), Flowgraph)

    # check node passes
    FGIR.node_pass(optimize.NodeOptimization())
    FGIR.topological_node_pass(optimize.TopologicalNodeOptimization())

    # check representations
    # note: exact memory location will change
    assert repr(FG)[:30] == '<pype.fgir.Flowgraph object at'
    assert repr(FG.get_var('N0')) == "'@N0'"

    test = FG.new_node(FGNodeType.unknown, 3)
    assert repr(test) == '<FGNodeType.unknown @N4<= : 3>'
