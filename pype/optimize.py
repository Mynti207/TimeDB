from pype.fgir import *
from pype.error import *

# Optimization pass interfaces


class Optimization(object):
    def visit(self, obj): pass


class FlowgraphOptimization(Optimization):
    '''
    Called on each flowgraph in a FGIR.

    May modify the flowgraph by adding or removing nodes (return a new
    Flowgraph). If you modify nodes, make sure inputs, outputs, and variables
    are all updated. May NOT add or remove flowgraphs.
    '''
    pass


class TopologicalFlowgraphOptimization(Optimization):
    '''
    Called on each flowgraph in a FGIR, in dependent order.
    Components which are used by other components will be called first.
    '''
    pass


class NodeOptimization(Optimization):
    '''
    Called on each node in a FGIR.

    May modify the node (return a new Node object, and it will be assigned).
    May NOT remove or add nodes (use a component pass).
    '''
    pass


class TopologicalNodeOptimization(NodeOptimization):
    pass

# Optimization pass implementations


class PrintIR(TopologicalNodeOptimization):
    '''
    A simple "optimization" pass which can be used to debug topological sorting
    '''

    def visit(self, node):
        print(str(node))


class AssignmentEllision(FlowgraphOptimization):
    '''
    Eliminates all assignment nodes.

    Assignment nodes are useful for the programmer to reuse the output of an
    expression multiple times, and the lowering transformation generates
    explicit flowgraph nodes for these expressions. However, they are not
    necessary for execution, as they simply forward their value. This removes
    them and connects their pre- and post-dependencies.
    '''

    def visit(self, flowgraph):

        # use a copy to avoid the following error when executing the loop
        # RuntimeError: dictionary changed size during iteration
        tmp_nodes = flowgraph.nodes.copy()

        # loop through all nodes in the flowgraph
        for (idx, node) in tmp_nodes.items():

            # check if it is an assignment node
            if node.type == FGNodeType.assignment:

                # get the node's predecessor
                # note: there is always only one predecessor node
                parent = node.inputs[0]

                # take nodes which depend on the assignment node,
                # and modify them to point to the predecessor node

                # loop through all nodes
                for (idx2, node2) in flowgraph.nodes.items():

                    # check if the node is a successor
                    if idx in node2.inputs:
                        # drop the node
                        node2.inputs.remove(idx)
                        # add its predecessor
                        node2.inputs.append(parent)

                # clean up variable name mappings, i.e. update flowgraph's
                # variable attribute to have the string map to its predecessor
                if idx in flowgraph.variables:
                    flowgraph.variables[idx] = parent

                # remove node
                del flowgraph.nodes[idx]

        # return updated flowgraph
        return flowgraph


class DeadCodeElimination(FlowgraphOptimization):
    '''
    Eliminates unreachable expression statements.

    Statements which never affect any output are effectively useless, and we
    call these "dead code" blocks. This optimization removes any expressions
    which can be shown not to affect the output.
    NOTE: input statements *cannot* safely be removed, since doing so would
    change the call signature of the component. For example, it might seem that
    the input x could be removed:
        { component1 (input x y) (output y) }
    but imagine this component1 was in a file alongside this one:
        { component2 (input a b) (:= c (component a b)) (output c) }
    By removing x from component1, it could no longer accept two arguments. So
    in this instance, component1 will end up unmodified after DCE.
    '''

    def visit(self, flowgraph):

        # determine which nodes can be reached from the output nodes
        # by following dependencies
        # note: input nodes cannot be safely removed, so consider 'reachable'
        reachable_nodes = flowgraph.inputs[:] + flowgraph.outputs[:]
        test_outputs = flowgraph.outputs[:]  # consider converting to queue?
        already_tested = []

        # loop through all outputs and their predecessors
        while len(test_outputs) > 0:

            if test_outputs[0] not in already_tested:

                # add to reachable nodes
                if flowgraph.nodes[test_outputs[0]].type != FGNodeType.output:
                    reachable_nodes.append(test_outputs[0])

                # grow list of nodes to check
                test_outputs.extend(flowgraph.nodes[test_outputs[0]].inputs)

                # keep track of tested nodes for efficiency
                already_tested.extend(test_outputs[0])

            # drop node that has just been tested
            test_outputs.remove(test_outputs[0])

        # drop duplicates
        reachable_nodes = set(reachable_nodes)

        # determine nodes that can be dropped
        unreachable_nodes = set(flowgraph.nodes.keys()) - reachable_nodes

        # delete unreachable nodes and their variable reference
        if len(unreachable_nodes) > 0:

            # http://stackoverflow.com/questions/483666/python-reverse-inverse
            # -a-mapping
            lookup_dict = {v: k for k, v in flowgraph.variables.items()}

            for node in unreachable_nodes:
                del flowgraph.nodes[node]
                if node in lookup_dict:
                    del flowgraph.variables[lookup_dict[node]]

        return flowgraph


class InlineComponents(TopologicalFlowgraphOptimization):
    '''
    Replaces every component invocation with a copy of that component's
    flowgraph. Topological order guarantees that we inline components before
    they are invoked.
    '''

    def __init__(self):
        self.component_cache = {}

    def visit(self, flowgraph):

        for (cnode_id, cnode) in [(nid, n) for (nid, n) in flowgraph.nodes.items() if n.type == FGNodeType.component]:

            target = self.component_cache[cnode.ref]
            # add a copy of every node in target flowgraph
            id_map = {}  # maps node ids in target to node id's in flowgraph
            for tnode in target.nodes.values():
                if (tnode.type == FGNodeType.input or tnode.type == FGNodeType.output):
                    newtype = FGNodeType.forward
                else:
                    newtype = tnode.type
                n = flowgraph.new_node(newtype, ref=tnode.ref)
                id_map[tnode.nodeid] = n.nodeid
                # Connect all copies together
                for tid, tnode in target.nodes.items():
                    flowgraph.nodes[id_map[tid]].inputs = [id_map[i] for
                                                           i in tnode.inputs]

                # Link inputs of cnode to inputs of target flowgraph
                for cnode_input, targ_input in zip(cnode.inputs, target.inputs):
                    flowgraph.nodes[id_map[targ_input]].inputs = [cnode_input]

                # Link output of target flowgraph to outputs of cnode
                for oid, onode in flowgraph.nodes.items():
                    if cnode_id in onode.inputs:
                        onode.inputs[onode.inputs.index(cnode_id)] = id_map[target.outputs[0]]

                # Remove all other references to cnode in flowgraph
                del flowgraph.nodes[cnode_id]
                victims = [s for s, nid in flowgraph.variables.items()
                           if nid == cnode_id]
                for v in victims:
                    del flowgraph.variables[v]

            self.component_cache[flowgraph.name] = flowgraph
            return flowgraph
