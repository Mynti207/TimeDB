from .fgir import *
from .optimize import FlowgraphOptimization
from .error import Warn

import asyncio


class PCodeOp(object):
    '''
    A class interface for creating coroutines.

    This helps us keep track of valid computational elements. Every coroutine
    in a PCode object should be an method of PCodeOp.
    '''

    @staticmethod
    async def _node(in_qs, out_qs, func):
        '''
        A helper function to create coroutines.

        `in_qs`: an ordered list of asyncio.Queues() which hold the node's
            inputs.
        `out_qs`: a list of asyncio.Queues() into which the function's output
            should go
        `func`: the function to apply to the inputs which produces the output
            value
        '''
        input_gen = (in_q.get() for in_q in in_qs)
        input_vals = await asyncio.gather(*input_gen)

        output_val = func(*input_vals)

        for out_q in out_qs:
            await out_q.put(output_val)

    @staticmethod
    async def forward(in_qs, out_qs):
        def f(input):
            return input
        await PCodeOp._node(in_qs, out_qs, f)

    @staticmethod
    async def libraryfunction(in_qs, out_qs, function_ref):
        def f(*inputs):
            return function_ref(*inputs)
        await PCodeOp._node(in_qs, out_qs, f)

    @staticmethod
    async def librarymethod(in_qs, out_qs, method_ref):
        def f(*inputs):
            return method_ref.__get__(inputs[0])(*inputs[1:])
        await PCodeOp._node(in_qs, out_qs, f)

    @staticmethod
    async def input(in_qs, out_qs):
        def f(input):
            return input
        await PCodeOp._node(in_qs, out_qs, f)

    @staticmethod
    async def literal(out_qs, value_ref):
        def f(*inputs):
            return value_ref
        await PCodeOp._node(in_qs, out_qs, f)


class PCode(object):

    def __init__(self):
        self.inputs = []  # ordered
        self.outputs = []  # ordered
        self.ops = []  # unordered
        self.retvals = None

    def add_op(self, pcode_op_coroutine):
        self.ops.append(pcode_op_coroutine)

    async def input_generator(self, input_args):
        gen_coroutines = [q.put(i) for q, i in zip(self.inputs, input_args)]
        await asyncio.gather(*gen_coroutines)

    async def output_collector(self, future):
        col_coroutines = [q.get() for q in self.outputs]
        output_args = await asyncio.gather(*col_coroutines)
        self.retvals = output_args
        return output_args

    async def driver(self, input_args, future):
        _, value, *_ = await asyncio.gather(self.input_generator(input_args),
                                            self.output_collector(future),
                                            *self.ops)
        future.set_result(value)

    def run(self, *input_args):
        return_future = asyncio.Future()
        asyncio.ensure_future(self.driver(input_args, return_future))
        loop = asyncio.get_event_loop()
        loop.set_debug(True)
        loop.run_until_complete(return_future)
        return return_future.result()[0]


class PCodeGenerator(FlowgraphOptimization):

    def __init__(self):
        self.pcodes = {}

    def visit(self, flowgraph):

        pc = PCode()

        # create asyncio queues for every edge
        # qs is indexed by tuples of the source and destination node ids
        # for the inputs of a component, the source should be None
        qs = {}  # { (src,dst)=>asyncio.Queue(), ... }

        # populate qs by iterating over inputs of every node
        for destination in flowgraph.nodes:
            for source in flowgraph.nodes[destination].inputs:
                qs[(source, destination)] = asyncio.Queue()

        # add an extra input queue for each component input
        component_inputs = []
        for dst in flowgraph.inputs:
            q = asyncio.Queue()
            component_inputs.append(q)
            qs[(None, dst)] = q
            qs[(None, dst)]._endpoints = (None, dst)
        pc.inputs = component_inputs

        # now create all the coroutines from the nodes
        for (node_id, node) in flowgraph.nodes.items():
            node_in_qs = [qs[src_id, node_id] for src_id in node.inputs]
            out_ids = [i for (i, n) in flowgraph.nodes.items()
                       if node_id in n.inputs]
            node_out_qs = [qs[node_id, dst_id] for dst_id in out_ids]

            if node.type == FGNodeType.forward:
                pc.add_op(PCodeOp.forward(node_in_qs, node_out_qs))
            elif node.type == FGNodeType.libraryfunction:
                pc.add_op(PCodeOp.libraryfunction(node_in_qs, node_out_qs,
                                                  node.ref))
            elif node.type == FGNodeType.librarymethod:
                pc.add_op(PCodeOp.librarymethod(node_in_qs, node_out_qs,
                                                node.ref))
            elif node.type == FGNodeType.input:
                # add an extra input queue for each component input
                node_in_q = qs[(None, node_id)]
                pc.add_op(PCodeOp.input([node_in_q], node_out_qs))
            elif node.type == FGNodeType.output:
                # remove the output node and just use its input queues directly
                pc.outputs = node_in_qs
            elif node.type == FGNodeType.literal:
                pc.add_op(PCodeOp.literal(node_out_qs, node.ref))

        self.pcodes[flowgraph.name] = pc
        self.queues = qs
