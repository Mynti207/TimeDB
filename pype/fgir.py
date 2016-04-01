import enum

FGNodeType = enum.Enum('FGNodeType',
                       'component libraryfunction librarymethod input output '
                       'assignment literal forward unknown')


class FGNode(object):
    def __init__(self, nodeid, nodetype, ref=None, inputs=[]):
        self.nodeid = nodeid
        self.type = nodetype
        self.ref = ref
        self.inputs = inputs

    def __repr__(self):
        disp = '<' + str(self.type) + ' ' + str(self.nodeid) + '<=' + \
            ','.join(map(str, self.inputs))+' : ' + str(self.ref) + '>'
        return disp


class Flowgraph(object):
    def __init__(self, name='?'):
        self.name = name
        self.variables = {}  # { str -> nodeid }
        self.nodes = {}  # { nodeid -> Node }
        self.inputs = []  # [ nodeid, ... ]
        self.outputs = []  # [ nodeid, ... ]
        self._id_counter = 0

    def new_node(self, nodetype, ref=None):
        nid = '@N' + str(self._id_counter)
        self._id_counter += 1
        node = FGNode(nid, nodetype, ref, [])
        self.nodes[nid] = node
        return node

    def get_var(self, name):
        return self.variables.get(name, None)

    def set_var(self, name, nodeid):
        self.variables[name] = nodeid

    def add_input(self, nodeid):
        self.inputs.append(nodeid)

    def add_output(self, nodeid):
        self.outputs.append(nodeid)

    def dotfile(self):
        s = ''
        s += 'digraph '+self.name+' {\n'
        for (src, node) in self.nodes.items():
            for dst in node.inputs:
                s += '  "'+str(dst)+'" -> "'+str(src)+'"\n'
        for (var, nid) in self.variables.items():
            s += '  "'+str(nid)+'" [ label = "'+str(var)+'" ]\n'
        for nid in self.inputs:
            s += '  "'+str(nid)+'" [ color = "green" ]\n'
        for nid in self.outputs:
            s += '  "'+str(nid)+'" [ color = "red" ]\n'
        s += '}\n'
        return s

    def pre(self, nodeid):
        return self.nodes[nodeid].inputs

    def post(self, nodeid):
        return [i for (i, n) in self.nodes.items()
                if nodeid in self.nodes[i].inputs]

    def visit(self, L, nodelist, n, tempmark):
        '''
        Helper function for topological sort

        Inputs:
        ----------
        L: list containing sorted nodes
        nodelist: list of unmarked nodes
        n: selected node n to visit (still in the list of unmarked nodes)
        tempmark: list of temporarily marked nodes

        See Wikipedia: (DFS)
        https://en.wikipedia.org/wiki/Topological_sorting
        '''

        if n in tempmark:
            # if n has a temporary mark then stop (not a DAG)
            print("WARNING: NOT A DAG", n)
            return

        if n in L:
            return  # do nothing

        # if n is not marked (i.e. has not been visited yet) then
        #   mark n temporarily
        #   for each node m with an edge from n to m do
        #        visit(m)
        #   mark n permanently
        #   unmark n temporarily
        #   add n to head of L

        tempmark.append(n)

        for m in self.nodes[n].inputs:
            self.visit(L, nodelist, m, tempmark)

        # print("L", L)
        L.append(n)  # assume if node in L, n is marked as visited
        tempmark.remove(n)
        nodelist.remove(n)

        return

    def topological_sort(self):
        '''
        A topological sort
        Reference Graphs lecture: (DFS)
        https://github.com/iacs-cs207/cs207/blob/master/lectures/Graphs.ipynb
        Implement DFS, see Wikipedia:
        https://en.wikipedia.org/wiki/Topological_sorting
        '''

        L = []  # (initially) empty list that will contain the sorted nodes

        nodelist = list(self.nodes.keys())  # unmarked nodes
        # print("nodelist", nodelist)
        tempmark = []  # temporarily marked nodes

        while len(nodelist) > 0:  # while there are unmarked nodes, do
            unmarkednode = nodelist[0]  # select an unmarked node n
            self.visit(L, nodelist, unmarkednode, tempmark)  # visit n

        # print L  # pre-reverse, like in slides
        L.reverse()

        # should return a list of node ids in sorted order
        # Bob wants inputs before outputs
        return L


class FGIR(object):

    def __init__(self):
        self.graphs = {}  # { component_name:str => Flowgraph }

    def __getitem__(self, component):
        return self.graphs[component]

    def __setitem__(self, component, flowgraph):
        self.graphs[component] = flowgraph

    def __iter__(self):
        for component in self.graphs:
            yield component

    def flowgraph_pass(self, flowgraph_optimizer):
        for component in self.graphs:
            fg = flowgraph_optimizer.visit(self.graphs[component])
            if fg is not None:
                self.graphs[component] = fg

    # def node_pass(self, node_optimizer, *args, topological=False):
    def node_pass(self, node_optimizer, topological=False):
        for component in self.graphs:
            fg = self.graphs[component]
            if topological:
                node_order = fg.topological_sort()
            else:
                node_order = fg.nodes.keys()
            for node in node_order:
                n = node_optimizer.visit(fg.nodes[node])
                if n is not None:
                    fg.nodes[node] = n

    def topological_node_pass(self, topo_optimizer):
        self.node_pass(topo_optimizer, topological=True)
