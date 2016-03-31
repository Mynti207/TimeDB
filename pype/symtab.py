import collections
import enum

SymbolType = enum.Enum(
    'SymbolType', 'component var input output libraryfunction librarymethod')

Symbol = collections.namedtuple('Symbol', 'name type ref')


class SymbolTable(object):
    '''
    SymbolTable: a dictionary of scoped symbol tables
    Each scoped symbol table is a dictionary of metadata for each variable.
    '''

    def __init__(self):
        # {scope: {name:str => {type:SymbolType => ref:object} }}
        self.T = {}
        self.T['global'] = {}

    def __getitem__(self, component):
        return self.T[component]

    def scopes(self):
        return self.T.keys()

    def __repr__(self):
        return str(self.T)

    def pprint(self):
        '''
        Loop through all defined scopes and print all the symbols
        associated with them.

        Format of the print:
        scope
            symbol1 name => symbol details
            symbol2 name => symbol details
        '''
        print('---SYMBOL TABLE---')
        for (scope, table) in self.T.items():
            print(scope)
            for (name, symbol) in table.items():
                print(' ', name, '=>', symbol)

    def addsym(self, sym, scope='global'):
        '''
        Adds a symbols to the symbol table, in the relevant dictionary
        item that corresponds to the symbol scope.
        '''

        # create dictionary if scope is not already present
        if scope not in self.T:
            self.T[scope] = {}

        # in all cases, add symbol
        self.T[scope][sym.name] = sym

    def lookupsym(self, sym, scope=None):
        if scope is not None:
            if sym in self.T[scope]:
                return self.T[scope][sym]
        if sym in self.T['global']:
            return self.T['global'][sym]
        return None
