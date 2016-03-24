import importlib
import inspect
import functools

from pype.symtab import *

ATTRIB_COMPONENT = '_pype_component'


def component(func):
    '''
    Marks a functions as compatible for exposing as a component in PyPE.
    '''
    func._attributes = {ATTRIB_COMPONENT: True}
    return func


def is_component(func):
    '''
    Checks whether the @component decorator was applied to a function.
    '''
    if '_attributes' in vars(func):
        return func._attributes[ATTRIB_COMPONENT]
    return False


class LibraryImporter(object):

    def __init__(self, modname=None):
        self.mod = None
        if modname is not None:
            self.import_module(modname)

    def import_module(self, modname):
        self.mod = importlib.import_module(modname)

    def add_symbols(self, symtab):
        assert self.mod is not None, 'No module specified or loaded'
        for (name, obj) in inspect.getmembers(self.mod):
            if inspect.isroutine(obj) and is_component(obj):
                # add a symbol to symtab
                symtab.addsym(Symbol(name, SymbolType.libraryfunction, obj))
            elif inspect.isclass(obj):
                for (methodname, method) in inspect.getmembers(obj):
                    # check if method was decorated; add a symbol
                    if inspect.isroutine(method) and is_component(method):
                        sym = Symbol(methodname,
                                     SymbolType.librarymethod, method)
                        symtab.addsym(sym)
        return symtab
