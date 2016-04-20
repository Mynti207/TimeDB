import pkg_resources

from pype.ast import *
from pype.error import *
from pype.fgir import *
from pype.lexer import *
from pype.lib_import import *
from pype.optimize import *
from pype.parser import *
from pype.pcode import *
from pype.pipeline import *
from pype.semantic_analysis import *
from pype.symtab import *
from pype.translate import *

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except:
    __version__ = 'unknown'
