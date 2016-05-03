import pkg_resources

from procs._corr import *
from procs.corr import *
from procs.junk import *
from procs.stats import *

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except:
    __version__ = 'unknown'
