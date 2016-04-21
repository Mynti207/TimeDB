import pkg_resources

from .dictdb import *
from .tsdb_client import *
from .tsdb_error import *
from .tsdb_ops import *
from .tsdb_serialization import *
from .tsdb_server import *

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except:
    __version__ = 'unknown'
