import pkg_resources

from .web_server import *
from .web_interface import *

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except:
    __version__ = 'unknown'
