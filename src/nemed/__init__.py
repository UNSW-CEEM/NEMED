# read version from installed package
from importlib.metadata import version
from nemed.process import *
__version__ = version("nemed")
