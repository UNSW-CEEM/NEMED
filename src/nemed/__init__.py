# read version from installed package
#from importlib.metadata import version
from .nemed import *
#__version__ = version("nemed")
import logging
import sys

logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(levelname)s: %(message)s"
)