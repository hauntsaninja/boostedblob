__version__ = "0.15.4"

import sys

if "pkg_resources" not in sys.modules:
    # workaround for the following:
    # https://github.com/benoitc/gunicorn/pull/2539
    sys.modules["pkg_resources"] = object()  # type: ignore[assignment]
    import aiohttp  # noqa: F401

    del sys.modules["pkg_resources"]

from . import read as read
from . import share as share
from . import write as write
from .boost import BoostExecutor as BoostExecutor
from .copying import copyfile as copyfile
from .copying import copytree as copytree
from .delete import remove as remove
from .delete import rmtree as rmtree
from .globals import ensure_session as ensure_session
from .listing import listdir as listdir
from .listing import listtree as listtree
from .listing import scandir as scandir
from .listing import scantree as scantree
from .path import AzurePath as AzurePath
from .path import BasePath as BasePath
from .path import BlobPath as BlobPath
from .path import CloudPath as CloudPath
from .path import GooglePath as GooglePath
from .path import LocalPath as LocalPath
from .path import exists as exists
from .path import getsize as getsize
from .path import isdir as isdir
from .path import isfile as isfile
from .path import stat as stat
from .syncing import sync as sync
