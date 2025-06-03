import functools
import urllib.parse
from typing import List, Optional, Type

from .path import CloudPath

plugins: List[Type[CloudPath]] = []


@functools.lru_cache()
def register_plugins() -> None:
    from .path import AzurePath, GooglePath

    global plugins
    plugins.append(GooglePath)
    plugins.append(AzurePath)

    import importlib
    import pkgutil

    for _, name, _ in pkgutil.iter_modules():
        if name.startswith("boostedblob_"):
            try:
                plugin_module = importlib.import_module(name)
                if hasattr(plugin_module, "ExportedCloudPath"):
                    plugins.append(plugin_module.ExportedCloudPath)
            except ImportError:
                continue


def try_get_cloud_path_type(url: urllib.parse.ParseResult) -> Optional[Type[CloudPath]]:
    for plugin in plugins:
        if plugin.is_cloud_path(url):
            return plugin
    return None
