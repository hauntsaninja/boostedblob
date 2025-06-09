import functools
import importlib
import pkgutil
import urllib.parse
from typing import Optional, Sequence

from .path import CloudPath

plugins: list[type[CloudPath]] = []


def _available_plugin_modules() -> Sequence[str]:
    # boostedblob_ext is a namespace package
    # submodules inside boostedblob_ext will be inspected for ExportedCloudPath attributes
    # - we use namespace package pattern so `pkgutil.iter_modules` is fast
    # - it's a separate top-level package because namespace subpackages of non-namespace
    #   packages don't quite do what you want with editable installs
    try:
        import boostedblob_ext  # type: ignore[import, unused-ignore]
    except ImportError:
        return []

    mods = []
    plugin_mods = pkgutil.iter_modules(boostedblob_ext.__path__, boostedblob_ext.__name__ + ".")
    for _, mod_name, _ in plugin_mods:
        mods.append(mod_name)
    return mods


@functools.cache
def _register_plugins() -> None:
    from .path import AzurePath, GooglePath

    plugins.append(AzurePath)
    plugins.append(GooglePath)

    for mod_name in _available_plugin_modules():
        mod = importlib.import_module(mod_name)
        if hasattr(mod, "ExportedCloudPath"):
            plugins.append(mod.ExportedCloudPath)


def try_get_cloud_path_type(url: urllib.parse.ParseResult) -> Optional[type[CloudPath]]:
    for plugin in plugins:
        if plugin.is_cloud_path(url):
            return plugin
    return None
