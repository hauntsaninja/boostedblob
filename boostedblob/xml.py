from typing import TYPE_CHECKING, Any, Mapping, Optional

if TYPE_CHECKING:
    import xml.etree.ElementTree as _etree

    etree = _etree
else:
    from lxml import etree


def dict_to_xml(dictionary: Mapping[str, Any]) -> bytes:
    if len(dictionary) != 1:
        raise ValueError("Document must have exactly one root")

    def inner(parent_key: str, value: Any, tree: Optional[etree.Element]) -> None:
        # Child is responsible for adding the parent tag to tree
        if isinstance(value, dict):
            current = root if tree is None else etree.SubElement(tree, parent_key)
            for k, v in value.items():
                inner(k, v, current)
        elif isinstance(value, list):
            for v in value:
                inner(parent_key, v, tree)
        else:
            assert tree is not None
            current = etree.SubElement(tree, parent_key)
            current.text = str(value)

    root_key, root_value = next(iter(dictionary.items()))
    root = etree.Element(root_key)
    inner(root_key, root_value, None)

    return etree.tostring(root, encoding="utf-8", xml_declaration=True)
