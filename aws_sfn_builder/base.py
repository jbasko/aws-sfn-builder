from typing import Any, ClassVar, Dict, Type

import dataclasses
from bidict import bidict


def _compile_node_value(value: Any, **compile_options) -> Any:
    if isinstance(value, Node):
        return value.compile(**compile_options)
    elif isinstance(value, list):
        return [_compile_node_value(item, **compile_options) for item in value]
    elif isinstance(value, dict):
        return {k: _compile_node_value(v, **compile_options) for k, v in value.items()}
    else:
        return value


def _parse_node_dict(d: Dict, **fields) -> "Node":
    state_types = Node._NODE_CLASSES

    if "Type" in d:
        state_cls = state_types[d["Type"]]
    elif "type" in fields:
        assert isinstance(fields["type"], str)
        state_cls = state_types[fields["type"]]
    else:
        state_cls = Node

    for attr_name, sl_name in state_cls._FIELDS.items():
        if sl_name in d:
            fields[attr_name] = d[sl_name]

    state_cls.parse_dict(d, fields)

    try:
        return state_cls(**fields)
    except TypeError as e:
        raise TypeError(f"Failed to instantiate {state_cls} because of: {e!r}")


@dataclasses.dataclass
class Node:
    """
    Base class for all nodes in the state machine object tree.
    """

    _FIELDS: ClassVar[bidict] = bidict()
    _OUR_FIELDS: ClassVar[bidict] = bidict()

    _NODE_CLASSES: ClassVar[Dict[str, Type]] = {}

    type: str = "Node"

    def __init_subclass__(cls, **kwargs):
        Node._NODE_CLASSES[cls.__name__] = cls

    @classmethod
    def name_from_sl(cls, name):
        """
        Translate a field name from States Language.
        """
        if name in cls._FIELDS.inv:
            return cls._FIELDS.inv[name]
        elif name in cls._OUR_FIELDS.inv:
            return cls._OUR_FIELDS.inv[name]
        raise KeyError(name)

    @classmethod
    def name_to_sl(cls, name):
        """
        Translate an attribute name to States Language.
        """
        return cls._FIELDS[name]

    @classmethod
    def parse(cls, raw: Any, **fields) -> "Node":

        if isinstance(raw, Node):
            # Do not recreate Node instance if it is being parsed without any changes
            if not fields:
                return raw
            else:
                return cls.parse(raw.compile(), **fields)

        if isinstance(raw, list):
            raise TypeError()

        # TODO Add an explicit test for this
        fields.setdefault("type", cls.__name__)

        # TODO None of the below belongs to Node class! Move to State.

        field_names = [f.name for f in dataclasses.fields(cls)]

        if isinstance(raw, dict):
            if "name" in field_names:
                if "Name" in raw:
                    fields.setdefault("name", raw["Name"])
                elif "Resource" in raw:
                    fields.setdefault("name", raw["Resource"])
                elif "Comment" in raw:
                    fields.setdefault("name", raw["Comment"])

            return _parse_node_dict(raw, **fields)

        if "name" in field_names:
            fields.setdefault("name", str(raw))

        if "obj" in field_names:
            fields.setdefault("obj", raw)

        # TODO Create instance of the specified type!

        instance = cls(**fields)
        return instance

    def compile(self, **compile_options) -> Dict:
        c = {}
        for f in self._FIELDS.keys():
            value = getattr(self, f, None)

            if value is not None:
                c[self._FIELDS[f]] = _compile_node_value(value, **compile_options)

        self.compile_dict(c)

        return c

    @classmethod
    def parse_dict(cls, d: Dict, fields: Dict) -> None:
        """
        A hook for custom Node classes.

        ``fields`` to be modified in place with the values parsed from ``d``.

        DO NOT call super().
        """
        pass

    def compile_dict(self, c: Dict) -> None:
        """
        A hook for custom Node class to add its own compile logic.
        DO NOT call super().
        The dictionary should be modified in place.
        This is called before applying external handlers (state_visitor).
        """
        pass
