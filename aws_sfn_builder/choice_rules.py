import datetime as dt
from typing import Any, Callable, Dict

import dataclasses
from bidict import bidict
from jsonpath_ng import parse as parse_jsonpath

from .base import Node


class _OperatorDef:
    def __init__(self, impl: Callable=None):
        self.name = None
        self.impl = impl

    def __set_name__(self, owner, name):
        self.name = name
        owner.ALL[self.name] = self

    def __get__(self, instance, owner):
        return self


def to_bool(x):
    return bool(x)


def to_numeric(x):
    try:
        return int(x)
    except ValueError:
        return float(x)


def to_timestamp(x):
    if isinstance(x, dt.datetime):
        return x.isoformat()
    return dt.datetime.fromisoformat(x).isoformat()


class Operators:
    ALL: Dict[str, _OperatorDef] = {}

    And = _OperatorDef()
    BooleanEquals = _OperatorDef(lambda a, x: to_bool(x) is a)
    Not = _OperatorDef()
    NumericEquals = _OperatorDef(lambda a, x: to_numeric(x) == a)
    NumericGreaterThan = _OperatorDef(lambda a, x: to_numeric(x) > a)
    NumericGreaterThanEquals = _OperatorDef(lambda a, x: to_numeric(x) >= a)
    NumericLessThan = _OperatorDef(lambda a, x: to_numeric(x) < a)
    NumericLessThanEquals = _OperatorDef(lambda a, x: to_numeric(x) <= a)
    Or = _OperatorDef()
    StringEquals = _OperatorDef(lambda a, x: str(x) == a)
    StringGreaterThan = _OperatorDef(lambda a, x: str(x) > a)
    StringGreaterThanEquals = _OperatorDef(lambda a, x: str(x) >= a)
    StringLessThan = _OperatorDef(lambda a, x: str(x) < a)
    StringLessThanEquals = _OperatorDef(lambda a, x: str(x) <= a)
    TimestampEquals = _OperatorDef(lambda a, x: to_timestamp(x) == a)
    TimestampGreaterThan = _OperatorDef(lambda a, x: to_timestamp(x) > a)
    TimestampGreaterThanEquals = _OperatorDef(lambda a, x: to_timestamp(x) >= a)
    TimestampLessThan = _OperatorDef(lambda a, x: to_timestamp(x) < a)
    TimestampLessThanEquals = _OperatorDef(lambda a, x: to_timestamp(x) <= a)


@dataclasses.dataclass
class Operator(Node):
    _FIELDS = bidict(
        **Node._FIELDS,
        **{
            "variable": "Variable",
            "next": "Next",
        },
    )

    type: str = "Operator"
    variable: str = None
    next: str = None
    name: str = None  # name of the operator
    value: Any = None  # value that the variable is being compared to

    def __post_init__(self):
        assert self.name in Operators.ALL

    @classmethod
    def parse_dict(cls, d: Dict, fields: Dict) -> None:
        op_fields = {}
        for k, v in d.items():
            if k == "Variable":
                op_fields["variable"] = v
            elif k == "Next":
                op_fields["next"] = v
            else:
                op_fields["name"] = k
                if k in ("And", "Or"):
                    op_fields["value"] = [Operator.parse(item) for item in v]
                elif k == "Not":
                    op_fields["value"] = Operator.parse(v)
                else:
                    op_fields["value"] = v
        fields.update(op_fields)

    def compile_dict(self, c: Dict):
        if self.name in ("And", "Or"):
            c[self.name] = [item.compile() for item in self.value]
        elif self.name == "Not":
            c[self.name] = self.value.compile()
        else:
            c[self.name] = self.value

    def matches(self, input) -> bool:
        if self.name == "Not":
            return not self.value[0].matches(input)

        elif self.name == "Or":
            return any(v.matches(input) for v in self.value)

        elif self.name == "And":
            return all(v.matches(input) for v in self.value)

        else:
            path = parse_jsonpath(self.variable)
            check_value = path.find(input)[0].value
            return Operators.ALL[self.name].impl(self.value, check_value)


@dataclasses.dataclass
class ChoiceRule(Node):
    _FIELDS = bidict(
        **Node._FIELDS,
        **{
            "variable": "Variable",
            "next": "Next",
        },
    )

    type: str = "ChoiceRule"
    variable: str = None
    operator: Operator = None
    next: str = None

    def matches(self, input) -> bool:
        return self.operator.matches(input)

    @classmethod
    def parse_dict(cls, d: Dict, fields: Dict) -> None:
        fields["operator"] = Operator.parse(d)

    def compile_dict(self, c: Dict):
        c.update(self.operator.compile())
