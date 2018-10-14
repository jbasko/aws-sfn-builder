import json
from typing import Any, Callable, Dict, List, Type, Union
from uuid import uuid4

import dataclasses
from bidict import bidict


def _generate_name():
    return str(uuid4())


def _parse_dict(d, **fields) -> "State":
    state_types = {
        "Pass": Pass,
        "Parallel": Parallel,
        "Choice": Choice,
        "Fail": Fail,
        "Task": Task,
    }
    state_cls = state_types.get(d.get("Type", None), State)
    for sl_name in d:
        fields[state_cls.name_from_sl(sl_name)] = d[sl_name]
    return state_cls(**fields)


def _compile(value, state_visitor: Callable[["State", Dict], None]=None):
    if isinstance(value, State):
        return value.compile(state_visitor=state_visitor)
    elif isinstance(value, list):
        return [_compile(item, state_visitor=state_visitor) for item in value]
    elif isinstance(value, dict):
        return {k: _compile(v, state_visitor=state_visitor) for k, v in value.items()}
    else:
        return value


@dataclasses.dataclass
class State:
    _FIELDS = bidict({
        "type": "Type",
        "comment": "Comment",
        "next": "Next",
        "end": "End",
        "resource": "Resource",
    })

    # Our fields are not part of States Language and therefore
    # should not be included in the compiled definitions, but
    # are accepted in the input.
    _OUR_FIELDS = bidict({
        "name": "Name",
    })

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

    obj: Any = None  # TODO Rename it to raw_obj
    name: str = dataclasses.field(default_factory=_generate_name)

    type: Type = None
    comment: str = None
    next: str = None
    end: bool = None
    resource: str = None

    @classmethod
    def parse(cls, raw: Any, **fields) -> "State":
        if isinstance(raw, list):
            raise TypeError()

        if isinstance(raw, dict):
            if "Name" in raw:
                fields.setdefault("name", raw["Name"])
            elif "Comment" in raw:
                fields.setdefault("name", raw["Comment"])
            return _parse_dict(raw, **fields)

        fields.setdefault("name", str(raw))
        fields.setdefault("obj", raw)

        instance = cls(**fields)
        return instance

    def compile(self, state_visitor: Callable[["State", Dict], None]=None) -> Dict:
        c = {}
        for f in self._FIELDS.keys():
            value = getattr(self, f, None)
            if value is not None:
                c[self._FIELDS[f]] = _compile(value, state_visitor=state_visitor)

        if hasattr(self.obj, 'get_state_attrs'):
            c.update(getattr(self.obj, 'get_state_attrs')(state=self))

        if state_visitor is not None:
            state_visitor(self, c)

        return c

    def dry_run(self, trace: List):
        trace.append(self.name)
        return self.next


@dataclasses.dataclass
class Task(State):
    type: str = "Task"


@dataclasses.dataclass
class Pass(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "result": "Result",
        },
    )

    type: str = "Pass"
    result: str = None


@dataclasses.dataclass
class Fail(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "cause": "Cause",
            "error": "Error",
        },
    )

    type: str = "Fail"
    cause: str = None
    error: str = None


@dataclasses.dataclass
class Parallel(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "branches": "Branches",
        },
    )

    type: str = "Parallel"
    branches: List["Sequence"] = dataclasses.field(default_factory=list)

    @classmethod
    def parse(cls, raw: List) -> "Parallel":
        assert isinstance(raw, List)
        return cls(
            branches=[Sequence.parse(raw_branch) for raw_branch in raw],
        )

    def dry_run(self, trace: List):
        parallel_trace = []
        for branch in self.branches:
            branch_trace = []
            branch.dry_run(branch_trace)
            parallel_trace.append(branch_trace)
        trace.append(parallel_trace)
        return self.next


@dataclasses.dataclass
class Choice(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "choices": "Choices",
            "default": "Default",
        },
    )

    type: str = "Choice"
    choices: List = dataclasses.field(default_factory=list)
    default: str = None


@dataclasses.dataclass
class Sequence(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "start_at": "StartAt",
            "states": "States",
        },
    )

    start_at: str = None
    states: Dict[str, State] = dataclasses.field(default_factory=dict)

    @classmethod
    def parse(cls, raw: List) -> "State":
        if not isinstance(raw, list):
            raise TypeError(raw)
        if raw and all(isinstance(item, list) for item in raw):
            return Parallel.parse(raw)
        else:
            states = []
            for raw_state in raw:
                if isinstance(raw_state, list):
                    states.append(Sequence.parse(raw_state))
                else:
                    states.append(Task.parse(raw_state))
        for i, state in enumerate(states[:-1]):
            state.next = states[i + 1].name
        return cls(
            start_at=states[0].name,
            states={s.name: s for s in states},
        )

    def dry_run(self, trace):
        state = self.states[self.start_at]
        while state is not None:
            state = self.states.get(state.dry_run(trace))
        return self.next


@dataclasses.dataclass
class Machine(Sequence):

    @classmethod
    def parse(cls, raw: Union[List, Dict]) -> "Machine":
        if isinstance(raw, list):
            sequence = super().parse(raw)
            if isinstance(sequence, Parallel):
                return cls(start_at=sequence.name, states={sequence.name: sequence})
            assert isinstance(sequence, Machine)
            return sequence
        elif isinstance(raw, dict):
            # Could be a proper state machine definition
            return cls(
                start_at=raw["StartAt"],
                states={state_name: Task.parse(state, name=state_name) for state_name, state in raw["States"].items()},
            )
        else:
            raise TypeError(raw)

    def to_json(self, json_options=None, state_visitor: Callable[[State, Dict], None]=None):
        """
        Generate a JSON that can be used as a State Machine definition.

        If you need to customise the generated output, pass state_visitor which
        will be called for every compiled state dictionary.
        """
        json_options = json_options or {}
        json_options.setdefault("indent", 4)
        return json.dumps(self.compile(state_visitor=state_visitor), **json_options)

    def dry_run(self, trace: List=None):
        if trace is None:
            trace = []
        state = self.states[self.start_at]
        while state is not None:
            state = self.states.get(state.dry_run(trace))

        if len(self.states) == 1 and isinstance(self.states[self.start_at], Parallel):
            # When the state machine consists of just one Parallel,
            # remove the extra wrapping list.
            return trace[0]
        else:
            return trace
