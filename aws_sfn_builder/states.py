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
        "Task": Task,
        "Choice": Choice,
        "Wait": Wait,
        "Succeed": Succeed,
        "Fail": Fail,
        "Parallel": Parallel,

        # Our own
        "Sequence": Sequence,
    }
    if "Type" in d:
        state_cls = state_types[d["Type"]]
    elif "type" in fields:
        assert isinstance(fields["type"], str)
        state_cls = state_types[fields["type"]]
    else:
        state_cls = State
    for sl_name in d:
        fields[state_cls.name_from_sl(sl_name)] = d[sl_name]
    fields.update(state_cls.parse_dict(d))
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
        "input_path": "InputPath",
        "output_path": "OutputPath",
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

        if isinstance(raw, State):
            # Do not recreate State instance if it is being parsed without any changes
            if not fields:
                return raw
            else:
                return cls.parse(raw.compile(), **fields)

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

        # TODO Create instance of the specified type!

        instance = cls(**fields)
        return instance

    @classmethod
    def parse_dict(cls, d: Dict) -> Dict:
        """
        Returns a dictionary listing the attributes of State that should be set.
        The keys of the returned dictionary are attribute names.
        The keys in the passed dictionary are States Language field names.

        You only need to implement parsers of nested structures.
        Plain fields will be translated to correct attributes by the
        generic parser _parse_dict.

        You will need to call and use the result of super().parse_dict(d)
        to make sure you don't lose the parent class functionality.
        """
        return {}

    def compile(self, state_visitor: Callable[["State", Dict], None]=None) -> Dict:
        c = {}
        for f in self._FIELDS.keys():
            value = getattr(self, f, None)

            # TODO Make it cleaner. Looks like a hack. Where does it belong?
            if f == "type" and value == "Sequence":
                continue

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
class Pass(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "result": "Result",
            "result_path": "ResultPath",
        },
    )

    type: str = "Pass"
    result: str = None
    result_path: str = None


@dataclasses.dataclass
class Task(Pass):
    # Inherits from Pass because it has almost all of the same fields + Retry & Catch

    _FIELDS = bidict(
        **Pass._FIELDS,
        **{
            "retry": "Retry",
            "catch": "Catch",
            "timeout_seconds": "TimeoutSeconds",
            "heartbeat_seconds": "HeartbeatSeconds",
        },
    )

    type: str = "Task"
    retry: List = None
    catch: List = None
    timeout_seconds: int = None
    heartbeat_seconds: int = None


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
class Wait(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "seconds": "Seconds",
            "seconds_path": "SecondsPath",
            "timestamp": "Timestamp",
            "timestamp_path": "TimestampPath",
        },
    )

    type: str = "Wait"
    seconds: int = None
    seconds_path: str = None
    timestamp: str = None
    timestamp_path: str = None


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
class Succeed(State):
    type: str = "Succeed"


@dataclasses.dataclass
class Parallel(Task):

    _FIELDS = bidict(
        **Task._FIELDS,
        **{
            "branches": "Branches",
        },
    )

    type: str = "Parallel"
    branches: List["Sequence"] = dataclasses.field(default_factory=list)

    @classmethod
    def parse_list(cls, raw: List) -> "Parallel":
        assert isinstance(raw, List)
        return cls(
            branches=[Sequence.parse_list(raw_branch) for raw_branch in raw],
        )

    @classmethod
    def parse_dict(cls, d: Dict):
        return {
            "branches": [State.parse(raw_branch, type="Sequence") for raw_branch in d["Branches"]]
        }

    def dry_run(self, trace: List):
        parallel_trace = []
        for branch in self.branches:
            branch_trace = []
            branch.dry_run(branch_trace)
            parallel_trace.append(branch_trace)
        trace.append(parallel_trace)
        return self.next


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

    @property
    def start_at_state(self) -> State:
        return self.states[self.start_at]

    @classmethod
    def parse_list(cls, raw: List) -> "State":
        if not isinstance(raw, list):
            raise TypeError(raw)
        if raw and all(isinstance(item, list) for item in raw):
            return Parallel.parse_list(raw)
        else:
            states = []
            for raw_state in raw:
                if isinstance(raw_state, list):
                    states.append(Sequence.parse_list(raw_state))
                else:
                    states.append(Task.parse(raw_state))
        for i, state in enumerate(states[:-1]):
            state.next = states[i + 1].name
        return cls(
            start_at=states[0].name if states else None,
            states={s.name: s for s in states},
        )

    @classmethod
    def parse_dict(cls, d: Dict):
        return {
            "states": {k: State.parse(v) for k, v in d["States"].items()},
        }

    def dry_run(self, trace):
        state = self.states[self.start_at]
        while state is not None:
            state = self.states.get(state.dry_run(trace))
        return self.next

    def insert(self, raw, before: str=None, after: str=None):
        new_state = State.parse(raw)
        if before:
            assert not after
            assert new_state.name != before
            inserted = False
            if self.start_at == before:
                self.start_at = new_state.name
                inserted = True
            for state in self.states.values():
                if state.next == before:
                    state.next = new_state.name
                    inserted = True
            if not inserted:
                raise ValueError(before)
            new_state.next = before
            self.states[new_state.name] = new_state

        elif after:
            assert not before
            assert new_state.name != after
            new_state.next = self.states[after].next
            self.states[after].next = new_state.name
            self.states[new_state.name] = new_state
        else:
            raise NotImplementedError()

    def remove(self, name: str):
        removed_state = self.states[name]
        for state in self.states.values():
            if state.next == name:
                state.next = removed_state.next
        if self.start_at == name:
            self.start_at = removed_state.next
        del self.states[name]

    def append(self, raw):
        new_state = State.parse(raw)
        if not self.states:
            self.states[new_state.name] = new_state
            self.start_at = new_state.name
            return

        terminal_states = [s for s in self.states.values() if not s.next]

        if not terminal_states:
            raise ValueError("Sequence has no terminal state, cannot append reliably")

        self.states[new_state.name] = new_state

        # There can be more than one terminal state.
        for s in terminal_states:
            s.next = new_state.name


@dataclasses.dataclass
class Machine(Sequence):

    @classmethod
    def parse(cls, raw: Union[List, Dict]) -> "Machine":
        if isinstance(raw, list):
            sequence = super().parse_list(raw)
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

        if self.start_at is None:
            return trace

        state = self.states[self.start_at]
        while state is not None:
            state = self.states.get(state.dry_run(trace))

        if len(self.states) == 1 and isinstance(self.states[self.start_at], Parallel):
            # When the state machine consists of just one Parallel,
            # remove the extra wrapping list.
            return trace[0]
        else:
            return trace
