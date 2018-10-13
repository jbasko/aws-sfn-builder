import json
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import dataclasses


def _generate_name():
    return str(uuid4())


@dataclasses.dataclass
class State:
    name: str = dataclasses.field(default_factory=_generate_name)
    obj: Any = None
    next_state: Optional["State"] = None

    # These fields can be used in half-baked state representations and if set
    # will be compiled into the state machine definition.
    _INJECTABLE_FIELDS = (
        "Type",
        "Comment",
        "Resource",
        "InputPath",
        "ResultPath",
        "OutputPath",
        "Result",
        "Retry",
        "Catch",
        "Choices",
        "Default",
        "TimeoutSeconds",
        "HeartbeatSeconds",
        "Seconds",
        "Error",
        "Cause",
        "Branches",
        "Next",  # if you inject this you are on your own
        "End",  # if you inject this you are on your own
    )

    @classmethod
    def parse(cls, raw: Any) -> "State":
        if isinstance(raw, list):
            raise TypeError()

        # Allow user to pass a half-baked dictionary representing the state
        # (with additional, non-standard "Name" field)
        name = str(raw)
        if isinstance(raw, dict):
            if "Name" in raw:
                name = raw["Name"]
            elif "Comment" in raw:
                name = raw["Comment"]
            else:
                # Use an auto-generated name as str(dict) will be a bad name
                name = None

        instance = cls(name=name, obj=raw)
        return instance

    def compile(self, state_visitor: Callable[["State", Dict], None]=None) -> Dict:
        c = {
            "Type": "Task",
        }

        if self.next_state:
            c["Next"] = self.next_state.name
        else:
            c["End"] = True

        if hasattr(self.obj, 'get_state_attrs'):
            c.update(getattr(self.obj, 'get_state_attrs')(state=self))

        if isinstance(self.obj, dict):
            for k in self._INJECTABLE_FIELDS:
                if k in self.obj:
                    c[k] = self.obj[k]

        if state_visitor is not None:
            state_visitor(self, c)

        return c

    def dry_run(self, trace: List):
        trace.append(self.name)
        return self.next_state


@dataclasses.dataclass
class Parallel(State):
    branches: List["Sequence"] = dataclasses.field(default_factory=list)

    @classmethod
    def parse(cls, raw: List) -> "Parallel":
        assert isinstance(raw, List)
        return cls(
            branches=[Sequence.parse(raw_branch) for raw_branch in raw],
        )

    def compile(self, state_visitor: Callable[["State", Dict], None]=None) -> Dict:
        c = {
            **super().compile(),
            "Type": "Parallel",
            "Comment": self.name,
            "Branches": [b.compile(state_visitor=state_visitor) for b in self.branches],
        }
        if state_visitor is not None:
            state_visitor(self, c)
        return c

    def dry_run(self, trace: List):
        parallel_trace = []
        for branch in self.branches:
            branch_trace = []
            branch.dry_run(branch_trace)
            parallel_trace.append(branch_trace)
        trace.append(parallel_trace)
        return self.next_state


@dataclasses.dataclass
class Sequence(State):
    start_at: State = None
    states: Dict[str, State] = dataclasses.field(default_factory=dict)

    @classmethod
    def parse(cls, raw: List) -> "Sequence":
        assert isinstance(raw, list)
        if raw and all(isinstance(item, list) for item in raw):
            states = [Parallel.parse(raw)]
        else:
            states = []
            for raw_state in raw:
                if isinstance(raw_state, list):
                    states.append(Sequence.parse(raw_state))
                else:
                    states.append(State.parse(raw_state))
        for i, state in enumerate(states[:-1]):
            state.next_state = states[i + 1]
        return cls(
            start_at=states[0],
            states={s.name: s for s in states},
        )

    def dry_run(self, trace):
        state = self.start_at
        while state is not None:
            state = state.dry_run(trace)
        return self.next_state

    def compile(self, state_visitor: Callable[["State", Dict], None]=None) -> Dict:
        compiled_state = {
            "StartAt": self.start_at.name,
            "States": {s.name: s.compile(state_visitor=state_visitor) for s in self.states.values()},
        }
        if state_visitor is not None:
            state_visitor(self, compiled_state)
        return compiled_state


@dataclasses.dataclass
class Machine(Sequence):

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
        state = self.start_at
        while state is not None:
            state = state.dry_run(trace)

        if len(self.states) == 1 and isinstance(self.start_at, Parallel):
            # When the state machine consists of just one Parallel,
            # remove the extra wrapping list.
            return trace[0]
        else:
            return trace
