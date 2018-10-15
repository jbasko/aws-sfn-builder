import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union
from uuid import uuid4

import dataclasses
from bidict import bidict
from jsonpath_ng import parse as parse_jsonpath

from .base import Node
from .choice_rules import ChoiceRule


def _generate_name():
    return str(uuid4())


class States:
    """
    Namespace for all names of states.
    """

    Pass = "Pass"
    Task = "Task"
    Choice = "Choice"
    Wait = "Wait"
    Succeed = "Succeed"
    Fail = "Fail"
    Parallel = "Parallel"

    Sequence = "Sequence"
    Machine = "Machine"

    ALL = [
        Pass,
        Task,
        Choice,
        Wait,
        Succeed,
        Fail,
        Parallel,
        Sequence,
        Machine,
    ]

    _TERMINAL = [
        Succeed,
        Fail,
        # + any End State
    ]

    _INTERNAL = [
        Sequence,
        Machine,
    ]

    @classmethod
    def is_terminal(cls, state: "State"):
        return state.next is None or state.type in cls._TERMINAL

    @classmethod
    def is_internal(cls, state: "State"):
        return state.type in cls._INTERNAL


@dataclasses.dataclass
class State(Node):
    _FIELDS = bidict(
        **Node._FIELDS,
        **{
            "type": "Type",
            "comment": "Comment",
            "next": "Next",
            "end": "End",
            "resource": "Resource",
            "input_path": "InputPath",
            "output_path": "OutputPath",
            "result_path": "ResultPath",
        },
    )

    # Our fields are not part of States Language and therefore
    # should not be included in the compiled definitions, but
    # are accepted in the input.
    _OUR_FIELDS = bidict({
        "name": "Name",
    })

    obj: Any = None  # TODO Rename it to raw_obj
    name: str = dataclasses.field(default_factory=_generate_name)

    type: Type = None
    comment: str = None
    next: str = None
    end: bool = None
    resource: str = None
    input_path: str = None
    output_path: str = None
    result_path: str = None

    @classmethod
    def parse(cls, raw: Any, **fields) -> "State":
        # Dictionary with no type defaults to Task which is most likely state
        # that user wants to instantiate.
        if not isinstance(raw, State):
            fields.setdefault("type", States.Task)
        return super().parse(raw, **fields)

    def compile(self, **compile_options) -> Dict:
        c = super().compile(**compile_options)

        # Do not include "Type" for our internal "states" such as "Machine" or "Sequence".
        if States.is_internal(self) and "Type" in c:
            del c["Type"]

        # TODO Rethink this feature.
        if hasattr(self.obj, 'get_state_attrs'):
            c.update(getattr(self.obj, 'get_state_attrs')(state=self))

        state_visitor = compile_options.get('state_visitor')
        if state_visitor is not None:
            state_visitor(self, c)

        return c

    def format_state_input(self, input):
        """
        Applies InputPath
        """
        if self.input_path:
            return parse_jsonpath(self.input_path).find(input)[0].value
        return input

    def format_result(self, input, resource_result):
        """
        Applies ResultPath
        """
        if self.result_path:
            result_path = parse_jsonpath(self.result_path)
            if not result_path.find(input):
                # A quick hack to set a non-existent key (assuming the parent of the path is a dictionary).
                result_path.left.find(input)[0].value[str(result_path.right)] = resource_result
                return input
            elif str(result_path) == "$":
                return resource_result
            else:
                result_path.update(input, resource_result)
                return input
        return resource_result

    def format_state_output(self, result):
        """
        Applies OutputPath
        """
        if not self.output_path:
            return result

        output_path = parse_jsonpath(self.output_path)
        if str(output_path) == "$":
            # From docs:
            # If the OutputPath has the default value of $, this matches the entire input completely.
            # In this case, the entire input is passed to the next state.
            return result
        else:
            output_matches = output_path.find(result)
            if output_matches:
                # From docs:
                # If the OutputPath matches an item in the state's input, only that input item is selected.
                # This input item becomes the state's output.
                assert len(output_matches) == 1
                return output_matches[0].value
            else:
                # From docs:
                # If the OutputPath doesn't match an item in the state's input,
                # an exception specifies an invalid path.
                raise NotImplementedError()

    def execute(self, input, resource_resolver: Callable=None) -> Tuple[Optional[str], Any]:
        resource_input = self.format_state_input(input)
        resource_result = resource_resolver(self.resource)(resource_input)
        result = self.format_result(input, resource_result)
        return self.next, self.format_state_output(result)

    def dry_run(self, trace: List):
        trace.append(self.name)
        return self.next


@dataclasses.dataclass
class Pass(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "result": "Result",
        },
    )

    type: str = States.Pass
    result: str = None
    result_path: str = None

    def compile_dict(self, c: Dict):
        if self.next is None:
            c["End"] = True


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

    type: str = States.Task
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

    type: str = States.Choice
    choices: List[ChoiceRule] = dataclasses.field(default_factory=list)
    default: str = None

    @classmethod
    def parse_dict(cls, d: Dict, fields: Dict) -> None:
        fields["choices"] = [ChoiceRule.parse(raw_choice_rule) for raw_choice_rule in d["Choices"]]

    def execute(self, input, resource_resolver: Callable):
        for choice_rule in self.choices:
            if choice_rule.matches(input):
                return choice_rule.next, input
        return self.default, input


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

    type: str = States.Wait
    seconds: int = None
    seconds_path: str = None
    timestamp: str = None
    timestamp_path: str = None

    def compile_dict(self, c: Dict):
        if self.next is None:
            c["End"] = True

    def execute(self, input, resource_resolver: Callable=None):
        # TODO We don't actually do any waiting here, but perhaps we could delegate it to some predefined resource.
        state_input = self.format_state_input(input)
        state_output = self.format_state_output(state_input)
        return self.next, state_output


@dataclasses.dataclass
class Fail(State):
    _FIELDS = bidict(
        **State._FIELDS,
        **{
            "cause": "Cause",
            "error": "Error",
        },
    )

    type: str = States.Fail
    cause: str = None
    error: str = None

    def execute(self, input, resource_resolver: Callable=None):
        # TODO No idea what should we do here.
        return None, None


@dataclasses.dataclass
class Succeed(State):
    type: str = States.Succeed


@dataclasses.dataclass
class Parallel(Task):

    _FIELDS = bidict(
        **Task._FIELDS,
        **{
            "branches": "Branches",
        },
    )

    type: str = States.Parallel
    branches: List["Sequence"] = dataclasses.field(default_factory=list)

    @classmethod
    def parse_list(cls, raw: List, **fields) -> "Parallel":
        assert isinstance(raw, List)
        return cls(
            branches=[Sequence.parse_list(raw_branch) for raw_branch in raw],
            **fields,
        )

    @classmethod
    def parse_dict(cls, d: Dict, fields: Dict) -> None:
        fields["branches"] = [State.parse(raw_branch, type="Sequence") for raw_branch in d["Branches"]]

    def compile_dict(self, c: Dict):
        if self.next is None:
            c["End"] = True

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

    type: str = States.Sequence
    start_at: str = None
    states: Dict[str, State] = dataclasses.field(default_factory=dict)

    @property
    def start_at_state(self) -> State:
        return self.states[self.start_at]

    @classmethod
    def parse_list(cls, raw: List, **fields) -> "State":
        if not isinstance(raw, list):
            raise TypeError(raw)
        if raw and all(isinstance(item, list) for item in raw):
            assert not fields
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
            **fields,
        )

    @classmethod
    def parse_dict(cls, d: Dict, fields: Dict) -> None:
        fields["states"] = {k: State.parse(v, name=k) for k, v in d["States"].items()}

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
    _FIELDS = bidict(
        **Sequence._FIELDS,
        **{
            "version": "Version",
            "timeout_seconds": "TimeoutSeconds",
        },
    )

    type: str = States.Machine
    timeout_seconds: int = None
    version: str = None

    @classmethod
    def parse(cls, raw: Union[List, Dict], **fields) -> "Machine":
        if isinstance(raw, list):
            sequence = super().parse_list(raw, **fields)
            if isinstance(sequence, Parallel):
                return cls(start_at=sequence.name, states={sequence.name: sequence}, **fields)
            assert isinstance(sequence, Machine)
            return sequence
        elif isinstance(raw, dict):
            # Proper state machine definition
            return Sequence.parse(raw, type=States.Machine, **fields)
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
        """
        DEPRECATED.
        This probably will change in near future so don't rely on it.

        A simple tracer for primitive state machines that consist only of sequences and paralellisations.
        Can be used to trace the basic structure.
        Returns a list of state names in the execution order.
        """

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
