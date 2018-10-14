"""
Examples are from:
- https://states-language.net/spec.html
- https://docs.aws.amazon.com/step-functions/latest/dg/concepts-amazon-states-language.html
"""
import json

import pytest

from aws_sfn_builder import Fail, Machine, Parallel, Pass, Sequence, State, States, Succeed, Task, Wait


def test_parse_of_state_is_the_state_itself():
    x1 = State()
    assert State.parse(x1) is x1

    x2 = State.parse(x1, comment="x2")
    assert x2 is not x1
    assert x2.comment == "x2"
    assert x1.comment is None

    assert State.parse(x2) is x2


def test_pass_state():
    source = {
        "Type": "Pass",
        "Result": {
            "x-datum": 0.381018,
            "y-datum": 622.2269926397355
        },
        "ResultPath": "$.coords",
        "Next": "End",
    }

    state = State.parse(source)

    assert isinstance(state, Pass)
    assert state.result_path == "$.coords"

    assert state.compile() == source


def test_task_state():
    source = {
        "Comment": "Task State example",
        "Type": "Task",
        "Resource": "arn:aws:swf:us-east-1:123456789012:task:HelloWorld",
        "Next": "NextState",
        "TimeoutSeconds": 300,
        "HeartbeatSeconds": 60
    }

    state = State.parse(source)

    assert isinstance(state, Task)

    assert state.compile() == source


def test_task_state_compiled_always_has_next_or_end():
    task = State.parse({
        "Type": "Task",
    }).compile()

    assert task.get("Next") or task["End"] is True


def test_task_uses_resource_as_default_name_ahead_of_comment():
    source = {
        "Type": "Task",
        "Resource": "arn:something",
        "Comment": "this is doing something",
    }

    state = State.parse(source)
    assert state.name == "arn:something"


def test_choice_state(example):
    source = example("choice_state_x")["States"]["ChoiceStateX"]

    state = State.parse(source)

    assert state.compile() == source


@pytest.mark.parametrize("extras", [
    {"Seconds": 10},
    {"SecondsPath": "$.seconds"},
    {"Timestamp": "2016-03-14T01:59:00Z"},
    {"TimestampPath": "$.expirydate"},
])
def test_wait_state(extras):
    source = {
        "Type": "Wait",
        "Next": "NextState",
        **extras,
    }

    state = State.parse(source)
    assert isinstance(state, Wait)
    assert state.type == "Wait"
    assert state.next == "NextState"

    assert state.compile() == source


def test_suceed_state():
    source = {
        "Type": "Succeed",
    }

    state = State.parse(source)
    assert isinstance(state, Succeed)
    assert state.type == "Succeed"

    assert state.compile() == source


def test_fail_state():
    source = {
        "Type": "Fail",
        "Error": "ErrorA",
        "Cause": "Kaiju attack",
    }

    state = State.parse(source)
    assert isinstance(state, Fail)

    assert state.compile() == source


def test_task_state_with_retry():
    source = {
        "Type": "Task",
        "Resource": "arn:aws:swf:us-east-1:123456789012:task:X",
        "Next": "Y",
        "Retry": [
            {
                "ErrorEquals": ["ErrorA", "ErrorB"],
                "IntervalSeconds": 1,
                "BackoffRate": 2,
                "MaxAttempts": 2
            },
            {
                "ErrorEquals": ["ErrorC"],
                "IntervalSeconds": 5
            }
        ],
        "Catch": [
            {
                "ErrorEquals": ["States.ALL"],
                "Next": "Z",
            }
        ]
    }

    state = State.parse(source)

    assert isinstance(state, Task)
    assert len(state.retry) == 2
    assert len(state.catch) == 1

    assert state.compile() == source


def test_parallel_state():
    source = {
        "Type": "Parallel",
        "Branches": [
            {
                "StartAt": "LookupAddress",
                "States": {
                    "LookupAddress": {
                        "Type": "Task",
                        "Resource": "arn:aws:lambda:us-east-1:123456789012:function:AddressFinder",
                        "End": True,
                    },
                },
            },
            {
                "StartAt": "LookupPhone",
                "States": {
                    "LookupPhone": {
                        "Type": "Task",
                        "Resource": "arn:aws:lambda:us-east-1:123456789012:function:PhoneFinder",
                        "End": True,
                    },
                },
            },
        ],
        "Next": "NextState",
    }

    state = State.parse(source)

    assert isinstance(state, Parallel)
    assert isinstance(state.branches[0], Sequence)
    assert isinstance(state.branches[0].start_at_state, Task)

    assert json.dumps(state.compile(), sort_keys=True) == json.dumps(source, sort_keys=True)


def test_machine_state(example):
    source = example("hello_world")

    machine = Machine.parse(source)

    assert machine.type == States.Machine
    assert isinstance(machine.start_at_state, Task)

    assert machine.compile() == source
