from aws_sfn_builder import Machine, Pass, State


def test_decompiles_simple_pass_state():
    raw_state = {
        "Type": "Pass",
        "Result": "Hello, world!",
        "End": True,
    }
    state = State.parse(raw_state)
    assert isinstance(state, Pass)
    assert state.next is None
    assert state.end is True
    assert state.result == "Hello, world!"


def test_decompiles_simple_task_state():
    raw_state = {
        "Type": "Task",
        "Resource": "arn:activity",
        "Next": "NextTask",
    }
    state = State.parse(raw_state, name="ThisTask")
    assert state.name == "ThisTask"
    assert state.next == "NextTask"
    assert state.type == "Task"


def test_decompiles_simple_sequence():
    sequence = Machine.parse(["a", "b", "c"]).compile()
    machine = Machine.parse(sequence)
    assert len(machine.states) == 3
    assert machine.start_at == "a"
