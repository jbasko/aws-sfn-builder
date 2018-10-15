import pytest

from aws_sfn_builder import Machine, ResourceManager, Runner, State


@pytest.mark.parametrize("input_path,expected_resource_input", [
    [None, {"guid": "123-456"}],
    ["$", {"guid": "123-456"}],
    ["$.guid", "123-456"],
])
def test_format_resource_input_returns_filtered_input(input_path, expected_resource_input):
    state = State.parse({
        "InputPath": input_path
    })
    resource_input = state.format_state_input({"guid": "123-456"})
    assert expected_resource_input == resource_input


@pytest.mark.parametrize("result_path,expected_result", [
    [None, "ok"],
    ["$", "ok"],
    ["$.status", {"guid": "123-456", "status": "ok"}]
])
def test_format_result_returns_applied_result(result_path, expected_result):
    state = State.parse({
        "ResultPath": result_path,
    })
    result = state.format_result({"guid": "123-456"}, "ok")
    assert expected_result == result


@pytest.mark.parametrize("output_path,expected_state_output", [
    [None, {"guid": "123-456"}],
    ["$", {"guid": "123-456"}],
    ["$.guid", "123-456"],
])
def test_format_state_output_returns_filtered_output(output_path, expected_state_output):
    state = State.parse({
        "OutputPath": output_path
    })
    state_output = state.format_state_output({"guid": "123-456"})
    assert expected_state_output == state_output


def test_executes_hello_world_state(example):
    hello_world_state = Machine.parse(example("hello_world")).start_at_state
    assert isinstance(hello_world_state, State)

    resources = ResourceManager(providers={
        "arn:aws:lambda:us-east-1:123456789012:function:HelloWorld": lambda x: "Hello, world!"
    })
    next_state, output = hello_world_state.execute({}, resource_resolver=resources)
    assert output == "Hello, world!"


def test_runs_hello_world_machine(example):
    sm = Machine.parse(example("hello_world"))

    runner = Runner(resources=ResourceManager(providers={
        "arn:aws:lambda:us-east-1:123456789012:function:HelloWorld": lambda x: "Hello, world!"
    }))

    assert runner.run(sm) == (sm.start_at_state, "Hello, world!")


def test_input_passed_to_next_task():
    sm = Machine.parse([
        {
            "InputPath": "$.first_input",
            "ResultPath": "$.first_output",
            "Resource": "MultiplierByTwo",
        },
        {
            "InputPath": "$.first_output",
            "ResultPath": "$.second_output",
            "Resource": "MultiplierByThree",
        },
        {
            "Resource": "Validator",
        },
    ])

    runner = Runner()
    runner.resource_provider("MultiplierByTwo")(lambda x: x * 2)
    runner.resource_provider("MultiplierByThree")(lambda x: x * 3)

    @runner.resource_provider("Validator")
    def validate_input(input):
        assert input == {
            "first_input": 1111,
            "first_output": 2222,
            "second_output": 6666,
        }
        # NB!
        return input

    final_state, output = runner.run(sm, input={"first_input": 1111})
    assert output == {
        "first_input": 1111,
        "first_output": 2222,
        "second_output": 6666,
    }


@pytest.mark.parametrize("input,expected_output", [
    [{}, {}],
    [{"x": 1}, {"x": 1}],
])
def test_executes_wait_state(input, expected_output):
    wait = State.parse({
        "Type": "Wait",
        "Seconds": 10,
        "Next": "NextState",
    })
    next_state, output = wait.execute(input=input)
    assert next_state == "NextState"
    assert expected_output == output


def test_executes_fail_state():
    fail = State.parse({
        "Type": "Fail",
        "Error": "ErrorA",
        "Cause": "Kaiju attack",
    })
    # TODO No idea what should be the next state or output of fail state.
    # TODO Should it just raise an exception?
    next_state, output = fail.execute(input=input)
    assert next_state is None
