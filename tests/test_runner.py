import pytest

from aws_sfn_builder import Machine, Runner


def test_executes_hello_world(example):
    hello_world_source = example("hello_world")

    sm = Machine.parse(hello_world_source)

    runner = Runner()

    @runner.resource_provider("arn:aws:lambda:us-east-1:123456789012:function:HelloWorld")
    def hello_world(payload):
        return '"Hello, world!"'

    assert runner.run(sm) == (None, '"Hello, world!"')


@pytest.mark.parametrize("result_path,expected_output", [
    ["$.status", {"guid": "guid-value", "status": 'job is okay'}],
    ["$", "job is okay"],
    [None, "job is okay"],  # TODO This is not according to the spec.
])
def test_basic_result_path(result_path, expected_output):
    sm = Machine.parse([
        {
            "Type": "Task",
            "Resource": "check_job",
            "InputPath": "$.guid",
            "ResultPath": result_path,
        },
    ])

    runner = Runner()

    @runner.resource_provider("check_job")
    def check_job(payload):
        assert payload == "guid-value"
        return 'job is okay'

    state, output = runner.run(sm, input={"guid": "guid-value"})
    assert state is None
    assert output == expected_output


@pytest.mark.parametrize("output_path,expected_output", [
    ["$.guid", "guid-value"],
    ["$", {"guid": "guid-value", "status": 'job is okay'}],
    [None, {"guid": "guid-value", "status": 'job is okay'}],  # TODO Again, this is non-standard, needs fixing.
])
def test_basic_output_path(output_path, expected_output):
    sm = Machine.parse([
        {
            "Type": "Task",
            "Resource": "check_job",
            "InputPath": "$.guid",
            "ResultPath": "$.status",
            "OutputPath": output_path,
        },
    ])

    runner = Runner()

    @runner.resource_provider("check_job")
    def check_job(payload):
        assert payload == "guid-value"
        return 'job is okay'

    state, output = runner.run(sm, input={"guid": "guid-value"})
    assert state is None
    assert output == expected_output


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


def test_runs_job_status_poller(example):
    source = example("job_status_poller")
    sm = Machine.parse(source)
    assert sm.compile() == source

    runner = Runner()

    @runner.resource_provider("arn:aws:lambda:REGION:ACCOUNT_ID:function:SubmitJob")
    def submit_job(payload):
        pass

    state, output = runner.run(sm)
