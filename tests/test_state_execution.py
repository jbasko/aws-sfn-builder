import pytest

from aws_sfn_builder import ResourceManager, State


@pytest.fixture
def jsp_resource_resolver():
    resources = ResourceManager()
    return resources.resolve


@pytest.mark.parametrize("input,expected", [
    [
        {"status": "FAILED"},
        ("Job Failed", {"status": "FAILED"}),
    ],
    [
        {"status": "SUCCEEDED"},
        ("Get Final Job Status", {"status": "SUCCEEDED"})
    ],
])
def test_executes_simple_choice(input, expected):
    choice = State.parse({
        "Type": "Choice",
        "Choices": [
            {
                "Variable": "$.status",
                "StringEquals": "FAILED",
                "Next": "Job Failed",
            },
            {
                "Variable": "$.status",
                "StringEquals": "SUCCEEDED",
                "Next": "Get Final Job Status",
            }
        ],
        "Default": "Wait X Seconds",
    })

    returned = choice.execute(input=input, resource_resolver=jsp_resource_resolver)
    assert returned == expected
