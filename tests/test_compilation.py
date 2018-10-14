from aws_sfn_builder import Task


def test_compiles_simple_task():
    task = Task.parse({
        "Type": "Task",
        "Resource": "arn:activity",
        "Next": "NextTask",
    })
    c = task.compile()
    assert sorted(c.keys()) == ["Next", "Resource", "Type"]
