from typing import Dict

import dataclasses

from aws_sfn_builder import Machine, State


def test_dicts_as_states():
    m = Machine.parse([
        {
            "Comment": "first",
            "Type": "Task",
            "Resource": "arn:first",
        },
        {
            "Name": "second",
            "Type": "Pass",
            "Resource": "arn:second",
        },
        "third",
    ])

    c = m.compile()
    assert c["StartAt"] == "arn:first"
    assert len(c["States"]) == 3
    assert c["States"]["arn:first"]["Comment"] == "first"
    assert c["States"]["arn:first"]["Resource"] == "arn:first"
    assert "Name" not in c["States"]["second"]
    assert c["States"]["second"]["Type"] == "Pass"


def test_custom_task():
    @dataclasses.dataclass
    class Task:
        name: str

        def __str__(self):
            return self.name

        def get_state_attrs(self, state):
            return {
                "Resource": f"arn:{self.name}"
            }

    m = Machine.parse([
        Task("first"),
        Task("second"),
    ])
    c = m.compile()
    assert c["States"]["first"]["Type"] == "Task"
    assert c["States"]["first"]["Resource"] == "arn:first"
    assert c["States"]["second"]["Resource"] == "arn:second"


def test_visitor_visits_every_compiled_state_dictionary():
    m = Machine.parse([
        ["a", "b", "c"],
        ["1", "2"],
    ])

    def state_visitor(state: State, compiled_state: Dict):
        compiled_state["Resource"] = f"arn.funny.{state.name}"

    c = m.compile(state_visitor=state_visitor)
    assert c["States"][c["StartAt"]]["Branches"][0]["States"]["a"]["Resource"] == "arn.funny.a"
