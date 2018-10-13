"""
TODO This is incomplete:
1. Activities should already exist
2. Cannot run more than once.
"""

import os
from typing import Dict

import autoboto.services.stepfunctions as stepfunctions
import pytest
from autoboto.services.stepfunctions.shapes import ActivityListItem

from aws_sfn_builder import Machine, State
from integration_tests.conftest import tests_profile


class TActivity:
    def __init__(self):
        self.name = None
        self.arn = None

    def __set_name__(self, owner, name):
        self.name = name
        self.full_name = f"{tests_profile.names_prefix}{self.name}"

    def __get__(self, instance, owner):
        return self

    def __str__(self):
        return self.full_name


class TActivities:
    a = TActivity()
    b = TActivity()
    c = TActivity()
    d = TActivity()
    e = TActivity()
    f = TActivity()
    g = TActivity()
    h = TActivity()

    all = [
        a, b, c, d, e, d, f, g, h,
    ]


@pytest.fixture(autouse=True)
def ensure_aws_profile_is_set():
    if os.environ.get("AWS_PROFILE") != "aws-sfn-builder-test":
        raise RuntimeError("You must create an AWS profile called aws-sfn-builder-test and activate it")


@pytest.fixture(scope="session")
def sfn_client() -> stepfunctions.Client:
    return stepfunctions.Client()


def test_create_state_machine(sfn_client):
    sm = Machine.parse([
        TActivities.a,
        TActivities.b,
        [
            [TActivities.d, TActivities.e],
            [TActivities.f],
        ],
        [
            [TActivities.g],
            [TActivities.h],
        ],
    ])

    activities: Dict[str, ActivityListItem] = {a.name: a for a in sfn_client.list_activities().activities}

    def state_visitor(state: State, compiled_state: Dict):
        if compiled_state.get("Type") == "Task" and "Resource" not in compiled_state:
            compiled_state["Resource"] = activities[state.name].activity_arn

    sm_definition = sm.to_json(state_visitor=state_visitor)

    sfn_client.create_state_machine(
        name=f"{tests_profile.names_prefix}-something-statemachine",
        definition=sm_definition,
        role_arn=tests_profile.role_arn,
    )
