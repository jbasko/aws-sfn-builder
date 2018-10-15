import random

from aws_sfn_builder import Machine, Runner


def test_runs_job_status_poller(example):
    source = example("job_status_poller")
    sm = Machine.parse(source)
    assert sm.compile() == source

    runner = Runner()

    @runner.resource_provider("arn:aws:lambda:REGION:ACCOUNT_ID:function:SubmitJob")
    def submit_job(payload):
        return payload

    @runner.resource_provider("arn:aws:lambda:REGION:ACCOUNT_ID:function:CheckJob")
    def check_job(payload):
        if random.random() < 0.2:
            return "FAILED"
        else:
            return "SUCCEEDED"

    final_state, output = runner.run(sm)
    assert final_state.name in ("Get Final Job Status", "Job Failed")
