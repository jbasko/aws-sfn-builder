from wr_profiles import envvar_profile_cls


@envvar_profile_cls
class TestsProfile:
    profile_root = "aws_sfn_builder_test"

    names_prefix: str = "aws-sfn-builder-test-"
    role_arn: str = None


tests_profile = TestsProfile()
