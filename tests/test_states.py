from aws_sfn_builder import Machine, Parallel


def test_simple_sequence():
    s = Machine.parse(["a", "b"])
    assert len(s.states) == 2
    assert s.start_at.name == "a"
    assert s.dry_run() == ["a", "b"]


def test_simple_parallel():
    source = [["a"], ["b"]]
    s = Machine.parse(source)
    assert len(s.states) == 1
    assert isinstance(s.start_at, Parallel)
    assert s.dry_run() == source


def test_parallel_inside_sequence():
    source = [
        "a",
        [
            ["b11", "b12"],
            ["b21", "b22"],
        ],
        "c",
    ]
    s = Machine.parse(source)
    assert len(s.states) == 3
    assert s.start_at.name == "a"
    assert s.dry_run() == source


def test_parallel_inside_parallel():
    source = [
        [
            "a",
        ],
        [
            [
                [
                    "b11",
                ],
                [
                    "b21",
                ]
            ],
            "b3",
        ]
    ]
    s = Machine.parse(source)
    assert s.dry_run() == source
