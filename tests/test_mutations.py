from aws_sfn_builder import Machine


def test_inserts_and_removes_in_a_sequence():
    sequence = Machine.parse(["a", "c", "e"])
    assert sequence.dry_run() == ["a", "c", "e"]

    sequence.insert("b", before="c")
    assert sequence.dry_run() == ["a", "b", "c", "e"]

    sequence.insert("f", after="e")
    assert sequence.dry_run() == ["a", "b", "c", "e", "f"]

    sequence.remove("c")
    assert sequence.dry_run() == ["a", "b", "e", "f"]

    sequence.remove("a")
    assert sequence.dry_run() == ["b", "e", "f"]

    sequence.remove("f")
    assert sequence.dry_run() == ["b", "e"]

    sequence.insert("x", before="b")
    assert sequence.dry_run() == ["x", "b", "e"]

    sequence.remove("b")
    sequence.remove("e")
    sequence.remove("x")

    assert sequence.dry_run() == []
    assert sequence.start_at is None


def test_inserts_and_removes_in_parallels():
    sm = Machine.parse([
        ["b", "c"],
        ["1", "2"],
    ])

    sm.start_at_state.branches[0].insert("a", before="b")
    sm.start_at_state.branches[1].insert("3", after="2")

    assert sm.dry_run() == [
        ["a", "b", "c"],
        ["1", "2", "3"],
    ]


def test_append_to_a_sequence():
    sm = Machine.parse([])
    sm.append("a")
    sm.append("b")
    assert sm.dry_run() == ["a", "b"]
