from aws_sfn_builder.choice_rules import Operator


def test_flat_numeric_equals_operator():
    source = {
        "Variable": "$.value",
        "NumericEquals": 0,
        "Next": "ValueIsZero",
    }

    op = Operator.parse(source)
    assert op.type == "Operator"
    assert op.name == "NumericEquals"
    assert op.value == 0
    assert op.variable == "$.value"
    assert op.next == "ValueIsZero"

    assert op.compile() == source

    assert op.matches({"value": 0})
    assert not op.matches({"value": 1})


def test_numeric_equals_operator_without_next():
    source = {
        "Variable": "$.value",
        "NumericEquals": 0,
    }

    op = Operator.parse(source)
    assert op.type == "Operator"
    assert op.name == "NumericEquals"
    assert op.value == 0
    assert op.variable == "$.value"
    assert op.next is None

    assert op.compile() == source


def test_nested_operator():
    source = {
        "And": [
            {
                "Variable": "$.value",
                "NumericGreaterThanEquals": 20
            },
            {
                "Variable": "$.value",
                "NumericLessThan": 30
            },
        ],
        "Next": "ValueInTwenties",
    }

    op = Operator.parse(source)

    assert op.type == "Operator"
    assert op.name == "And"
    assert op.next == "ValueInTwenties"

    assert isinstance(op.value, list)
    assert isinstance(op.value[0], Operator)
    assert op.value[0].variable == "$.value"

    assert op.compile() == source

    assert op.matches({"value": 20})
    assert op.matches({"value": 29})
    assert not op.matches({"value": 30})


def test_nested_ands_and_ors_and_nots():
    source = {
        "And": [
            {
                "Variable": "$.value",
                "NumericGreaterThan": 20
            },
            {
                "Variable": "$.value",
                "NumericLessThan": 30
            },
            {
                "Or": [
                    {
                        "Variable": "$.value",
                        "NumericGreaterThan": 26
                    },
                    {
                        "Variable": "$.value",
                        "NumericLessThan": 23
                    },
                ]
            }
        ],
    }

    op = Operator.parse(source)
    assert not op.matches({"value": 15})
    assert not op.matches({"value": 20})
    assert op.matches({"value": 21})
    assert op.matches({"value": 22})
    assert not op.matches({"value": 23})
    assert not op.matches({"value": 24})
    assert not op.matches({"value": 25})
    assert not op.matches({"value": 26})
    assert op.matches({"value": 27})
    assert op.matches({"value": 28})
    assert op.matches({"value": 29})
    assert not op.matches({"value": 30})
