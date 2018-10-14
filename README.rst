===============
aws-sfn-builder
===============

AWS Step Functions State Machine builder.

The primary motivation for this was the amount of boilerplate (``Next``, ``End``) required to compose a valid
state machine definition, but soon one got carried away.

Python 3.6+ only.

Installation
------------

.. code-block:: shell

    pip install aws-sfn-builder


Generate State Machine Definition from Simple Description
---------------------------------------------------------

.. code-block:: python

    from aws_sfn_builder import Machine

    Machine.parse([
        "a",
        [
            ["b-10", "b-11"],
            ["b-20"],
        ],
        "c",
    ]).to_json()

This will generate:

.. code-block:: json

    {
        "StartAt": "a",
        "States": {
            "a": {
                "Type": "Task",
                "Next": "d3d52323-137d-4228-9956-d3b77cc43a92"
            },
            "d3d52323-137d-4228-9956-d3b77cc43a92": {
                "Type": "Parallel",
                "Next": "c",
                "Branches": [
                    {
                        "StartAt": "b-10",
                        "States": {
                            "b-10": {
                                "Type": "Task",
                                "Next": "b-11"
                            },
                            "b-11": {
                                "Type": "Task",
                                "End": true
                            }
                        }
                    },
                    {
                        "StartAt": "b-20",
                        "States": {
                            "b-20": {
                                "Type": "Task",
                                "End": true
                            }
                        }
                    }
                ]
            },
            "c": {
                "Type": "Task",
                "End": true
            }
        }
    }

Parse Existing State Machine Definition
---------------------------------------

.. code-block:: python

    # TODO load state_machine_json_dict

    state_machine = Machine.parse(state_machine_json_dict)


Compile State Machine
---------------------

.. code-block:: python

    # TODO initialise state_machine

    state_machine.compile()


Test Your State Machine
-----------------------

*Work in progress.*

.. code-block:: python

    # TODO initialise state_machine

    runner = Runner()

    @runner.resource_provider("arn:aws:lambda:us-east-1:123456789012:function:Foo")
    def foo(input):
        return "foo-result"

    final_state, output = runner.run(state_machine)
