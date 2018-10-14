===============
aws-sfn-builder
===============

AWS Step Functions State Machine boilerplate generator.

I was too lazy to set all the ``Next``'s and ``End``'s so I wrote this.

The state machines that I have in mind are pretty simple. They are a combination of sequences and parallelisations.

.. code-block:: python

    raw = [
        # Step can be declared just as a simple string
        "a",

        # Or you can provide all the details you know
        {
            "Name": "b",  # used for the name of the state, but removed from the state body
            "Type": "Pass",
            "Result": "b-result",
        },

        # c1 and c2 can run in parallel with d:
        [
            ["c1", "c2"],
            ["d"],
        ],
    ]

Make this into a State machine definition with ``Machine.parse(raw).to_json()``.

If you need to customise the compiled dictionaries, you can pass ``state_visitor=`` keyword argument
to ``to_json`` (or to ``compile()``). State visitor is a function that takes two positional arguments: the
object representing the state, and the dictionary that we have compiled to represent the state in the
state machine definition as expected by AWS.

Here's a result of ``Machine.parse(raw).to_json()``:

.. code-block:: json

    {
        "StartAt": "a",
        "States": {
            "a": {
                "Type": "Task",
                "Next": "b"
            },
            "b": {
                "Type": "Pass",
                "Next": "f1ba541c-632b-4a7e-94c4-7aaf60dde8cd",
                "Result": "b-result"
            },
            "f1ba541c-632b-4a7e-94c4-7aaf60dde8cd": {
                "Type": "Parallel",
                "End": true,
                "Comment": "f1ba541c-632b-4a7e-94c4-7aaf60dde8cd",
                "Branches": [
                    {
                        "StartAt": "c1",
                        "States": {
                            "c1": {
                                "Type": "Task",
                                "Next": "c2"
                            },
                            "c2": {
                                "Type": "Task",
                                "End": true
                            }
                        }
                    },
                    {
                        "StartAt": "d",
                        "States": {
                            "d": {
                                "Type": "Task",
                                "End": true
                            }
                        }
                    }
                ]
            }
        }
    }
