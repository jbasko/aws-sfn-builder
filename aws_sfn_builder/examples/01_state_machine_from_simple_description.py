from aws_sfn_builder import Machine

machine = Machine.parse([
    "a",
    [
        ["b-10", "b-11"],
        ["b-20"],
    ],
    "c",
])

print(machine.to_json())
