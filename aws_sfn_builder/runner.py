from typing import Any, Callable, Dict, Optional, Tuple

from jsonpath_ng import parse as parse_jsonpath

from .states import Machine, State, States


class ResourceManager:
    """
    Usage:

        resources = ResourceManager()

        @resources.provider("arn.hello-world")
        def hello_world(payload):
            return '"Hello, world!"'

    """

    def __init__(self):
        self._providers = {}

    def resolve(self, resource_arn: str):
        return self._providers[resource_arn]

    def provider(self, resource_arn) -> Callable:
        """
        Decorator to register a resource provider.
        The decorated function should take one positional argument `payload`
        and return output of the resource execution.
        """

        def decorator(func):
            self._providers[resource_arn] = func
            return func

        return decorator


class Runner:
    def __init__(self, resources: ResourceManager=None):
        self._resources: ResourceManager = resources or ResourceManager()

    def resource_provider(self, resource_arn) -> Callable:
        """
        An alternative to ResourceManager.provider of registering a resource provider
        -- through the Runner instance. Handy when you don't have or don't need
        an explicit ResourceManager instance.
        """
        return self._resources.provider(resource_arn)

    def run(self, sm: Machine, input=None) -> Tuple[Optional[State], Any]:
        if input is None:
            input = {}

        next_state = sm.start_at
        while next_state is not None:
            state = sm.states[next_state]
            next_state, input = self.execute_state(sm=sm, state=state, input=input)

        return sm.states.get(next_state, None), input

    def execute_state(self, sm: Machine, state: State, input: Dict) -> Tuple[Optional[str], Any]:
        if state.type == States.Task:
            resource_executor = self._resources.resolve(state.resource)

            resource_input = input
            if state.input_path:
                resource_input = parse_jsonpath(state.input_path).find(input)[0].value

            resource_result = resource_executor(resource_input)
            if state.result_path:
                result_path = parse_jsonpath(state.result_path)
                if not result_path.find(input):
                    # A quick hack to set a non-existent key (assuming the parent of the path is a dictionary).
                    result_path.left.find(input)[0].value[str(result_path.right)] = resource_result
                elif str(result_path) == "$":
                    input = resource_result
                else:
                    result_path.update(input, resource_result)
                resource_output = input
            else:
                resource_output = resource_result

            if state.output_path:
                output_path = parse_jsonpath(state.output_path)
                if str(output_path) == "$":
                    # From docs:
                    # If the OutputPath has the default value of $, this matches the entire input completely.
                    # In this case, the entire input is passed to the next state.
                    pass
                else:
                    output_matches = output_path.find(resource_output)
                    if output_matches:
                        # From docs:
                        # If the OutputPath matches an item in the state's input, only that input item is selected.
                        # This input item becomes the state's output.
                        assert len(output_matches) == 1
                        resource_output = output_matches[0].value
                    else:
                        # From docs:
                        # If the OutputPath doesn't match an item in the state's input,
                        # an exception specifies an invalid path.
                        raise NotImplementedError()

            return state.next, resource_output

        return None, input
