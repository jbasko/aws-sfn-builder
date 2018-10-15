import collections
import time
from typing import Any, Callable, Optional, Tuple

from .states import Machine, State


class ResourceManager:
    """
    Usage:

        resources = ResourceManager()

        @resources.provider("arn.hello-world")
        def hello_world(payload):
            return '"Hello, world!"'

    """

    def __init__(self, providers=None):
        self._providers = {}

        if providers:
            self._providers.update(providers)

    def resolve(self, resource_arn: str):
        try:
            return self._providers[resource_arn]
        except KeyError:
            raise RuntimeError(f"Failed to resolve resource {resource_arn!r} -- no provider registered")

    def __call__(self, resource_arn: str):
        return self.resolve(resource_arn)

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

    def run(self, sm: Machine, input=None, _timeout=2) -> Tuple[Optional[State], Any]:
        if input is None:
            input = {}

        start_time = time.time()
        state = None
        next_state = sm.start_at

        last_10_states = collections.deque()

        while next_state is not None:
            state = sm.states[next_state]
            last_10_states.append(next_state)
            while len(last_10_states) > 10:
                last_10_states.popleft()
            try:
                next_state, input = state.execute(input=input, resource_resolver=self._resources)
            except Exception as e:
                raise RuntimeError(
                    f"State {state.name} ({state.type}) execution failed with an exception: {e!r}"
                )
            if time.time() - start_time > _timeout:
                raise RuntimeError(
                    f"State machine {(sm.comment or sm.name)!r} failed to terminate in {_timeout} seconds. "
                    f"Last {len(last_10_states)} states: {last_10_states}.",
                )

        # Return the final state
        return state, input
