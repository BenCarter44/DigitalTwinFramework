"""Deployment configuration for the digital twin framework.

The single place where stream endpoint addresses are decided.  No other
module (and no demo) may contain a hardcoded transport address.

Binding policy: the framework binds to loopback unless the deployment
explicitly configures something else.  The pubsub data plane carries
cloudpickled payloads, so anyone who can reach the broker ports can
execute code in every subscriber -- non-loopback binds require an
explicit configuration and a firewalled/private network.
"""

import os

# loopback-only by default -- see the binding policy above
DEFAULT_BIND_HOST = "127.0.0.1"

# fixed default ports, used by the standalone broker (two-terminal demos)
DEFAULT_PUB_ADDR = f"tcp://{DEFAULT_BIND_HOST}:5000"
DEFAULT_SUB_ADDR = f"tcp://{DEFAULT_BIND_HOST}:5001"

# wildcard port: let the OS pick.  Used by embedded (subprocess) brokers,
# which report their bound addresses back to the parent.
RANDOM_PUB_ADDR = f"tcp://{DEFAULT_BIND_HOST}:*"
RANDOM_SUB_ADDR = f"tcp://{DEFAULT_BIND_HOST}:*"

ENV_PUB_ADDR = "DT_STREAM_PUB_ADDR"
ENV_SUB_ADDR = "DT_STREAM_SUB_ADDR"


def stream_addresses(
    pub_addr: str | None = None, sub_addr: str | None = None
) -> tuple[str, str]:
    """Resolve the (publish, subscribe) addresses of the stream broker.

    Precedence: explicit argument, then the `DT_STREAM_PUB_ADDR` /
    `DT_STREAM_SUB_ADDR` environment variables, then the loopback defaults.
    """

    return (
        pub_addr or os.environ.get(ENV_PUB_ADDR) or DEFAULT_PUB_ADDR,
        sub_addr or os.environ.get(ENV_SUB_ADDR) or DEFAULT_SUB_ADDR,
    )


def embedded_stream_addresses() -> tuple[str, str]:
    """Resolve the bind addresses of a service-embedded stream broker.

    Same environment variables as `stream_addresses`, but an unconfigured
    embedded broker takes a random loopback port instead of the fixed
    demo ports -- it reports what it bound, so nothing has to agree on a
    number up front.
    """

    return (
        os.environ.get(ENV_PUB_ADDR) or RANDOM_PUB_ADDR,
        os.environ.get(ENV_SUB_ADDR) or RANDOM_SUB_ADDR,
    )
