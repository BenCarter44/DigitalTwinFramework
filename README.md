# Experimental Digital Twin Framework

Main set of features implemented:
- Model Investigator
- Utility Tasks
- Persistent Tasks
- Callbacks
- Simple ZMQ pubsub backend
- Graph builder
- Convert to a Python Package
- Several Tests / Examples
- Science Agents
- Request inference API on runtime
- Barrier
- Split
- Join
- Shared SIM / subtasks running on agent, accessible by all investigators
- Barrier working on remote
- Split working on remote
- Join working on remote

## Running the unit tests:

1. `pip install .[test]`
2. `pytest` (or `tox` for all supported interpreters)

The unit tests start their own stream broker on a random port; no setup.

## Running the demos:

1. `pip install .`
2. `cd test/`
3. In one terminal, run `local_broker.py` -- it prints the addresses it
   bound
4. In a second terminal, cd into the demo and run `run_me.py`
5. In a third terminal, in the same demo, run its `sensor.py` if it has
   one (`01-start-inference-stop` and `04-start-agent-stop` do)

**When running a demo: be sure to start the ZMQ PubSub broker!**

The third terminal is the point, not an inconvenience: a sensor is an
external entity.  It is a process of its own with a lifetime of its own,
it publishes JSON on a shared channel, and it knows nothing about twins.
The twin binds that channel with `runtime.add_input(dtype, channel)`, and
a second twin binding the same channel receives the same messages -- which
is how one instrument feeds many twins.  Start and stop the sensor
independently of the twin; neither cares.

Demos without a `sensor.py` produce their input inside the twin, which is
what persistent components are still for: `06-agent-pi` drives itself off
a timer, and `07-barrier` off several.

Every side resolves the broker addresses the same way: `DT_STREAM_PUB_ADDR`
and `DT_STREAM_SUB_ADDR`, defaulting to `tcp://127.0.0.1:5000` and `:5001`
(see `digitaltwin.config`).  Set them in every terminal to move the broker.

**Binding policy**: the broker binds to loopback by default, and it must
stay that way unless you know what you are doing -- twin-internal payloads
are cloudpickled, so anyone who can reach the broker ports can execute code
in every subscriber.  A non-loopback bind needs an explicit configuration
and a private/firewalled network.  External channels are decoded with the
codec their binding names: `json` (the default) and `raw` are safe to
accept from a producer you do not control, `cloudpickle` is not.
