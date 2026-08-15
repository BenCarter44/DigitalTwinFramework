# Experimental Digital Twin Framework

Currently implemented:
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

Not yet implemented:
- Split
- Join
- Repo cleanup
- - Check async defs if async is needed
- - Nice task cleanup 
- - Commenting
- - Docs
- - Type Annotation 

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

**When running a demo: be sure to start the ZMQ PubSub broker!**

Both sides resolve the broker addresses the same way: `DT_STREAM_PUB_ADDR`
and `DT_STREAM_SUB_ADDR`, defaulting to `tcp://127.0.0.1:5000` and `:5001`
(see `digitaltwin.config`).  Set them in both terminals to move the broker.

**Binding policy**: the broker binds to loopback by default, and it must
stay that way unless you know what you are doing -- pubsub payloads are
cloudpickled, so anyone who can reach the broker ports can execute code in
every subscriber.  A non-loopback bind needs an explicit configuration and
a private/firewalled network.
