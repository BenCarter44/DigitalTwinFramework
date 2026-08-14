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



## Running tests:

1. `pip install -e .`
2. `cd tests/`
3. In one terminal, run `local_broker.py`
4. In second terminal, cd into the test and run `run_me.py`

**When running tests: be sure to start the ZMQ PubSub broker!**
