# Digital Twin Runtime – Remote-as-a-Service (DT-aaS)

This directory contains a minimal implementation that turns a local **Digital Twin Runtime** into a remote procedure-call service.

## What is this?

The core `DigitalTwin` package lives in `src/digitaltwin` and exposes the `DTRuntime` object, which you normally instantiate and use **directly**.  With the code in this directory you can:

* **Expose an instance of `DTRuntime` over the network** using ZeroMQ.
* **Interact with the runtime from the client** via the lightweight `RemoteDTRuntime` class.
* **Serialize user code (Agents, Investigators, Tasks)** with `cloudpickle` to
  pass them to the service.

## Architecture Overview

```
+-------------------+           +-------------------+
|  Client (Python)  |  <==>      |  Remote Service   |
|  RemoteDTRuntime  |            |  RemoteDTService  |
+-------------------+           +----------+--------+
                                              |
                                              v
                                 +-----------------------+
                                 |  DTRuntime (local)   |
                                 +-----------------------+
```

* **`RemoteDTService`** – a ZeroMQ `REP` socket that listens on provided address.  It receives requests encoded as a tiny JSON frame that looks like:

```json
{
    "method": "add_task", 
    "args": ["<b64-pickled-obj>", "<b64-pickled-obj>"],
    "kwargs": {"key":"<b64-pickled-obj"}
}
```

The service unpickles the arguments, invokes the corresponding method on the wrapped `DTRuntime`, and returns a pickled result.

* **`RemoteDTRuntime`** – a synchronous client that communicates with the
  service over a ZeroMQ `REQ` socket.  It serializes your arguments with
  `cloudpickle`, base64-encodes them, and mirrors the remote API (`start`,
  `add_task`, `add_investigator`, `add_agent`, etc....).

* **`RemoteDTOrchestrator`** – a tiny helper that can create new
  `RemoteDTRuntime` instances, should you want to launch multiple runtimes (sessions).

## Getting Started

1. **Install the project** (including the new `cloudpickle` dependency):

   ```bash
   pip install .
   ```

2. **Launch the remote service** from your application or a terminal:

   ```bash
   cd tests/
   python3 remote_service
   ```

   The service will bind to `tcp://127.0.0.1:5555`.

3. **Launch the PubSub broker**:

   ```bash
   cd tests/
   python3 local_broker.py
   ```

3. **Remotely interact with runtime** from another process:

   ```python
   from digitaltwin.remote.client import RemoteDTRuntime

   runtime = RemoteDTRuntime("tcp://localhost:5555")
   # ... add tasks, reporters, etc.
   runtime.start()
   ```

## Testing & Examples

The test suite demonstrates both the server and client in action:

* `test/09-remote/test_remote_service_client.py` – shows a client creating a
  `RemoteDTRuntime`, invoking methods, and displaying the results
* `test/remote_service.py` – runs a basic service using the `RemoteDTService` class.


## Dependencies

* `pyzmq` – ZeroMQ binding for Python.
* `cloudpickle` – serialises arbitrary Python objects across the wire.


---

The remote interface will eventually be ported over as an ORBIT plugin.