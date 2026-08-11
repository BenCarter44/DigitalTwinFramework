# Digital Twin as a Service

> **TL;DR** – The following document outlines a phased plan to expose ``DTRuntime`` as a remote procedure‑call service.  The plan covers architecture, serialization (cloudpickle), RPC transport, discovery, security, and incremental testing.  It is intended to be inserted into ``src/digitaltwin/remote/README.md`` and used by developers as a living design record.

## 1. High‑Level Goal

*Expose the ``DTRuntime`` instance as a service that can be
* • spawned on a remote host (Linux, WSL, Docker, etc.)
* • controlled via a lightweight, language‑agnostic RPC layer.

The service will handle only the minimal lifecycle of the runtime: start, stop, add, and remove tasks.  Users (Python scripts, front‑ends, CI pipelines) will communicate with it through a well‑defined protocol.

## 2. Core Requirements

| # | Requirement | Notes |
|---|-------------|-------|
| 1 | **Serialization** | Use ``cloudpickle`` to serialize all objects that cross the process boundary: the runtime, agent objects, and their associated functions or callbacks.
| 2 | **Transport** | Use ZeroMQ (`pyzmq`) for a simple, duplex‑socket wire.  The design is transport‑agnostic so future swaps (e.g., gRPC) can be made with minimal code changes.
| 3 | **RPC Protocol** | Design a minimal JSON‑serialisable request/response schema:
| | * ``method`` – e.g. ``add_task``
| | * ``params`` – dict of cloudpickle‑pickled bytes, base64‑encoded
| | * ``id`` – correlation ID
| 4 | **Service Lifecycle** | Launch the service simply with ``python remote_service.py`` – no Docker or systemd needed for the proof‑of‑concept. |
| 5 | **Security** | Minimal: optional token header; advanced auth (TLS, mutual‑TLS) can be added later if required.
| 6 | **Monitoring & Logging** | Not required for the initial proof‑of‑concept.
| 7 | **Versioning** | Tag the protocol (e.g., ``1.0``) and bump when the RPC surface changes.

## 3. Proposed Architecture

```
+------------------+      +------------------+
|  Client (Python  |<---->|  Remote Service   |
|  API)            |      |  (ZMQ)           |
+------------------+      +------------------+
          |
          v
+------------------------------+
|  DTRuntime (remote)         |
|  - Holds task registry
|  - Runs async event loop
|  - Exposes start/stop/add/remove
+------------------------------+
```

### 3.1 Remote Service Layer

* Thin wrapper around the real ``DTRuntime``.
* Exposes the minimal RPC surface: ``start()``, ``stop()``, ``add_task()``, ``remove_task()``.
* Serialises inputs/outputs with **cloudpickle** → base64 JSON.
* Dispatches requests to runtime methods in a thread‑safe way using ``asyncio.run_coroutine_threadsafe``.

### 3.2 Client SDK

* Small Python package (``digitaltwin.remote.client``).
* Provides a ``RemoteDTRuntime`` proxy class that implements the same RPC methods locally.
* Handles serialization/deserialization automatically.

## 4. Implementation Roadmap

| Phase | Tasks | Deliverables | Effort |
|-------|-------|--------------|-------|
| 0 | Add ``cloudpickle`` to ``requirements.txt`` | Updated dependencies | 1 h |
| 1 | Local stub (no networking) | ``src/digitaltwin/remote/local_stub.py`` | 2 h |
| 2 | ZeroMQ transport (request/response loop) | ``src/digitaltwin/remote/zmq_service.py`` | 4 h |
| 3 | Runtime wrapper exposing RPC surface | ``src/digitaltwin/remote/service.py`` + ``remote_service.py`` | 6 h |
| 4 | Simple token auth (optional) | Updated service | 2 h |
| 5 | Client SDK (``RemoteDTRuntime``) | ``src/digitaltwin/remote/client.py`` | 4 h |
| 6 | Unit & integration tests | ``tests/test_remote.py`` | 4 h |
| 7 | Update this README | `README.md` | 2 h |

Total ≈ 25 h.

## 5. Sample Code

### 5.1 Client Usage

```python
from digitaltwin.remote.client import RemoteDTRuntime
from digitaltwin.components import Agent

runtime = RemoteDTRuntime("tcp://127.0.0.1:5555")
agent = Agent(name="demo")
runtime.add_task("demo_task", agent.main_loop, inputs=[...])
runtime.start()
```

### 5.2 Service Entry Point

```python
#!/usr/bin/env python
from digitaltwin.runtime import DTRuntime
from digitaltwin.remote.service import RemoteService

if __name__ == "__main__":
    rt = DTRuntime()
    srv = RemoteService(rt, bind="tcp://*:5555")
    srv.run()
```

---

*This file is a living design document and should be revised as implementation details evolve.*

## Implementation Notes

* **Base64 for the pickled payloads** – The RPC protocol encodes all cloudpickle‑serialized objects inside a JSON frame.  ZeroMQ can transport arbitrary bytes, but using a text‑safe base64 representation guarantees the payload remains a valid JSON string, which simplifies debugging, inspection, and potential cross‑language consumption.  The base64 overhead is negligible relative to the size of the pickled data in this context.
