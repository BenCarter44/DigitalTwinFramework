# Remote Demo: DT Runtime as a Service

Digital Twin Application:
- sensor counts to 10
- 1 agent
- data sink

Client side:
- `run_me.py` obtains a session (runtime on the server), submits an sensor,
  agent, and sink to the remote service. 

Server side:
- `../remote_service.py` - simply serves as the runtime host. It cares not of the
  actual app (hence outside of this directory). API calls are routed to the runtime
- `../local_broker.py` - This is the pubsub broker backend. It is *not* know
  anything about the runtime at all. 


How this will relate to ORBIT:
- Integration with ORBIT is taking this and packaging it into an ORBIT plugin.
  The equivalent of the `../remote_service.py` and `../local_broker.py` will run
  on the ORBIT broker host as a plugin. 
- Currently, I'm using ZMQ for dummy transport, but this can easily be ported
  over to ORBIT's transport utilities.
- The client will run the equivalent of `run_me.py`  
