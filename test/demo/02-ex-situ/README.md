# Demo - Ex-Situ

**Panels:**
```
Ex Situ:          Learner   
                  |     V
In Situ:  SENSOR ==> INFERENCE ==> SINK
```

- The sensor counts to 30. 
- The model learner publishes a new model every time it receives a number
  divisible by 10 from the sensor (new model: increments inference by 100). 
- Sink displays output.  

## Architectural view of demo:

Demonstrates the system architecture with a single physics component and a
single implementation (surrogate model). 

1. Events
2. Streams
3. In-Situ Prediction
4. Output streams
5. Ex-Situ Model creation / redeployment


## Technical view of demo:
This sets up one sensor task, one model investigator, and one sink. This gives
a basic example of the object-oriented approach to modularize each component. 


### Tail the following:
- `sensor.out`
- `model-inference.out`
- `model-learner.out`
- `sink.out`

