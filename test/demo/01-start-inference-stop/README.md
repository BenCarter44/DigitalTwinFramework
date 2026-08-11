# Demo - The basics: 1 sensor, 1 physics item, inference only

**Panels:   SENSOR > INFERENCE TASK > DATA SINK**

Sensor: counts to 30   ----> inference returns 100 - sensor ----> sink displays output 

--- 

## Architectural view of demo:

Demonstrates the system architecture with a single physics component and a
single implementation (surrogate model). 

1. Events
2. Streams
3. In-Situ Prediction
4. Output streams


## Technical view of demo:
This sets up one sensor task, one model investigator, and one sink. This gives
a basic example of the object-oriented approach to modularize each component. 


### Tail the following:
- `sensor.out`
- `model-inference.out`
- `sink.out`

