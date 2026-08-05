from digitaltwin.components import DataType

SENSOR_DTYPE = DataType("sensor")
INFERENCE_DTYPE = DataType("inference-from-mymodel")

ZMQ_PS_BROKER_PUB = "tcp://127.0.0.1:5000"
ZMQ_PS_BROKER_SUB = "tcp://127.0.0.1:5001"
