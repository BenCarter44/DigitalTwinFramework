from digitaltwin.streaming import ZMQ_Broker

pub_addr = "tcp://127.0.0.1:5000"
sub_addr = "tcp://127.0.0.1:5001"

zq = ZMQ_Broker(pub_addr, sub_addr)

print(zq.get_connection_str())
zq.run()
