"""M0.4 -- embedded broker: random ports, loopback default, clean stop."""

from digitaltwin import ZMQ_BrokerProcess
from digitaltwin.config import DEFAULT_BIND_HOST


async def test_start_reports_bound_random_loopback_ports():
    proc = ZMQ_BrokerProcess()
    try:
        pub_addr, sub_addr = await proc.start()

        for addr in (pub_addr, sub_addr):
            assert addr.startswith(f"tcp://{DEFAULT_BIND_HOST}:")
            assert int(addr.rsplit(":", 1)[1]) > 0

        assert pub_addr != sub_addr
        assert proc.get_connection_str() == (pub_addr, sub_addr)
        assert proc.is_alive()

    finally:
        await proc.stop()


async def test_start_stop_cycling():
    proc = ZMQ_BrokerProcess()
    seen = []

    for _ in range(3):
        addrs = await proc.start()
        assert proc.is_alive()
        seen.append(addrs)

        # start is idempotent while running
        assert await proc.start() == addrs

        await proc.stop()
        assert not proc.is_alive()
        assert proc.get_connection_str() == (None, None)

        # stop is idempotent
        await proc.stop()

    # random ports: a fresh cycle does not reuse the previous bind
    assert len(set(seen)) == len(seen)


async def test_configured_addresses_are_honoured():
    # port 0 is the POSIX 'pick one for me' spelling; either way the
    # caller learns the actual port from the reported addresses
    proc = ZMQ_BrokerProcess("tcp://127.0.0.1:0", "tcp://127.0.0.1:0")
    try:
        pub_addr, sub_addr = await proc.start()
        assert not pub_addr.endswith(":0")
        assert not sub_addr.endswith(":0")
    finally:
        await proc.stop()
