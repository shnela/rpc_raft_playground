import logging
from random import random, choice
import sys
from threading import Thread
from time import sleep
import warnings

import rpyc
from rpyc.utils.server import ThreadedServer


CLUSTER_PORTS = (
    18850,
    18851,
    18852,
)


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - [pid %(process)d] %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


class MyService(rpyc.Service):
    def __init__(self):
        super().__init__()
        logger.debug("init")
        self.val = 0
    
    def on_connect(self, conn):
        logger.debug("connected")

    def on_disconnect(self, conn):
        logger.debug("disconnected")

    def exposed_incr(self):
        val = self.val
        self.val += 1
        return val


def run_service(service_port):
    service = MyService()
    t = ThreadedServer(
        service, port=service_port
        )
    t.start()


def init_connections(service_port):
    connections = dict()

    for cluster_port in CLUSTER_PORTS:
        if cluster_port == service_port:
            continue

        while True:
            try:
                connections[cluster_port] = rpyc.connect("localhost", port=cluster_port)
                break
            except ConnectionRefusedError:
                warnings.warn(f"Can't connect to port {cluster_port}, retry.")
                sleep(1)
    
    return connections


def call_cluster(service_port):
    connections = init_connections(service_port)
    while True:
        peer_port, peer_connection = choice(list(connections.items()))
        cluster_service = peer_connection.root

        inc_result = cluster_service.incr()
        logger.info(f"{service_port} -> {peer_port} {inc_result}")
        sleep(1 + random())


if __name__ == "__main__":
    service_port = int(sys.argv[1])

    service_thread = Thread(target=run_service, args=(service_port,))
    service_thread.start()

    call_cluster(service_port)
