import socket
import random
import time

from krpc import Krpc, KrpcRequest
from event import EventDispatcher, Event, KrpcEvent, EventProcessor, Timer, EventType


def gen_node_id() -> bytes:
    random.seed(time.time())
    return random.randbytes(20)


def load_self_node_id() -> bytes:
    return b'0123456789helloworld'
    # return int.from_bytes(b'0123456789helloworld', 'big', signed=True)


def print_node_id(node_id: bytes):
    print(node_id.hex())


def bytes_get_bits(node_id: bytes, nbits: int):
    n_copy = nbits // 8
    tmp_id = node_id[:n_copy]

    char = node_id[n_copy]
    nbits = nbits % 8
    x = 0
    for n in range(0, nbits):
        mask = 0x80
        x |= mask >> n

    if nbits > 0:
        char |= x
        tmp_id += char
        n_copy += 1
    tmp_id += b'\0' * (20 - n_copy)
    return tmp_id


def compact_addr_to_str(compact_addr: bytes):
    ip_str = socket.inet_ntoa(compact_addr[0:4])
    port = int.from_bytes(compact_addr[4:], 'little', signed=False)
    return f"{ip_str}:{port}"


def compact_node_to_str(compact_node: bytes):
    node_id = compact_node[:20].hex()
    addr = compact_addr_to_str(compact_node[20:])
    return f"{node_id}[{addr}]"


def distance_metric(id1: bytes, id2: bytes):
    i1 = int.from_bytes(id1, byteorder='big', signed=False)
    i2 = int.from_bytes(id2, byteorder='big', signed=False)
    return i1 ^ i2


class Bucket:
    def __init__(self, node_start, pow):
        self.node_list = []
        self.cache_list = []
        self.node_start = node_start
        self.pow = pow



class Dht(EventProcessor):
    K = 8

    def __init__(self, local_ip, local_port):
        self.self_node_id = load_self_node_id()
        self.dispatcher = EventDispatcher(self, local_ip, local_port)
        self.KrpcRequest = KrpcRequest
        self.KrpcRequest.init_class(self.self_node_id)
        self.table = []
        self.self_node_id

    def do_dht(self, ev: KrpcEvent):


    @staticmethod
    def receive_packet(ev: KrpcEvent):
        print("receive_packet: ", ev.event_type, ev.remote_krpc)

    @staticmethod
    def receive_ping(ev: KrpcEvent):
        print('receive ping response: ', ev.event_type, ev.remote_krpc)

    @staticmethod
    def receive_find_node(ev: KrpcEvent):
        print('receive find node response: ', ev.event_type, ev.remote_krpc)

    def post_event(self, ev: Event or KrpcEvent):
        if ev.event_type == EventType.EVENT_TIMEOUT:
            pass
        if ev.event_type == EventType.EVENT_REQUEST:
            pass
        if ev.event_type == EventType.EVENT_RESPONSE:
            self.build_table(ev.remote_krpc)

    def build_table(self, krpc: Krpc):
        print('build table', krpc)
        pass

    def check_table(self):
        pass

    def run(self):
        node_list = self.get_start_node_list()

        for node_addr in node_list:
            ping_packet = self.KrpcRequest.ping()
            print(node_addr)
            self.dispatcher.send_krpc(ping_packet, node_addr, self.receive_ping)

            find_node_packet = self.KrpcRequest.find_node(self.self_node_id)
            self.dispatcher.send_krpc(find_node_packet, node_addr, self.receive_find_node)

            find_node_packet = self.KrpcRequest.find_node(gen_node_id())
            self.dispatcher.send_krpc(find_node_packet, node_addr, self.receive_find_node)

        timer = Timer(10, lambda x: print("hello", x), 'world', oneshot=False)
        timer.start()
        self.dispatcher.add_timer(timer)

        while True:
            self.dispatcher.process_event()

    @staticmethod
    def resolv_host(hostname) -> list:
        _name, _alias_list, address_list = socket.gethostbyname_ex(hostname)
        print(hostname, address_list)
        return address_list

    @staticmethod
    def get_start_node_list():
        start_node_list = (
            ('router.bittorrent.com', 6881),
            ('router.utorrent.com', 6881),
            ('dht.transmissionbt.com', 6881),
        )

        node_addr_list = []
        for hostname, port in start_node_list:
            ip_list = Dht.resolv_host(hostname)
            for ip in ip_list:
                node_addr = (ip, port)
                node_addr_list.append(node_addr)
        return node_addr_list


def test():
    self_id = load_self_node_id()
    print_node_id(self_id)

    print(distance_metric(b'hello0127', b'hello0111'))

    Krpc.init_class(self_id)
    ping_packet = Krpc.ping()
    print(ping_packet)

    print(ping_packet.ping_response())


if __name__ == '__main__':
    dht = Dht("0.0.0.0", 42892)
    dht.run()
