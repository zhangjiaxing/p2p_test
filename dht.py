import socket
import random
import time
import copy
import typing
from struct import unpack, pack
from collections import OrderedDict

from krpc import Krpc, KrpcRequest
from event import EventDispatcher, Event, KrpcEvent, EventProcessor, Timer, EventType

"""
参考
https://segmentfault.com/a/1190000002528378
https://blog.csdn.net/u012785382/article/details/70738880
https://www.jianshu.com/p/159c647c0095?utm_campaign=maleskine&utm_content=note&utm_medium=seo_notes
https://fenying.gitbooks.io/bittorrent-specification-chinese-edition/content/
https://www.cnblogs.com/LittleHann/p/6180296.html#_lab2_1_2
"""


def gen_node_id() -> bytes:
    random.seed(time.time())
    return random.randbytes(20)


def load_self_node_id() -> bytes:
    return b'0123456789helloworld'
    # return int.from_bytes(b'0123456789helloworld', 'big', signed=True)


def print_node_id(node_id: bytes):
    print(node_id.hex())


def bytes_get_bit(node_id: bytes, idx: int) -> bool:
    bytes_pos = idx // 8
    char = node_id[bytes_pos]

    bit_pos = idx % 8
    mask = 0x80 >> bit_pos
    if mask | char == mask:
        return True
    else:
        return False


def bytes_set_bit(node_id: bytes, idx: int, state: bool):
    ret_node_id = bytearray(node_id)
    bytes_pos = idx // 8
    char = ret_node_id[bytes_pos]

    bit_pos = idx % 8
    if state:
        mask = 0x80 >> bit_pos
        char |= mask
        ret_node_id[bytes_pos] = char
    else:
        mask = 0x80 >> bit_pos
        mask = ~mask
        char |= mask
        ret_node_id[bytes_pos] = char
    return bytes(ret_node_id)


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


class Node:
    def __init__(self, node_id: bytes, ip: str, port: int):
        self.node_id = node_id
        self.ip = ip
        self.port = port

    def __eq__(self, other):
        return self.node_id == other.node_id

    @staticmethod
    def node_list_from_bytes(nodes_bytes: bytes) -> typing.List:
        node_list = []
        pos = 0
        while pos + 26 <= len(nodes_bytes):
            end = pos + 26

            node_data = nodes_bytes[pos:end]
            node = Node.from_bytes(node_data)
            node_list.append(node)

            pos = end
        return node_list

    @staticmethod
    def from_bytes(node_data: bytes):
        node_id = node_data[:20]
        ip_bytes = node_data[20:24]
        port_bytes = node_data[24:26]
        ip = socket.inet_ntoa(ip_bytes)
        port = unpack("!H", port_bytes)[0]
        return Node(node_id, ip, port)

    def to_bytes(self):
        return self.node_id + socket.inet_aton(self.ip) + pack("!H", self.port)

    def __str__(self):
        return f"Node(node_id: {self.node_id})"


class Bucket:
    K = 8

    def __init__(self, node_start: bytes, index: int, power: int):
        """
        :param node_start: 存储id的起始
        :param index: K桶下标, 另外index-1的数字也代表node_start 和 self_node_id 前多少位相同
        :param power: 存储的节点范围 == 2**power， 最大为2**160， 每fork一次，节点存储范围减少一半

        在最左边的K桶范围越大，距离self_node_id越远， 越右边的K桶范围越小，距离self_node_id越近
        第一个K桶，index是0，node_start 从0开始，容量是2**160
        分裂一次后，第一个K桶0下标的bit和self_node_id正好相反，第二个K桶0下标的bit和self_node_id一样， 容量则除以二

        会K桶的分裂特性节省内存，节点查找合适的K桶性能差。如果一开始就创建160个K桶，则K桶不需要分裂，节点查找合适的K桶会更容易（性能更好），
        下面是算法：
        range idx in range(160):
            if bytes_get_bit(node_id, idx) == bytes_get_bit(self_node_id, idx):
                continue
            else:
                return idx  # 返回合适的K桶下标
        """
        self.nodes: typing.OrderedDict[bytes, Node] = OrderedDict()  # 越后面的位置node越新鲜
        self.caches: typing.OrderedDict[bytes, Node] = OrderedDict()  # 越后面的位置node越新鲜
        self.node_start: bytes = node_start
        self.index: int = index
        self.power: int = power

    def __str__(self):
        return f"Bucket(idx: {self.index}, pow: {self.power}, start: {self.node_start})"

    def is_full(self):
        return len(self.nodes) == self.K

    def can_fork(self):
        # 每个节点只能分裂一次，每次分裂后，新生的两个K桶，左边的不能分裂，右边的可以分裂。
        # 这样只有K桶数组最后一个元素才有可能可以分裂成两个K桶
        max_power = 160 - self.index
        # min_power = 160 - self.index - 1

        if self.power == max_power and self.power > 3:
            return True
        else:
            return False

    def fork(self) -> typing.Tuple:
        self_id = load_self_node_id()
        node_start_left = copy.copy(self.node_start)
        node_start_right = copy.copy(self.node_start)
        bit = bytes_get_bit(self_id, self.index)
        node_start_left = bytes_set_bit(node_start_left, self.index, not bit)
        node_start_right = bytes_set_bit(node_start_right, self.index, bit)

        b1 = Bucket(node_start_left, self.index, self.power - 1)
        b2 = Bucket(node_start_right, self.index + 1, self.power - 1)
        return b1, b2

    def in_range(self, node_id: bytes):
        node_int = int.from_bytes(node_id, 'big', signed=False)
        start_int = int.from_bytes(self.node_start, 'big', signed=False)
        end_int = start_int + 2 ** self.power
        return start_int <= node_int < end_int

    def add_node(self, node: Node):
        print("add_node: ", self, node)
        if len(self.nodes) >= self.K and node.node_id not in self.nodes:
            self.nodes[node.node_id] = node
            self.nodes.move_to_end(node.node_id)
        else:
            self.caches[node.node_id] = node
            self.caches.move_to_end(node.node_id)
            self.clear_caches()

    def clear_caches(self):
        while len(self.caches) > 16:
            self.caches.popitem()


class Dht(EventProcessor):
    K = 8

    def __init__(self, local_ip, local_port):
        self.self_node_id = load_self_node_id()
        self.dispatcher = EventDispatcher(self, local_ip, local_port)
        self.KrpcRequest = KrpcRequest
        self.KrpcRequest.init_class(self.self_node_id)
        self.table: typing.List[Bucket] = []
        first_bucket = Bucket(b'\0' * 20, 0, 160)
        self.table.append(first_bucket)

    def check_bucket(self, idx: int):
        bucket = self.table[idx]
        if bucket.is_full() and bucket.can_fork():
            bucket1, bucket2 = bucket.fork()
            self.table.pop(-1)
            self.table.append(bucket1)
            self.table.append(bucket2)

    def join_table(self, node: Node):
        for idx, bucket in enumerate(self.table):
            if bucket.in_range(node.node_id):
                bucket.add_node(node)
                self.check_bucket(idx)
                return
        print("ERROR: join_dht")
        print("node", node)
        for idx, bucket in enumerate(self.table):
            print(idx, bucket)
        print("=======================END=============")

    def ping_response_join_table(self, krpc: Krpc):
        krpc_dict = krpc.json()
        node_id: bytes = krpc_dict[b'r'][b'id']
        node_ip = krpc.sender_ip
        node_port = krpc.sender_port

        node = Node(node_id, node_ip, node_port)
        self.join_table(node)

    @staticmethod
    def receive_ping(ev: KrpcEvent):
        print('receive ping response: ', ev.event_type, ev.response_krpc)

    @staticmethod
    def receive_find_node(ev: KrpcEvent):
        print('receive find node response: ', ev.event_type, ev.response_krpc)

    def post_event(self, ev: Event or KrpcEvent):
        if ev.event_type == EventType.EVENT_TIMEOUT:
            pass
        if ev.event_type == EventType.EVENT_REQUEST:
            pass
        if ev.event_type == EventType.EVENT_RESPONSE:
            self.ping_response_join_table(ev.response_krpc)

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
