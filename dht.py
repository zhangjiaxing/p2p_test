import enum
import socket
import random
import time
import copy
import typing
from struct import unpack, pack
from functools import partial
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

TODO:
支持保存dht
"""


def random_node_id() -> bytes:
    random.seed(time.time())
    return random.randbytes(20)


def load_self_node_id() -> bytes:
    return bytes.fromhex("2202030405060708090001020304050607080900")
    # return b'0123456789helloworld'
    # return int.from_bytes(b'0123456789helloworld', 'big', signed=True)


def find_node_id() -> bytes:
    return bytes.fromhex("FF01020304050607080901020304050607080900")


def print_node_id(node_id: bytes):
    print(node_id.hex())


def bytes_get_bit(node_id: bytes, idx: int) -> bool:
    bytes_pos = idx // 8
    char = node_id[bytes_pos]

    bit_pos = idx % 8
    mask = 0x80 >> bit_pos
    if mask & char == mask:
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
        mask = (~mask) & 0xff
        char &= mask
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


class NodeState(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2
    DEAD = 3


class Node:
    def __init__(self, node_id: bytes, ip: str, port: int):
        self.node_id = node_id
        self.ip = ip
        self.port = port
        self._state = NodeState.DEAD
        self._last_time = 0

    def __eq__(self, other):
        return self.node_id == other.node_id

    def __hash__(self):
        return int.from_bytes(self.node_id, byteorder='big', signed=False)

    def addr(self):
        return self.ip, self.port

    def active(self):
        self._last_time = time.time()
        self._state = NodeState.ACTIVE

    def get_state(self) -> NodeState:
        inactive_time = time.time() - self._last_time
        if inactive_time > 60 * 20:
            self._state = NodeState.DEAD
        elif inactive_time > 60 * 15:
            self._state = NodeState.INACTIVE
        else:
            self._state = NodeState.ACTIVE
        return self._state

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
        node_id = self.node_id.hex()
        return f"Node(node_id: {node_id}, addr: {self.ip}:{self.port})"


def distance_metric(node1: bytes or Node, node2: bytes or Node):
    id1 = node1.node_id if isinstance(node1, Node) else node1
    id2 = node2.node_id if isinstance(node2, Node) else node2

    i1 = int.from_bytes(id1, byteorder='big', signed=False)
    i2 = int.from_bytes(id2, byteorder='big', signed=False)
    return i1 ^ i2


def sort_node_list(node_list: typing.List[Node], target_id: bytes):
    distance_cmp = partial(distance_metric, target_id)
    node_list.sort(key=distance_cmp)


class DhtBase(EventProcessor):
    def find_node(self, target_node: bytes):
        pass

    def ping_node(self, node: Node):
        pass


class Bucket:
    K = 8

    def __init__(self, dht: DhtBase, node_start: bytes, index: int, power: int):
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
        self._last_change = 0
        self._dht: DhtBase = dht

    def __str__(self):
        start = self.node_start.hex()
        return f"Bucket(idx: {self.index}, pow: {self.power}, start: {start}, node: {len(self.nodes)}, \
            caches: {len(self.caches)})"

    def capacity(self, client_node_id: bytes) -> int:
        if client_node_id is not None and self.in_range(client_node_id):
            return 4 * self.K
        else:
            return self.K

    def is_full(self, client_node_id: bytes) -> bool:
        return len(self.nodes) >= self.capacity(client_node_id)

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

        b1 = Bucket(self._dht, node_start_left, self.index, self.power - 1)
        b2 = Bucket(self._dht, node_start_right, self.index + 1, self.power - 1)
        return b1, b2

    def in_range(self, node_id: bytes):
        node_int = int.from_bytes(node_id, 'big', signed=False)
        start_int = int.from_bytes(self.node_start, 'big', signed=False)
        end_int = start_int + 2 ** self.power
        return start_int <= node_int < end_int

    def add_node(self, node: Node, client_node_id: bytes):
        # print(">>> in add_node: ", self, node)
        self._last_change = time.time()

        if node.node_id in self.nodes:
            self.nodes[node.node_id].active()
            return

        if len(self.nodes) < self.capacity(client_node_id):
            self.nodes[node.node_id] = node
            self.nodes.move_to_end(node.node_id)
        else:
            self.caches[node.node_id] = node
            self.caches.move_to_end(node.node_id)
            self.clear_caches()

    def clear_caches(self):
        while len(self.caches) > 16:
            self.caches.popitem()

    def update_all(self):
        self.check_node_state()
        self.check_update()

    def check_node_state(self):
        for node_id, node in self.nodes.items():
            state: NodeState = node.get_state()
            if state == NodeState.DEAD:
                self.nodes.pop(node_id)
                if self.caches:
                    cache_id, cache_node = self.caches.popitem(last=True)
                    self.nodes[cache_id] = cache_node
            elif state == NodeState.INACTIVE:
                self._dht.ping_node(node)
            else:
                pass

    def check_update(self):
        if self._last_change + 15 * 60 > time.time():
            pass

        if not self.nodes:
            node_id_int = int.from_bytes(self.node_start, byteorder='big', signed=False)
            node_id_int += 1 << (self.power - 1)
            node_id = node_id_int.to_bytes(20, 'big', signed=False)
        else:
            node_list = list(self.nodes.keys())
            node_id = random.choice(node_list)

        self._dht.find_node(node_id)


class Dht(DhtBase):
    K = 8

    def __init__(self, local_ip, local_port):
        self.self_node_id = load_self_node_id()
        self.dispatcher = EventDispatcher(self, local_ip, local_port)
        self.KrpcRequest = KrpcRequest
        self.KrpcRequest.init_class(self.self_node_id)
        self.table: typing.List[Bucket] = []
        first_bucket = Bucket(self, b'\0' * 20, 0, 160)
        self.table.append(first_bucket)

    def check_bucket(self, idx: int):
        bucket = self.table[idx]
        if bucket.is_full(self.self_node_id) and bucket.can_fork():
            bucket1, bucket2 = bucket.fork()
            self.table.pop(-1)

            for node in bucket.nodes.values():
                if bucket1.in_range(node.node_id):
                    bucket1.add_node(node, self.self_node_id)
                else:
                    bucket2.add_node(node, self.self_node_id)

            self.table.append(bucket1)
            self.table.append(bucket2)

    def join_table(self, node: Node):
        for idx, bucket in enumerate(self.table):
            if bucket.in_range(node.node_id):
                bucket.add_node(node, self.self_node_id)
                self.check_bucket(idx)
                print("node_join_table: ", node)
                return
        print("ERROR: join_dht")
        print("node", node)
        for idx, bucket in enumerate(self.table):
            print(idx, bucket)
        print("=======================END=============")

    def response_join_table(self, krpc: Krpc):
        krpc_dict = krpc.json()
        node_id: bytes = krpc_dict[b'r'][b'id']
        node_ip = krpc.sender_ip
        node_port = krpc.sender_port

        node = Node(node_id, node_ip, node_port)
        node.active()
        self.join_table(node)

    def test_join_table(self):
        node = Node(random_node_id(), "0.0.0.0", 6666)
        self.join_table(node)

    def print_table(self):
        print("[table]===========================")
        print("table len: ", len(self.table))
        for bucket in self.table:
            print(bucket)

    @staticmethod
    def receive_ping(ev: KrpcEvent, args):
        if ev.event_type == EventType.EVENT_TIMEOUT:
            return
        print('receive ping response: ', ev.event_type, ev.remote_krpc.sender_ip)

    def process_request(self, ev: KrpcEvent):
        krpc: Krpc = ev.remote_krpc
        krpc_dict: dict = krpc.json()
        if krpc_dict.get(b'y') != b'q':
            pass

        print('收到请求', krpc_dict)
        if krpc_dict.get(b'q') == b'ping':
            packet = krpc.ping_response()
            self.dispatcher.send_krpc(packet, (krpc.sender_ip, krpc.sender_port))

    def post_event(self, ev: Event or KrpcEvent):
        if ev.event_type == EventType.EVENT_TIMEOUT:
            pass
        if ev.event_type == EventType.EVENT_REQUEST:
            self.process_request(ev)
        if ev.event_type == EventType.EVENT_RESPONSE:
            self.response_join_table(ev.remote_krpc)

    def find_near_nodes(self, target_node: bytes) -> typing.List[Node]:
        near_node_list = []
        found: bool = False
        for bucket in self.table:
            if bucket.in_range(target_node):
                found = True

            if found:
                near_node_list.extend(bucket.nodes.values())
                if len(near_node_list) >= 8:
                    return near_node_list[:8]
        return near_node_list

    def ping_node(self, node: Node):
        find_node_packet = self.KrpcRequest.find_node(node.node_id)
        self.dispatcher.send_krpc(find_node_packet, node.addr(), timeout=3)

    def find_node(self, target_node: bytes):
        print(">>>>>>> in find_node", target_node.hex())

        distance_min = float('inf')
        distance_cur = 1 << 160
        near_node_list: typing.List[Node] = self.find_near_nodes(target_node)

        while distance_cur < distance_min:
            print('near_node_list:')
            for near_node in near_node_list:
                print("near_node: ", near_node)

            distance_min = distance_cur
            q = []
            node_set: typing.Set = set()
            for near_node in near_node_list:
                find_node_packet = self.KrpcRequest.find_node(target_node)
                self.dispatcher.send_krpc(find_node_packet, near_node.addr(), sync=True, timeout=2)
                tid = find_node_packet.transaction_id()
                q.append(tid)

            for tid in q:
                ev: KrpcEvent = self.dispatcher.wait_response(tid)
                if ev is not None and ev.event_type == EventType.EVENT_RESPONSE:
                    try:
                        response: typing.Dict = ev.remote_krpc.json()
                        nodes = Node.node_list_from_bytes(response[b'r'][b'nodes'])
                        node_set.update(nodes)
                    except Exception as e:
                        print(e)

            if not node_set:
                break

            node_list = list(node_set)
            sort_node_list(node_list, target_node)
            near_node_list = node_list[:16]
            distance_cur = distance_metric(near_node_list[0], target_node)
        print("find done =========")
        return near_node_list

    def find_self_node(self, node_addr_list: list, min_distance=float('+inf')):
        print(">>>>>>> in find_self_node")

        q = []
        node_set: typing.Set = set()
        for node_addr in node_addr_list:
            find_node_packet = self.KrpcRequest.find_node(self.self_node_id)
            self.dispatcher.send_krpc(find_node_packet, node_addr, sync=True, timeout=2)
            tid = find_node_packet.transaction_id()
            q.append(tid)

        for tid in q:
            ev: KrpcEvent = self.dispatcher.wait_response(tid)
            if ev is not None and ev.event_type == EventType.EVENT_RESPONSE:
                try:
                    response: typing.Dict = ev.remote_krpc.json()
                    nodes = Node.node_list_from_bytes(response[b'r'][b'nodes'])
                    node_set.update(nodes)
                except Exception as e:
                    print(e)

        print("node_set len :", len(node_set))
        if not node_set:
            print("node_set len = 0")
            self.print_table()
            return

        node_list = list(node_set)
        sort_node_list(node_list, self.self_node_id)
        node_list = node_list[:16]

        print("old_distance=", min_distance)
        print("new_distance=", distance_metric(node_list[0], self.self_node_id))

        if min_distance > distance_metric(node_list[0], self.self_node_id):
            min_distance = distance_metric(node_list[0], self.self_node_id)
        else:
            print("find node done,,,,,,,,,")
            self.print_table()
            return

        next_addr_list = []
        for node in node_list:
            print(f'find_node, node:{node} target: {self.self_node_id.hex()}')

            addr = (node.ip, node.port,)
            next_addr_list.append(addr)

        if len(next_addr_list) > 0:
            self.find_self_node(next_addr_list, min_distance)

    def startup_join_dht(self):
        node_list = self.get_start_node_list()
        # for node_addr in node_list:
        #     ping_packet = self.KrpcRequest.ping()
        #     print('ping: ', node_addr, ping_packet)
        #     self.dispatcher.send_krpc(ping_packet, node_addr, self.receive_ping)
        #
        #     self.find_node(node_addr, self.self_node_id)
        #     # self.find_node(node_addr, random_node_id())

        timer = Timer(0, lambda _: self.find_self_node(node_list), oneshot=True)
        timer.start()
        self.dispatcher.add_timer(timer)

        timer = Timer(120, lambda x: self.update_bucket(), oneshot=False)
        timer.start()
        self.dispatcher.add_timer(timer)

        timer = Timer(60, lambda x: self.find_node(random_node_id()), oneshot=True)
        timer.start()
        self.dispatcher.add_timer(timer)

        timer = Timer(30, lambda x: self.print_table(), oneshot=False)
        timer.start()
        self.dispatcher.add_timer(timer)

    def update_bucket(self):
        for bucket in self.table:
            bucket.update_all()

    def run(self):
        self.startup_join_dht()
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
            ('123.121.1.47', 6881),
            ('222.67.255.103', 6881),
            ('115.205.154.6', 6881),
            ('223.109.185.175', 6881),
            # 117.86.48.188:6881
            # 140.249.254.31:6881
            # 220.120.78.94:6881
            # 31.49.12.192:6881
            # 35.155.156.153:6881
            # 54.70.28.180:6881
            # 121.157.67.69:6881
        )

        node_addr_list = []
        for hostname, port in start_node_list:
            try:
                ip_list = Dht.resolv_host(hostname)
                for ip in ip_list:
                    node_addr = (ip, port)
                    node_addr_list.append(node_addr)
            except Exception as e:
                print(e)

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

    # for i in range(39999):
    #     dht.join_table_test()
    # dht.print_table()
