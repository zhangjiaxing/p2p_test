import socket
import select
import random
import bencode


def gen_node_id() -> bytes:
    return random.randbytes(20)


def self_node_id():
    return b'0123456789helloworld'


def gen_token() -> bytes:
    return random.randbytes(10)


def print_node_id(node_id: bytes):
    print(node_id.hex())


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


class Krpc:
    _transactionID = 0
    self_node_id = b''

    @classmethod
    def init_class(cls, node_id: bytes):
        cls.self_node_id = node_id

    @classmethod
    def get_transaction_id(cls):
        cls._transactionID += 1
        cls._transactionID %= 2**32
        return cls._transactionID.to_bytes(4, 'big', signed=False)

    @classmethod
    def create_request(cls, func: str, args: dict):
        rpc = {
            "t": cls.get_transaction_id(),
            "y": "q",
            "q": func,
            "a": args
        }
        return Krpc(rpc)

    @classmethod
    def create_response(cls, transaction_id: bytes, response_data: dict):
        rpc = {
            "t": transaction_id,
            "y": "r",
            "r": response_data,
        }
        return Krpc(rpc)

    @classmethod
    def create_error(cls, transaction_id: bytes, err_number: int, msg: str = None):
        err_desc = {
            201: "一般错误",
            202: "服务错误",
            203: "协议错误, 比如不规范的包, 无效的参数, 或者错误的token",
            204: "未知方法"
        }

        msg = msg or err_desc.get(err_number, "未知错误")
        rpc = {
            "t": transaction_id,
            "y": "e",
            "e": [err_number, msg]
        }
        return Krpc(rpc)

    @classmethod
    def ping(cls):
        args = {
            "id": cls.self_node_id
        }
        return cls.create_request("ping", args)

    @classmethod
    def find_node(cls, target_node: bytes):
        args = {
            "id": cls.self_node_id,
            "target": target_node
        }
        return cls.create_request("find_node", args)

    @classmethod
    def get_peers(cls, info_hash: bytes):
        args = {
            "id": cls.self_node_id,
            "info_hash": info_hash
        }
        return cls.create_request("get_peers", args)

    def ping_response(self):
        data = {
            "id": self.__class__.self_node_id
        }
        t = self.rpc["t"]
        return self.create_response(t, data)

    def find_node_response(self, nodes: bytes):
        data = {
            "id": self.__class__.self_node_id,
            "nodes": nodes,
        }
        t = self.rpc["t"]
        return self.create_response(t, data)

    def get_peers_response_values(self, values: list):
        data = {
            "id": self.__class__.self_node_id,
            "values": values,
            "token": gen_token()
        }
        t = self.rpc["t"]
        return self.create_response(t, data)

    def get_peers_response_nodes(self, nodes: bytes):
        data = {
            "id": self.__class__.self_node_id,
            "nodes": nodes,
            "token": gen_token()
        }
        t = self.rpc["t"]
        return self.create_response(t, data)

    @staticmethod
    def from_bytes(data: bytes):
        rpc = bencode.decode(data)
        return Krpc(rpc)

    def __init__(self, rpc: dict):
        self.rpc = rpc

    def json(self):
        return self.rpc

    def __str__(self):
        return repr(self.rpc)

    def bencode(self):
        return bencode.encode(self.rpc)


class Dht:

    def __init__(self, local_ip, local_port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        self.socket.bind((local_ip, local_port,))


    @staticmethod
    def get_start_node_list():
        return (
            ('router.bittorrent.com', 6881),
            ('router.utorrent.com', 6881),
            ('router.bitcomet.com', 6881),
            ('dht.transmissionbt.com', 6881),
        )


if __name__ == '__main__':
    self_id = self_node_id()
    print_node_id(self_id)

    print(distance_metric(b'ababab0127', b'ababab0111'))

    Krpc.init_class(self_id)
    ping_packet = Krpc.ping()
    print(ping_packet)

    print(ping_packet.ping_response())

