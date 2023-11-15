import time
import random
import bencode
import typing


def gen_token() -> bytes:
    return random.randbytes(10)


class Krpc:
    _transactionID = 0
    self_node_id = b''

    @classmethod
    def init_class(cls, node_id: bytes):
        cls.self_node_id = node_id

    @classmethod
    def gen_transaction_id(cls) -> bytes:
        cls._transactionID += 1
        cls._transactionID %= 2**32
        return cls._transactionID.to_bytes(4, 'big', signed=False)

    @classmethod
    def create_request(cls, func: str, args: dict):
        rpc = {
            b"t": cls.gen_transaction_id(),
            b"y": "q",
            b"q": func,
            b"a": args,
        }
        return cls(rpc)

    @classmethod
    def create_response(cls, transaction_id: bytes, response_data: dict):
        rpc = {
            b"t": transaction_id,
            b"y": "r",
            b"r": response_data,
        }
        return cls(rpc)

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
            b"t": transaction_id,
            b"y": "e",
            b"e": [err_number, msg]
        }
        return cls(rpc)

    @classmethod
    def ping(cls):
        args = {
            b"id": cls.self_node_id
        }
        return cls.create_request("ping", args)

    @classmethod
    def find_node(cls, target_node: bytes):
        args = {
            b"id": cls.self_node_id,
            b"target": target_node
        }
        return cls.create_request("find_node", args)

    @classmethod
    def get_peers(cls, info_hash: bytes):
        args = {
            b"id": cls.self_node_id,
            b"info_hash": info_hash
        }
        return cls.create_request("get_peers", args)

    def ping_response(self):
        data = {
            b"id": self.__class__.self_node_id,
        }
        t = self.rpc[b't']
        return self.create_response(t, data)

    def find_node_response(self, nodes: bytes):
        data = {
            b"id": self.__class__.self_node_id,
            b"nodes": nodes,
        }
        t = self.rpc[b't']
        return self.create_response(t, data)

    def get_peers_response_values(self, values: list):
        data = {
            b"id": self.__class__.self_node_id,
            b"values": values,
            b"token": gen_token()
        }
        t = self.rpc[b't']
        return self.create_response(t, data)

    def get_peers_response_nodes(self, nodes: bytes):
        data = {
            b"id": self.__class__.self_node_id,
            b"nodes": nodes,
            b"token": gen_token()
        }
        t = self.rpc[b't']
        return self.create_response(t, data)

    @staticmethod
    def from_bytes(data: bytes, sender_ip: str, sender_port: int):
        rpc = bencode.decode(data)
        return Krpc(rpc, sender_ip, sender_port)

    def __init__(self, rpc: dict, sender_ip: str = None, sender_port: int = None):
        self.rpc = rpc
        self.sender_ip = sender_ip
        self.sender_port = sender_port

    def transaction_id(self) -> bytes:
        return self.rpc.get(b't')

    def json(self):
        return self.rpc

    def __str__(self):
        return repr(self.rpc)

    def bencode(self):
        return bencode.encode(self.rpc)


class KrpcRequest(Krpc):
    def __init__(self, rpc: dict):
        super().__init__(rpc)
        self.deadline = 0
        self.callback: typing.Callable or None = None

    def __lt__(self, other):
        return self.deadline < other.deadline

    def set_timeout(self, timeout=5):
        if timeout < 1:
            timeout = 1
        self.deadline = time.time() + timeout

    def set_callback(self, callback: typing.Callable):
        self.callback = callback
