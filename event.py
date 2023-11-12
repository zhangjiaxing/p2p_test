import heapq
import select
import time
import socket
import typing
from enum import Enum
from krpc import Krpc, KrpcRequest


class EventType(Enum):
    EVENT_TIMEOUT = 0
    EVENT_REQUEST = 1
    EVENT_RESPONSE = 2


class Event:
    def __init__(self, event_type: EventType, local_krpc: KrpcRequest or None, remote_krpc: Krpc or None = None):
        self.event_type = event_type
        self.local_krpc: KrpcRequest or None = local_krpc
        self.remote_krpc: KrpcRequest or None = remote_krpc

    def __str__(self):
        desc = ''
        if self.event_type == EventType.EVENT_TIMEOUT:
            desc += 'timeout event:'
        elif self.event_type == EventType.EVENT_REQUEST:
            desc += 'request event:'
        elif self.event_type == EventType.EVENT_RESPONSE:
            desc += 'response event:'

        desc += '\nlocal_krpc: '
        desc += str(self.local_krpc)
        desc += '\nremote_krpc: '
        desc += str(self.remote_krpc)
        desc += '\n'
        return desc


class EventDispatcher:
    def __init__(self, local_ip, local_port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        self.sock.bind((local_ip, local_port,))
        self.krpc_heap: typing.List[KrpcRequest] = []
        self.krpc_dict = {}
        self.response_dict = {}
        self.request_list = []
        self.request_handler: typing.Callable or None = None

    def set_request_handler(self, callback):
        self.request_handler = callback

    def push_request(self, krpc: KrpcRequest):
        heapq.heappush(self.krpc_heap, krpc)
        transaction_id = krpc.transaction_id()
        self.krpc_dict[transaction_id] = krpc

    def fetch_request(self, transaction: bytes) -> KrpcRequest:
        if transaction in self.krpc_dict:
            krpc = self.krpc_dict.pop(transaction)
            return krpc

    def process_event(self):
        ev = None

        rl, wl, xl = select.select([self.sock], [], [], 0.1)
        if self.sock in rl:
            recv_packet, addr = self.sock.recvfrom(1500)
            recv_krpc = Krpc.from_bytes(recv_packet)
            ev = self.process_receive_krpc(recv_krpc)
        else:
            if len(self.krpc_heap) > 0:
                deadline = self.krpc_heap[0].deadline

                if time.time() > deadline:
                    ev = self.process_timeout_krpc()

        if ev is not None:
            if ev.event_type == EventType.EVENT_REQUEST:
                self.request_list.append(ev)
            else:
                t = ev.local_krpc.transaction_id()
                self.response_dict[t] = ev

        while len(self.request_list) > 0:
            self.request_handler(self.request_list[0])
            self.request_list.pop(0)

        for t, ev in self.response_dict.items():
            if ev.local_krpc.callback:
                ev.local_krpc.callback(ev)
        self.response_dict.clear()

    def process_timeout_krpc(self) -> Event | None:
        krpc = heapq.heappop(self.krpc_heap)
        t = krpc.transaction_id()
        if t in self.krpc_dict:
            self.krpc_dict.pop(t)
            return Event(EventType.EVENT_TIMEOUT, krpc)
        else:
            return None

    def process_receive_krpc(self, recv_krpc: Krpc) -> Event | None:
        t = recv_krpc.transaction_id()
        if t in self.krpc_dict.keys():
            send_rpc = self.krpc_dict.pop(t)
            return Event(EventType.EVENT_RESPONSE, send_rpc, recv_krpc)
        else:
            return Event(EventType.EVENT_REQUEST, None, recv_krpc)

    def send_krpc(self, krpc: KrpcRequest, sock_addr, callback, timeout=5):
        krpc.set_timeout(timeout)
        krpc.set_callback(callback)
        self.push_request(krpc)
        packet = krpc.bencode()
        self.sock.sendto(packet, sock_addr)

    def check_transaction_event(self, transaction_id):
        return transaction_id in self.response_dict

    def wait_transaction(self, transaction_id: bytes) -> Event:
        while not self.check_transaction_event(transaction_id):
            self.process_event()
        return self.response_dict[transaction_id]

    def check_request_event(self) -> bool:
        return bool(self.request_list)
