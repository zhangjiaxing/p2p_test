import heapq
import select
import time
import socket
import typing
from enum import Enum
from krpc import Krpc, KrpcRequest


class EventType(Enum):
    EVENT_STARTUP = 0
    EVENT_QUIT = 1
    EVENT_TIMEOUT = 3
    EVENT_REQUEST = 4
    EVENT_RESPONSE = 5
    EVENT_ERROR = 6


class Event:
    def __init__(self, event_type: EventType):
        self.event_type = event_type


class KrpcEvent(Event):
    def __init__(self, event_type: EventType, local_krpc: KrpcRequest or None = None, remote_krpc: Krpc or None = None):
        super().__init__(event_type)
        self.local_krpc: KrpcRequest or None = local_krpc
        self.remote_krpc: Krpc or None = remote_krpc

    def __str__(self):
        desc = ''
        if self.event_type == EventType.EVENT_TIMEOUT:
            desc += 'timeout event:'
        elif self.event_type == EventType.EVENT_REQUEST:
            desc += 'request event:'
        elif self.event_type == EventType.EVENT_RESPONSE:
            desc += 'response event:'

        desc += '\nreq_krpc: '
        desc += str(self.local_krpc)
        desc += '\nresponse_krpc: '
        desc += str(self.remote_krpc)
        desc += '\n'
        return desc


class EventProcessor:
    def post_event(self, ev: Event):
        pass


class Timer:
    def __lt__(self, other):
        return self.next < other.next

    def __init__(self, timeout, callback, args=None, oneshot=True):
        self.timeout = timeout
        self.oneshot = oneshot
        self._callback = callback
        self.args = args
        self.next = float('+inf')

    def start(self):
        self.next = self.timeout + time.time()

    def timeleft(self):
        return self.next - time.time()

    def trigger(self):
        self._callback(self.args)
        now = time.time()
        if self.oneshot:
            self.next = float('+inf')
        else:
            self.next += self.timeout

        print(f">>>>>>>> time: {now}, next:{self.next}, timeout: {self.timeout}")


class EventDispatcher:
    def __init__(self, processor: EventProcessor, local_ip, local_port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, 0)
        self.sock.bind((local_ip, local_port,))
        self.timer_list = []
        self.krpc_heap: typing.List[KrpcRequest] = []  # 本机的krpc请求，找到超时的请求
        self.krpc_dict = {}     # 本机发送的krpc请求
        self.wait_set = set()
        self.wait_dict = {}
        self.processor = processor

    def __str__(self):
        return f"EventDispatcher: len(krpc_dict):{len(self.krpc_dict)}, len(krpc_heap): {len(self.krpc_heap)}"

    def push_request(self, krpc: KrpcRequest):
        heapq.heappush(self.krpc_heap, krpc)
        transaction_id = krpc.transaction_id()
        self.krpc_dict[transaction_id] = krpc

    def fetch_request(self, transaction: bytes) -> KrpcRequest:
        if transaction in self.krpc_dict:
            krpc = self.krpc_dict.pop(transaction)
            return krpc

    def process_event(self):
        ev: KrpcEvent or None = None

        rl, wl, xl = select.select([self.sock], [], [], 0.2)
        if self.sock in rl:
            recv_packet, addr = self.sock.recvfrom(1500)
            recv_krpc = Krpc.from_bytes(recv_packet, addr[0], addr[1])
            ev = self.process_receive_krpc(recv_krpc)
        else:
            while self.timer_list and self.timer_list[0].timeleft() <= 0:
                timer = heapq.heappop(self.timer_list)
                timer.trigger()
                heapq.heappush(self.timer_list, timer)

            if self.krpc_heap and time.time() >= self.krpc_heap[0].deadline:
                ev = self.process_timeout_krpc()

        if ev is not None:
            self.processor.post_event(ev)

            if ev.local_krpc:
                tid = ev.local_krpc.transaction_id()
                if tid in self.wait_set:
                    self.wait_dict[tid] = ev

            if ev.local_krpc and ev.local_krpc.callback:
                ev.local_krpc.callback(ev, ev.local_krpc.args)

    def process_timeout_krpc(self) -> KrpcEvent or None:
        krpc = heapq.heappop(self.krpc_heap)
        t = krpc.transaction_id()
        if t in self.krpc_dict:
            self.krpc_dict.pop(t)
            return KrpcEvent(EventType.EVENT_TIMEOUT, krpc)
        else:
            return None

    def process_receive_krpc(self, recv_krpc: Krpc) -> KrpcEvent or None:
        t = recv_krpc.transaction_id()
        if t in self.krpc_dict.keys():
            send_rpc: KrpcRequest = self.krpc_dict.pop(t)
            error = recv_krpc.error()
            if error is not None:
                return KrpcEvent(EventType.EVENT_ERROR, send_rpc, recv_krpc)
            else:
                return KrpcEvent(EventType.EVENT_RESPONSE, send_rpc, recv_krpc)
        else:
            return KrpcEvent(EventType.EVENT_REQUEST, None, recv_krpc)

    def send_krpc(self, krpc: KrpcRequest, sock_addr, callback=None, args=None, sync=False, timeout=5):
        krpc.set_timeout(timeout)
        krpc.set_callback(callback, args)

        if sync:
            self.wait_set.add(krpc.transaction_id())

        self.push_request(krpc)
        packet = krpc.bencode()
        self.sock.sendto(packet, sock_addr)

    def wait_response(self, transaction_id: bytes):
        self.wait_set.add(transaction_id)

        while True:
            if transaction_id in self.wait_dict:
                self.wait_set.discard(transaction_id)
                return self.wait_dict.pop(transaction_id)
            else:
                self.process_event()

    def add_timer(self, timer: Timer):
        heapq.heappush(self.timer_list, timer)
