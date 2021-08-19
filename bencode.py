def encode(item):
    r = bytearray()

    if isinstance(item, int):
        encode_int(item, r)
    elif isinstance(item, str):
        encode_str(item, r)
    elif isinstance(item, list):
        encode_list(item, r)
    elif isinstance(item, dict):
        encode_dict(item, r)
    else:
        raise ValueError(type(item))
    return r


def encode_int(i: int, r: bytearray):
    r += b'i'
    r += str(i).encode()
    r += b'e'


def encode_str(s: str, r: bytearray):
    s_bytes = s.encode("utf-8")
    prefix = str(len(s_bytes)).encode()
    r += prefix
    r += b':'
    r += s_bytes


def encode_list(list1: list, r: bytearray):
    r += b'l'
    for item in list1:
        r += encode(item)
    r += b'e'


def encode_dict(d: dict, r: bytearray):
    r += b'd'
    for k, v in d.items():
        r += encode(k)
        r += encode(v)
    r += b'e'


def decode_int(data: bytes, start_idx: int):
    start_idx += 1
    end_idx = data.find(b'e', start_idx)
    if end_idx == -1:
        raise IndexError(start_idx)
    i_bytes = data[start_idx:end_idx]
    return int(i_bytes.decode()), end_idx+1


def decode_str(data: bytes, start_idx: int):
    sep_idx = data.find(b':', start_idx)
    if sep_idx == -1:
        raise IndexError(start_idx)
    length = int(data[start_idx:sep_idx].decode())

    str_idx = sep_idx + 1
    end_idx = str_idx + length
    string_bytes = data[str_idx: end_idx]
    string = string_bytes.decode()
    return string, end_idx


def decode(data: bytes):
    try:
        val, remaining_pos = _decode(data, 0)
    except IndexError as idx:
        val = None
        remaining_pos = idx.args[0]

    if remaining_pos != len(data):
        remaining_data = data[remaining_pos:]
        remaining_tips = remaining_data[:20]
        error_message = f"decode error at index {remaining_pos}, {remaining_tips}"
        raise ValueError(error_message)
    return val


def _decode(data: bytes, start_idx: int):
    if not isinstance(data, bytes) and len(data) - start_idx < 1:
        raise IndexError(start_idx)
    t = data[start_idx]
    if t == ord(b'i'):
        return decode_int(data, start_idx)
    elif ord(b'0') <= t <= ord(b'9'):
        return decode_str(data, start_idx)
    elif t == ord(b'l'):
        return decode_list(data, start_idx)
    elif t == ord(b'd'):
        return decode_dict(data, start_idx)
    else:
        raise IndexError(start_idx)


def decode_list(data: bytes, start_idx: int):
    list1 = []
    start_idx += 1
    while True:
        t = data[start_idx]
        if t == ord(b'e'):
            return list1, start_idx+1
        item, start_idx = _decode(data, start_idx)
        list1.append(item)


def decode_dict(data: bytes, start_idx: int):
    d = {}
    start_idx += 1
    while True:
        t = data[start_idx]
        if t == ord(b'e'):
            return d, start_idx+1
        key, start_idx = _decode(data, start_idx)
        value, start_idx = _decode(data, start_idx)
        d[key] = value


if __name__ == '__main__':
    print(encode("hello"))
    print(encode(123456))
    print(encode([5, 6, 7, 8, 9, 10, 11, 12, 13]))

    obj = {
        'name': 'zhangsan',
        'age': 13,
        'list': [1, "2", 3, "40", "500", "C6000"]
    }
    code = encode(obj)
    print(code)
    print(decode(code))
