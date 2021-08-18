def encode(item):
    if isinstance(item, int):
        item_bytes = encode_int(item)
    elif isinstance(item, str):
        item_bytes = encode_str(item)
    elif isinstance(item, list):
        item_bytes = encode_list(item)
    elif isinstance(item, dict):
        item_bytes = encode_dict(item)
    else:
        raise TypeError()
    return item_bytes


def encode_int(i: int):
    bi = str(i).encode()
    return b'i' + bi + b'e'


def encode_str(s: str):
    s_bytes = s.encode("utf-8")
    prefix = "{length}:".format(length=str(len(s_bytes)))
    return prefix.encode('ascii') + s_bytes


def encode_list(list1: list):
    l_bytes = b'l'
    for item in list1:
        l_bytes += encode(item)
    l_bytes += b'e'
    return l_bytes


def encode_dict(d: dict):
    d_bytes = b'd'
    for k, v in d.items():
        d_bytes += encode(k)
        d_bytes += encode(v)
    d_bytes += b'e'
    return d_bytes


def decode_int(data: bytes):
    end_idx = data.find(b'e')
    if end_idx == -1:
        raise IndexError(-len(data))
    i_bytes = data[1:end_idx]
    return int(i_bytes.decode()), data[end_idx+1:]


def decode_str(data: bytes):
    sep_idx = data.find(b':')
    if sep_idx == -1:
        raise IndexError(-len(data))
    length = int(data[:sep_idx].decode())
    start_idx = sep_idx + 1
    end_idx = start_idx + length
    string_bytes = data[start_idx: end_idx]
    string = string_bytes.decode()
    return string, data[end_idx:]


def decode(data: bytes):
    try:
        val, remaining_data = _decode(data)
    except IndexError as idx:
        val = None
        pos = idx.args[0]
        remaining_pos = len(data) + pos
        remaining_data = data[remaining_pos:]

    if remaining_data != bytes():
        remaining_pos = len(data) - len(remaining_data)
        remaining_tips = remaining_data[:20]
        error_message = f"decode error at index {remaining_pos}, {remaining_tips}"
        raise TypeError(error_message)

    return val


def _decode(data: bytes):
    if not isinstance(data, bytes) and len(data) < 1:
        raise IndexError(-len(data))
    t = data[0]
    if t == ord(b'i'):
        return decode_int(data)
    elif ord(b'0') <= t <= ord(b'9'):
        return decode_str(data)
    elif t == ord(b'l'):
        return decode_list(data)
    elif t == ord(b'd'):
        return decode_dict(data)
    else:
        raise IndexError(-len(data))


def decode_list(data: bytes):
    list1 = []
    data = data[1:]
    while True:
        t = data[0]
        if t == ord(b'e'):
            return list1, data[1:]
        item, data = _decode(data)
        list1.append(item)


def decode_dict(data: bytes):
    d = {}
    data = data[1:]
    while True:
        t = data[0]
        if t == ord(b'e'):
            return d, data[1:]
        key, data = _decode(data)
        value, data = _decode(data)
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
