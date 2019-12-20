__doc__ = """

Rendezvous hashing based ring of nodes.

Uses murmur3 to convert key to a 32 bit number and uses the weighing scheme
proposed in the original white paper that introduced Rendezvous hashing.

"""

import mmh3
import json
import uuid

# variable from paper
a = 1103515245
b = 12345
c = 2 ** 31


def murmur(key):
    """Return murmur3 hash of the key as 32 bit signed int."""
    return mmh3.hash(key)


def weight(node, key):
    """Return the weight for the key on node.

    Uses the weighing algorithm as prescibed in the original HRW white paper.

    @params:
        node : 32 bit signed int representing IP of the node.
        key : string to be hashed.

    """

    _hash = murmur(key)
    return (a * ((a * node + b) ^ _hash) + b) % c


def hash(key, nodes):
    """Return the node to which the given key hashes to."""
    assert len(nodes) > 0
    # weights = []
    w_n = [-1, None]
    for n in nodes:
        w = weight(int(n), key)
        w_n = [w, n] if w > w_n[0] else w_n

    return w_n[1]


def print_info(nodes):
    for k, v in nodes.items():
        print('Server {0} quản lý {1} Object'.format(k, len(v)))


def input_kichban1():
    return {1: [], 2: [], 3: [], 4: [], 5: []}


def input_kichban2():
    nodes = {1: [], 2: [], 3: [], 4: [], 5: []}
    add_10000_object(nodes)
    return nodes

def add_10000_object(nodes):
    for _ in range(10000):
        uid = str(uuid.uuid4())
        n = hash(uid, nodes.keys())
        nodes[n].append(uid)


if __name__ == "__main__":
    nodes = input_kichban2()
    print('Trạng thái ban đầu:')
    print_info(nodes)
    nodes = input_kichban1()
    nodes.update({6: [], 7: [], 8: [], 9: [], 10: []})

    add_10000_object(nodes)
    add_10000_object(nodes)
    print('Trạng thái sau khi thêm 100000 Oject')
    print_info(nodes)
