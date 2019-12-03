__doc__ = """

Rendezvous hashing based ring of nodes.

Uses murmur3 to convert key to a 32 bit number and uses the weighing scheme
proposed in the original white paper that introduced Rendezvous hashing.

"""

import mmh3
import json

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


if __name__ == "__main__":
    import uuid, time

    node = {1: [], 2: [], 3: []}

    then = time.time()
    for _ in range(100000):
        uid = json.dumps({str(uuid.uuid4()): str(uuid.uuid4())})
        n = hash(uid, node.keys())
        node[n].append(uid)
