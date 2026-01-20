import mmap
import os
import struct


def _fnv1a_hash64(s):
    h = 1469598103934665603
    for c in s.encode("utf-8", errors="replace"):
        h ^= c
        h *= 1099511628211
        h &= 0xFFFFFFFFFFFFFFFF
    return h


def _fnv1a_hash32(s):
    h = 2166136261
    for c in s.encode("utf-8", errors="replace"):
        h ^= c
        h *= 16777619
        h &= 0xFFFFFFFF
    return h


class NodeHashIndex:
    """
    Binary hash lookup for node_id -> community_id using the index .ndx produced by gfaidx
    """
    def __init__(self, ndx_path):
        self._fh = open(ndx_path, "rb")
        self._size = os.fstat(self._fh.fileno()).st_size
        # < for little-endian
        # Q for unsigned long long (64 bits) hash
        # I for unsigned int (32 bits) hash
        # I for unsigned int (32 bits) community ID
        self._entry = struct.Struct("<QII")
        if self._size % self._entry.size != 0:
            raise ValueError(f"Invalid .ndx size for {ndx_path}")
        self._n = self._size // self._entry.size
        self._mm = mmap.mmap(self._fh.fileno(), 0, access=mmap.ACCESS_READ)
        self._path = ndx_path

    def __del__(self):
        try:
            self._mm.close()
            self._fh.close()
        except Exception:
            pass

    def lookup(self, node_id):
        query_hash32 = _fnv1a_hash32(node_id)
        query_hash64 = _fnv1a_hash64(node_id)
        lo, hi = 0, self._n
        while lo < hi:
            mid = (lo + hi) // 2
            offset = mid * self._entry.size
            h, h32, com = self._entry.unpack_from(self._mm, offset)
            if h < query_hash64:
                lo = mid + 1
            elif h > query_hash64:
                hi = mid
            else:
                left = mid
                while left > 0:
                    prev = left - 1
                    ph, _, _ = self._entry.unpack_from(self._mm, prev * self._entry.size)
                    if ph != query_hash64:
                        break
                    left = prev
                i = left
                while i < self._n:
                    h64, h32, com = self._entry.unpack_from(self._mm, i * self._entry.size)
                    if h64 != query_hash64:
                        break
                    if h32 == query_hash32:
                        return com
                    i += 1
                return None
        return None
