import mmap
import os
import struct


class NodeHashIndex:
    """
    Binary hash lookup for node_id -> community_id using the .ndx file.
    """
    def __init__(self, ndx_path):
        self._fh = open(ndx_path, "rb")
        self._size = os.fstat(self._fh.fileno()).st_size
        self._entry = struct.Struct("<QI4x")
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

    def _hash(self, s):
        h = 1469598103934665603
        for c in s.encode("utf-8", errors="replace"):
            h ^= c
            h *= 1099511628211
            h &= 0xFFFFFFFFFFFFFFFF
        return h

    def lookup(self, node_id):
        query = self._hash(node_id)
        lo, hi = 0, self._n
        while lo < hi:
            mid = (lo + hi) // 2
            offset = mid * self._entry.size
            h, com = self._entry.unpack_from(self._mm, offset)
            if h < query:
                lo = mid + 1
            elif h > query:
                hi = mid
            else:
                return com
        return None
