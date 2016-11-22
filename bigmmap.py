import mmap
import struct
from collections import deque, Counter


class BigMMap:
    """An mmap of int32 numbers contained in a very big file."""
    ZERO_VALUE = 0x7FFFFFFE

    def __init__(self, filename, mmap_count=2, page_size=64):
        self.page_size = page_size * 1024 * 1024
        self.mmap_count = mmap_count
        self.history_size = 1000
        try:
            self.f = open(filename, 'r+b')
            self.f.seek(0, 2)
            self.length = self.f.tell()
        except IOError:
            self.f = None
            self.length = 0
        self.map = {}
        self.access_count = Counter()
        self.accessed_pages = deque()

    def flush(self, page=None):
        if page is not None:
            if page in self.map:
                self.map[page].flush()
        else:
            for i in self.map:
                self.map[i].flush()

    def close(self, page=None):
        self.flush(page)
        if page is not None:
            if page in self.map:
                self.map[page].close()
                del self.map[page]
        else:
            for m in self.map:
                self.map[m].close()
            self.map.clear()
            if self.f is not None:
                self.f.close()

    def _get_page(self, offset):
        """Returns a tuple (mmap, adj. offset)."""
        if (offset << 2) + 4 >= self.length:
            raise ValueError('Offset {0} is outside the file length {1}.'.format(offset, self.length >> 2))
        page = offset / self.page_size
        if page not in self.map:
            if len(self.map) >= self.mmap_count:
                # Find the least used map
                usage = self.history_size
                m = None
                for i in self.map:
                    if self.access_count[i] < usage:
                        m = i
                        usage = self.access_count[i]
                self.close(m)
            fofs = page * self.page_size << 2
            flen = min(self.page_size << 2, self.length - fofs)
            self.map[page] = mmap.mmap(self.f.fileno(), flen, offset=fofs)
        # Update counts
        self.access_count[page] += 1
        self.accessed_pages.append(page)
        while len(self.accessed_pages) > self.history_size:
            self.access_count[self.accessed_pages.popleft()] -= 1
        return (self.map[page], (offset - page * self.page_size) << 2)

    def __len__(self):
        return self.length >> 2

    def __getitem__(self, offset):
        if self.f is None:
            return None
        m = self._get_page(offset)
        s = m[0][m[1]:m[1] + 4]
        v = struct.unpack('<l', s)[0]
        if v == 0:
            return None
        elif v == self.ZERO_VALUE:
            return 0
        else:
            return v

    def __setitem__(self, offset, value):
        if self.f is None:
            return
        if value is None:
            v = 0
        elif value == 0:
            v = self.ZERO_VALUE
        else:
            v = value
        try:
            s = struct.pack('<l', v)
        except struct.error as e:
            print 'Erroneous value:', v
            raise e
        m = self._get_page(offset)
        m[0][m[1]:m[1] + 4] = s
