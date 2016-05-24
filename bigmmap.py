import mmap, struct
from collections import deque, Counter

class BigMMap:
    """An mmap of int32 numbers contained in a very big file."""

    def __init__(self, filename, mmap_count=2, page_size=64):
        self.page_size = page_size * 1024 * 1024
        self.mmap_count = mmap_count
        self.history_size = 1000
        self.f = open(filename, 'r+b')
        self.f.seek(0, 2)
        self.length = self.f.tell()
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
        return 0

    def __getitem__(self, offset):
        m = self._get_page(offset)
        s = m[0][m[1]:m[1] + 4]
        return struct.unpack('<l', s)[0]

    def __setitem__(self, offset, value):
        s = struct.pack('<l', value)
        m = self._get_page(offset)
        m[0][m[1]:m[1] + 4] = s
