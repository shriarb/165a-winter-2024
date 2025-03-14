from lstore.index import Index
from time import time
from collections import defaultdict
from threading import Lock
from lstore.bufferpool import BufferPool
from lstore.config import *

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)

        self.total_columns = self.num_columns + DEFAULT_PAGE_COUNT
        self.path = ""

        self.records = 0
        self.updates = 0
        self.RID_map = {}
        
        self.lock_manager = defaultdict()
        self.insert_lock = Lock()
        self.update_lock = Lock()

    def get_rid(self, key):
        return self.RID_map[key]
        
        
    def set_path(self, path):
        self.path = path
        BufferPool.set_storage_path(self.path)

    def find_page_address(self, page_type):
        tail_count = self.updates
        base_count = self.records - tail_count
        pindx = base_count % TUPLES_PER_PAGERANGE
        if page_type == 'base':
            bindx = base_count % TUPLES_PER_PAGE
            return pindx, bindx
        else:
            tindx = tail_count % TUPLES_PER_PAGE
            return pindx, tindx

    def insert_base_record(self, columns):
        pindx, bindx = self.find_page_address('base')
        for i, value in enumerate(columns):     
            id = (self.name, "base", i, pindx, bindx)
            page = BufferPool.get(id)
            pindx, bindx, id = self._handle_page_capacity(page, pindx, bindx, i)
            page.write(value)
            offset = page.num_records - 1
            BufferPool.update_cache(id, page)

        self._update_metadata(columns, pindx, bindx, offset)

    def _handle_page_capacity(self, page, pindx, bindx, col_index):
        """Handles page capacity and updates page indices if needed."""
        if not page.has_capacity():
            if bindx == MAX_PAGES_PER_RANGE - 1:
                pindx += 1
                bindx = 0
            else:
                bindx += 1
        return pindx, bindx, (self.name, "base", col_index, pindx, bindx)

    def _update_metadata(self, columns, pindx, bindx, offset):
        """Updates metadata like page directory, RID map, and record count."""
        rid = columns[0]
        self.page_directory[rid] = [self.name, "base", pindx, bindx, offset]
        self.RID_map[columns[self.key + DEFAULT_PAGE_COUNT]] = rid
        self.records += 1
        self.index.insert(columns[DEFAULT_PAGE_COUNT:], rid)

    def tail_write(self, columns):
        pindx, tindx = self.find_page_address('tail')
        for i, value in enumerate(columns):
            id = (self.name, "tail", i, pindx, tindx)
            page = BufferPool.get(id)
            pindx, tindx, id = self._handle_page_capacity(page, pindx, tindx, i)        
            page = BufferPool.get(id)
            page.write(value)
            offset = page.num_records - 1
            BufferPool.update_cache(id, page)
        self._update_metadata(columns, pindx, tindx, offset)
        self.updates += 1

    def rid_lookup(self, col_index, search_val):
        return [
            rid for rid in self.page_directory 
            if self.find_record(rid)[col_index + DEFAULT_PAGE_COUNT] == search_val
        ]

    def find_value(self, col_index, address):
        page = BufferPool.get((address[0], address[1], col_index, address[2], address[3]))
        return page.get_value(address[4])

    def update_value(self, col_index, address, val):
        id = (address[0], address[1], col_index, address[2], address[3])
        page = BufferPool.get(id)
        page.update(address[4], val)
        BufferPool.update_cache(id, page)

    def find_record(self, rid):
        location = self.page_directory[rid]
        return [self.find_value(i, location) for i in range(DEFAULT_PAGE_COUNT + self.num_columns)]

    
    def __merge(self):
        print("merge is happening")
        pass
 
