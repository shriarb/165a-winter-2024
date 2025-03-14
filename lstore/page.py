from lstore.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.is_dirty = False
        self.TPS = 0
        self.pinned = 0


    def has_capacity(self):
        return self.num_records < TUPLES_PER_PAGE


    def write(self, data):
        self.data[self.num_records * 8:(self.num_records + 1) * 8] = int(data).to_bytes(8, byteorder='big')
        self.num_records += 1

    def setAsdirty(self):
        self.is_dirty = True

    def get_value(self, ind):
        value = int.from_bytes(self.data[ind * 8:(ind + 1) * 8], 'big')
        return value
    
    def find_value(self, dest):
        offsets = []
        for i in range(TUPLES_PER_PAGE):
            value = int.from_bytes(self.data[i * 8:(i + 1) * 8], 'big')
            if value == dest:
                offsets.append(i)
        return offsets        

    def update(self, indx, data):
        self.data[indx * 8:(indx + 1) * 8] = int(data).to_bytes(8, byteorder='big')

class PageRange:

    def __init__(self):
        self.base_index = 0 
        self.tail_index = 0 
        self.base = [None] * MAX_PAGES_PER_RANGE  
        self.tail = [None] 
        
    def is_page_exist(self, offset, type):
        if type == "base":
            return self.base[offset] != None
        else:
            return self.tail[offset] != None
        
    def create_base_page(self, offset, content = None): 
        if content == None:
            self.base[offset] = Page()
        else:
            self.base[offset] = content

    def increment_base_page_idx(self):
        self.base_index += 1

    def latest_base_page(self):
        return self.base[self.base_index]

    def latest_tail_page(self):
        return self.tail[self.tail_index]

    def insert_tail_page(self):
        if self.tail[self.tail_index] == None:
            self.tail[self.tail_index] = Page()
        else:
            self.tail.append(Page())
            self.tail_index += 1
        

    def last_base_page(self):
        return self.base_index == MAX_PAGES_PER_RANGE - 1