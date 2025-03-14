from datetime import datetime
from lstore.page import Page
from lstore.config import *
import pickle
import os

class BufferPool:
    storage_path = ""
    cache = {}
    max_cache_size = 0
    
    
    def __init__(page_cache, size = 2000):
        page_cache.max_cache_size = size
        
    @classmethod 
    def set_storage_path(page_cache, path):
        """Sets the storage directory for persisting pages."""
        page_cache.storage_path = path
    
    @classmethod
    def is_cached(page_cache, id):
        """Checks if a page exists in the cache."""
        return id in page_cache.cache.keys()

    @classmethod
    def store_page(page_cache, id, page):
        """Stores a page in memory and marks it as modified."""
        page_cache.cache[id] = page
        page_cache.cache[id].setAsdirty()

    @classmethod
    def update_cache(page_cache, id, page):
        """Updates an existing page in memory and marks it as modified."""
        page_cache.cache[id] = page
        page_cache.cache[id].setAsdirty()

    @classmethod
    def is_cache_full(page_cache):
        """Checks if the cache has reached its maximum capacity."""
        return len(page_cache.cache) >= 2000

    @classmethod
    def construct_page_path(page_cache, buffer_id):
        """Generates a file path for storing the page."""
        dirname = os.path.join(page_cache.storage_path, str(buffer_id[2]), str(buffer_id[3]), buffer_id[1])
        dirr = os.path.join(dirname, str(buffer_id[4]) + '.pkl')
        return dirr

    @classmethod
    def get(page_cache, id):
        """
        Retrieves a page from cache or loads it from disk.
        If the page does not exist, a new one is created.
        """
        if id in page_cache.cache:
            return page_cache.cache[id]

        dirPath = page_cache.construct_page_path(id)

        if not os.path.isfile(dirPath):
            # Page does not exist on disk, create a new one
            page = Page()
            page_cache.store_page(id, page)
            return page
        else:
            # Load page from disk if not in cache
            if not page_cache.is_cached(id):
                page_cache.cache[id] = page_cache.read(dirPath)
            return page_cache.cache[id]
    
    @classmethod
    def read(page_cache, file_path):
        """Reads a page from disk storage."""
        f = open(file_path, 'r+b')
        page = Page()
        metadata = pickle.load(f)
        
        page.num_records = metadata[0]
        page.is_dirty = metadata[1]
        page.pinned = metadata[2]
        page.TPS = metadata[3]
        page.data = pickle.load(f)
        f.close()
        return page

    @classmethod
    def write(page_cache, page, id):
        """Writes a page to disk to ensure persistence."""
        directory = os.path.join(page_cache.storage_path, str(id[2]), str(id[3]), id[1])
        file_path = os.path.join(directory, f"{id[4]}.pkl")

        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "wb") as file:
            metadata = [page.num_records, page.is_dirty, page.pinned, page.TPS]
            pickle.dump(metadata, file)
            pickle.dump(page.data, file)

    @classmethod
    def shutdown(page_cache):
        """Flushes all cached pages to disk before closing the program."""
        for id, page in page_cache.cache.items():
            page_cache.write(page, id)
