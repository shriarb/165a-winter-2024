import threading

class ThreadLock:
    """
    Reader-Writer Lock with writer priority and condition variables.
    Ensures fair access while allowing multiple readers and a single writer at a time.
    """
    
    def __init__(self):
        self.mutex = threading.Lock()  
        self.readers_count = 0  
        self.writer_active = False  

    def acquire_read_lock(self):
        """ Attempt to acquire a shared read lock. """
        self.mutex.acquire()

        if self.writer_active:        
            self.mutex.release()
            return False
        else:
            self.readers_count += 1
            self.mutex.release()
            return True

    def release_read_lock(self):
        """ Release a previously acquired read lock. """
        self.mutex.acquire()
        self.readers_count -= 1
        self.mutex.release()

    def acquire_write_lock(self):
        """ Attempt to acquire an exclusive write lock. """
        self.mutex.acquire()

        if self.readers_count != 0:        
            self.mutex.release()
            return False
        elif self.writer_active:  
            self.mutex.release()
            return False
        else:
            self.writer_active = True
            self.mutex.release()
            return True

    def release_write_lock(self):
        """ Release a previously acquired write lock. """
        self.mutex.acquire()
        self.writer_active = False
        self.mutex.release()
