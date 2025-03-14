from lstore.table import Table, Record
from lstore.index import Index
from lstore.concurrency_control import ThreadLock
from lstore.query import Query

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []

        self.read_locks = set()
        self.write_locks = set()
        self.insert_locks = set()
        self.target_table = None

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if self.target_table == None:
            self.target_table = table
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            key = args[0]
            if key not in self.target_table.lock_manager:
                self.insert_locks.add(key)
                self.target_table.lock_manager[key] = ThreadLock()
            if key not in self.write_locks and key not in self.insert_locks:
                if self.target_table.lock_manager[key].acquire_write_lock():
                    self.write_locks.add(key)
                else:
                    return self.abort() 
        return self.commit()
    
    def abort(self):
        for key in self.read_locks:
            self.target_table.lock_manager[key].release_read_lock()
        for key in self.write_locks:
            self.target_table.lock_manager[key].release_write_lock()
        for key in self.insert_locks:
            del self.target_table.lock_manager[key]
        return False


        # # Release read locks
        # [self.target_table.lock_manager[key].release_read_lock() for key in self.read_locks]

        # # Release write locks
        # [self.target_table.lock_manager[key].release_write_lock() for key in self.write_locks]

        # # Remove insert locks
        # [self.target_table.lock_manager.pop(key, None) for key in self.insert_locks]

        # return False

    
    def commit(self):
        # TODO: commit to database
        for query, args in self.queries:
            query(*args)
            if query == Query.delete:
                 del self.target_table.lock_manager[key]
                 if key in self.write_locks:
                     self.insert_locks.remove(key)
                 if key in self.insert_locks:
                     self.insert_locks.remove(key)

        for key in self.read_locks:
            self.target_table.lock_manager[key].release_read_lock()
        for key in self.write_locks:
            self.target_table.lock_manager[key].release_write_lock()
        for key in self.insert_locks:
            self.target_table.lock_manager[key].release_write_lock()
        return True

    