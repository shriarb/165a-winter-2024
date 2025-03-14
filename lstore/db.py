from lstore.table import Table
from lstore.bufferpool import BufferPool
import os
import pickle

class Database():

    def __init__(self):
        self.tables = {}
        self.directory_path = ""

    # Not required for milestone1
    def open(self, path):
        self.directory_path = path
        BufferPool().set_storage_path(self.directory_path)
        
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            path = os.path.join(self.directory_path, "db_catalog.pkl")
            file = open(path, 'r+b')
            t_meta = pickle.load(file)
            file.close()
            for name in t_meta:
                metadata = t_meta[name]
                table = self.create_table(metadata[0], metadata[1], metadata[2])
                table.page_directory = metadata[3]
                table.records = metadata[4]
                table.updates = metadata[5]
                table.RID_map = metadata[6]
                table.path = self.directory_path


    def close(self):
        metadata = {}
        for table in self.tables.values():
            metadata[table.name] = [table.name, table.num_columns, table.key, table.page_directory, table.records]
            metadata[table.name].append(table.updates)
            metadata[table.name].append(table.RID_map)
        metadata_file = os.path.join(self.directory_path, "db_catalog.pkl")
        file = open(metadata_file, 'w+b')
        pickle.dump(metadata, file)
        file.close()

        # with open(metadata_file, "wb") as file:
        #     pickle.dump(metadata, file)
        BufferPool.shutdown()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables.keys():
            print(f"table {name} already exist in the database")
        else:
            table = Table(name, num_columns, key_index)
            table.set_path(os.path.join(self.directory_path, table.name))
            self.tables[name] = table;
            return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables.keys():
            del self.tables[name]
        else:
            print(f"table {name} does not exist")


    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if name in self.tables.keys():
            return self.tables[name]
        else:
            print(f"table {name} does not exist")
