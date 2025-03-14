from lstore.page import Page
from BTrees.OOBTree import OOBTree

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None for _ in range(table.num_columns)]
        self.table = table
        self.key = table.key
        self.column_lookup = {}

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        index = self.indices[column]
        if not index.has_key(value):
            return []
        return index[value]

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        sublist = []
        index = self.indices[column]
        for list1 in list(index.values(min=begin, max=end)):
            sublist += list1
        return sublist

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = OOBTree()

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None


    def is_indexed(self, column):
        return self.indices[column] == None
    
    def insert(self, columns, rid):
        for index in range(1, self.table.num_columns):
            if self.indices[index] == None:
                self.create_index(index)
            if not self.indices[index].has_key(columns[index]):
                self.indices[index][columns[index]]= [rid]
            else:
                self.indices[index][columns[index]].append(rid)
            self.column_lookup[columns[index]] = index

