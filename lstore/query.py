from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime
from lstore.config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.index = Index(table)

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        key = self.table.RID_map[primary_key]
        del self.table.RID_map[primary_key]
        del self.table.page_directory[key]
        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        colindx = columns[self.table.key]
        if colindx in self.table.RID_map.keys():
            return False;
        
        self.table.insert_lock.acquire()
        retrive_metadata = self.get_metadata(columns)
        self.table.insert_base_record(retrive_metadata)
        self.table.insert_lock.release()
        return True

    def get_metadata(self, columns):
        schema_encoding = '0' * self.table.num_columns
        indirection = MAX_64BIT_INT
        rid = self.table.records
        time = datetime.now().strftime("%Y%m%d%H%M%S")
        base_id = rid
        metadata = [rid, int(time), schema_encoding, indirection, base_id]
        col = list(columns)
        metadata.extend(col)
        return metadata
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        matched_rids = []

        # Determine how to retrieve the RID(s)
        if search_key_index == self.table.key:
            if search_key in self.table.RID_map.keys():
                matched_rids.append(self.table.RID_map[search_key])
        elif self.table.index.is_indexed(search_key_index):
            matched_rids = self.table.index.locate(search_key_index, search_key)
        else:
            matched_rids = self.table.rid_lookup(search_key_index, search_key)

        if len(matched_rids) == 0:
            return []  # Return empty list if no matching records

        results = []
        for rid in matched_rids:
            retrieved_data = self.table.find_record(rid)
            data_values = retrieved_data[DEFAULT_PAGE_COUNT : DEFAULT_PAGE_COUNT + self.table.num_columns + 1]

            # Check if record has been updated
            if retrieved_data[INDIRECTION_COLUMN] != MAX_64BIT_INT:
                tail_rid = retrieved_data[INDIRECTION_COLUMN]
                tail_record = self.table.find_record(tail_rid)
                updated_values = tail_record[DEFAULT_PAGE_COUNT : DEFAULT_PAGE_COUNT + self.table.num_columns + 1]

                schema_bits = retrieved_data[SCHEMA_ENCODING_COLUMN]
                modified_columns = self.get_updated_columns(schema_bits)

                # Apply updates from tail record
                for idx, modified in enumerate(modified_columns):
                    if modified:
                        data_values[idx] = updated_values[idx]

            # Apply column projection filter
            for idx, should_include in enumerate(projected_columns_index):
                if not should_include:
                    data_values[idx] = None

            # Store retrieved record
            results.append(Record(rid, search_key, data_values))

        return results

    def get_updated_columns(self, schema):
        """Returns a list indicating which columns have been updated based on schema encoding."""
        
        num_columns = self.table.num_columns
        updated_flags = [0] * num_columns

        if schema == 0:
            return updated_flags  # No updates detected

        col_index = num_columns - 1
        while schema:
            updated_flags[col_index] = schema % 10  # Extract least significant digit
            schema //= 10  # Shift encoding to the right
            col_index -= 1  # Move to the next column

        return updated_flags

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        matched_rids = []
        # Determine how to retrieve the RID(s)
        if search_key_index == self.table.key:
            if search_key in self.table.RID_map.keys():
                matched_rids.append(self.table.RID_map[search_key])
        elif self.table.index.is_indexed(search_key_index):
            matched_rids = self.table.index.locate(search_key_index, search_key)
        else:
            matched_rids = self.table.rid_lookup(search_key_index, search_key)

        if len(matched_rids) == 0:
            return []  # Return empty list if no matching records

        results = []
        for rid in matched_rids:
            retrieved_data = self.table.find_record(rid)
            data_values = retrieved_data[DEFAULT_PAGE_COUNT : DEFAULT_PAGE_COUNT + self.table.num_columns + 1]

            # If the record has been updated, traverse the version history
            version_rid = rid  # Default: Base record
            version_count = 0

            while retrieved_data[INDIRECTION_COLUMN] != MAX_64BIT_INT and version_count > relative_version:
                version_rid = retrieved_data[INDIRECTION_COLUMN]  # Move to the latest update
                retrieved_data = self.table.find_record(version_rid)  # Fetch the updated record
                version_count -= 1  # Move back one step

            # Fetch the correct version's values
            data_values = retrieved_data[DEFAULT_PAGE_COUNT : DEFAULT_PAGE_COUNT + self.table.num_columns + 1]

            # Apply column projection
            for idx, should_include in enumerate(projected_columns_index):
                if not should_include:
                    data_values[idx] = None

            # Return a Record object
            results.append(Record(version_rid, search_key, data_values))

        return results

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        columns = list(columns)
        
        if primary_key not in self.table.RID_map.keys():
            return False
        if columns[self.table.key] in self.table.RID_map.keys():
            return False
        if columns[self.table.key] != None:
            return False
        
        self.table.update_lock.acquire()

        rid = self.table.RID_map[primary_key]
        base_id = rid
        address = self.table.page_directory[rid]
        updated_schema = '' 
        latest_rid = self.table.records
        data = self.table.find_record(rid)
        indirection = data[INDIRECTION_COLUMN]
        entry_time = datetime.now().strftime("%Y%m%d%H%M%S")
        if indirection == MAX_64BIT_INT:
            update_chain_rid = rid
            for idx, content in enumerate(columns):
                if content == None:
                    updated_schema += '0'
                    columns[idx] = MAX_64BIT_INT
                else:
                    updated_schema += '1'
        else:
            last_tail_record = self.table.find_record(indirection)
            update_chain_rid = last_tail_record[RID_COLUMN]
            schema = last_tail_record[SCHEMA_ENCODING_COLUMN]
            schema = self.get_updated_columns(schema)
            for idx, content in enumerate(schema):
                if columns[idx] != None:
                    updated_schema += '1'
                else:
                    columns[idx] = last_tail_record[idx + DEFAULT_PAGE_COUNT]
                    updated_schema += '1' if last_tail_record[idx + DEFAULT_PAGE_COUNT] != MAX_64BIT_INT else '0'
        
        # update base record
        self.table.update_value(INDIRECTION_COLUMN, address, latest_rid)
        self.table.update_value(SCHEMA_ENCODING_COLUMN, address, updated_schema)
        
        retrive_metadata = [latest_rid, int(entry_time), updated_schema, update_chain_rid, base_id]
        retrive_metadata.extend(columns)
        self.table.tail_write(retrive_metadata)
        
        self.table.update_lock.release()
        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        sum_result = 0
        data_col_index = aggregate_column_index + DEFAULT_PAGE_COUNT
        for index in range(start_range, end_range + 1):
            if index in self.table.RID_map.keys():
                record_id = self.table.RID_map[index]
                stored_data = self.table.find_record(record_id)
                if stored_data[INDIRECTION_COLUMN] == MAX_64BIT_INT:
                    sum_result += stored_data[data_col_index]
                else:
                    modified_rid = stored_data[INDIRECTION_COLUMN]
                    tail_version = self.table.find_record(modified_rid)
                    new_column_value = tail_version[DEFAULT_PAGE_COUNT:DEFAULT_PAGE_COUNT + self.table.num_columns + 1]
                    schema = tail_version[SCHEMA_ENCODING_COLUMN]
                    schema = self.get_updated_columns(schema)
                    if schema[aggregate_column_index] == 1:
                        sum_result += new_column_value[aggregate_column_index]
                    else:
                        sum_result += stored_data[data_col_index]

        return sum_result

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
