import sqlite3
import os


def create_database(db_path):

    if not os.path.exists(db_path):
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        
        # Create a cursor object using the cursor method of the connection
        cursor = conn.cursor()
        
        # Create table 'tx' if it doesn't exist with columns: 'key' (integer primary key autoincrement), 
        # 'hash' (BLOB unique), and 'block' (integer)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tx (
            key INTEGER PRIMARY KEY AUTOINCREMENT,
            hash BLOB UNIQUE,
            block INTEGER
        );
        """)
        
        # Create table 'signature' if it doesn't exist with columns: 'output' (integer), 'tx_key' (integer),
        # set 'tx_key' as a foreign key referencing 'key' from the 'tx' table.
        # The primary key is a composite of 'output' and 'tx_key'.
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS signature (
            output INTEGER,
            tx_key INTEGER,
            FOREIGN KEY(tx_key) REFERENCES tx(key),
            PRIMARY KEY (output, tx_key)
        );
        """)

        # Add index to output column of signature table to make it easy to lookup outputs.
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_signature_output_desc
            ON signature(output DESC)
        """)
        # Commit the changes and close the connection
        conn.commit()
        conn.close()
    
    print('Database and table creation or verification complete.')


class DatabaseManager:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute('PRAGMA foreign_keys = ON')
        self.conn.commit()
        print('Database manager started.')

    def add_transactions(self, transactions):
        # transactions = [(b'\x00\x01\x02', 1), (b'\x03\x04\x05', 2)]
        
        # Prepare the SQL statement for inserting a new transaction
        sql_insert_transaction = """
        INSERT INTO tx (hash, block) VALUES (?, ?);
        """
        # Execute the SQL statement for each transaction in the transactions list
        cursor = self.conn.cursor()
        try:
            cursor.executemany(sql_insert_transaction, transactions)
            self.conn.commit()
            print(f'SAVED {len(transactions)} TRANSACTIONS to DATABASE.')
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()
        finally:
            cursor.close()


    def add_output_tx_pair(self, output_tx_pairs):
        # Prepare the SQL statement for inserting a new output-transaction pair
        sql_insert_output_tx_pair = """
        INSERT INTO signature (output, tx_key) VALUES (?, ?);
        """
        cursor = self.conn.cursor()
        try:
            cursor.executemany(sql_insert_output_tx_pair, output_tx_pairs)
            self.conn.commit()
            print(f'SAVED {len(output_tx_pairs)} OUTPUT-TX PAIRS to DATABASE.')
        except sqlite3.IntegrityError as e:
            print(f"A foreign key constraint failed: {e}")
            self.conn.rollback()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
            
    def retrieve_tx_keys_in_range(self, start, end):
        """
        Retrieves rows from the 'tx' table where 'key' is within the specified range.
        
        Parameters:
        start (int): The start of the range (inclusive).
        end (int): The end of the range (inclusive).
        
        Returns:
        list: A list of keys from the 'tx' table within the given range, ordered in descending order.
        """
        query = """
        SELECT * FROM tx
        WHERE key BETWEEN ? AND ?
        ORDER BY key DESC
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (start, end))
        rows = cursor.fetchall()[::-1]
        return rows
    
    def find_transaction_by_hash(self, hash_value):
        """
        Searches for a transaction in the 'tx' table with the given hash.

        Parameters:
        hash_value (bytes): The hash of the transaction to search for.

        Returns:
        tuple: The transaction details if found, else None.
        """
        query = """
        SELECT * FROM tx
        WHERE hash = ?
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (hash_value,))
        row = cursor.fetchone()
        cursor.close()
        return row

    

    def get_largest_block_value_from_tx_table(self):
        sql_query = "SELECT MAX(block) FROM tx;"
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_query)
            max_block = cursor.fetchone()[0]  # fetchone() returns a tuple, [0] gets the first element
            return max_block
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return None
        finally:
            cursor.close()

    def get_largest_key_from_tx_table(self):
        sql_query = "SELECT MAX(key) FROM tx;"
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_query)
            max_block = cursor.fetchone()[0]  # fetchone() returns a tuple, [0] gets the first element
            return max_block
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return None
        finally:
            cursor.close()

    def get_largest_tx_value_from_signature_table(self):
        sql_query = "SELECT MAX(tx_key) FROM signature;"
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_query)
            max_block = cursor.fetchone()[0]  # fetchone() returns a tuple, [0] gets the first element
            return max_block
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return None
        finally:
            cursor.close()
            
    def get_table_row_count(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def print_first_100_transactions(self, table):
        cursor = self.conn.cursor()
        fetch_query = f'SELECT * FROM {table} LIMIT 100'
        cursor.execute(fetch_query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)

    def find_hashes_by_output(self, output_value):
        # Prepare the SQL query to find all rows in 'signature' with the given output,
        # and join with the 'tx' table to get the corresponding hash blobs.
        query = """
        SELECT tx.hash
        FROM signature
        INNER JOIN tx ON signature.tx_key = tx.key
        WHERE signature.output = ?
        """
        
        # Execute the SQL query, passing the output_value as a parameter to ensure safe querying (prevents SQL injection).
        cursor = self.conn.cursor()
        cursor.execute(query, (output_value,))
        
        # Fetch all matching rows.
        rows = cursor.fetchall()
        
        # Process the rows to extract just the hash blobs.
        hashes = [row[0].hex() for row in rows]
        
        return hashes
    
    def get_latest_block_completed(self):
        pass
        
    def close(self):
        self.conn.close()