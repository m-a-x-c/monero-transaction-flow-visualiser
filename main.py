import database_manager as db
import update_tx_table as tx
import update_signature_table as sig
import transaction_graph as tg
import requests
import json
import sys


# TO DO:
# - sanity check of user input of tx hash
# - create readme
# - create requirements page
# - store the latest block and transaction index to a new metadata table
# - create a table to store metadata from blocks (e.g. timestamp)

def is_monerod_running():
    print('Checking if monerod is running.')

    url='http://localhost:18081/json_rpc'
    data = {
        "jsonrpc": "2.0",
        "id": "0",
        "method": "get_info"
    }
    headers = {'content-type': 'application/json'}

    try:
        response = requests.post(url, data=json.dumps(data), headers=headers)
        # If the request was successful and we get a valid response, assume monerod is running
        if response.status_code == 200 and "result" in response.json():
            return True
        else:
            return False
    except:
        return False

def monerod_check_loop():
    monerodRunning = is_monerod_running()

    while not monerodRunning:
        response = input("\nmonerod is not running. This program cannot run without monerod.\nPress Enter once you have started monerod.\n")
        monerodRunning = is_monerod_running()
    
    print("monerod is running.")

def user_options():
    while True:
        print('\nEnter an option and press enter:')
        print('[1] Create or update transactions database.')
        print('[2] Create transaction graph form transaction hash.')
        print('[3] Exit program.')
        response = input('Choose option: ').strip()

        try:
            response = int(response)
        except:
            response = 0

        if (response == 1) or (response == 2) or (response == 3):
            break
        else:
            print('Incorrect choice provided. Choose again.')
    
    return response

def graph_limit_user_logic(ask_user=False):
    limit = 200

    if not ask_user:
        return limit
    
    limit = input('Choose the maximum number of nodes to show (max=1000): ')
    limit = limit.strip()

    try:
        limit = int(limit)
    except:
        limit = 200
    
    if limit < 1:
        limit = 1
    
    if limit > 1000:
        limit = 1000
    
    return limit

if __name__ == "__main__":
    UPDATE_DATABASE = 1
    CREATE_TRANSACTION_GRAPH = 2
    EXIT_PROGRAM = 3
    DB_PATH = 'database/output_to_ring_signature.db'

    monerod_check_loop()

    while True:
        user_choice = user_options()

        if user_choice == UPDATE_DATABASE:
            print('')
            db.create_database(DB_PATH)
            db_manager = db.DatabaseManager(DB_PATH)
            tx.update_tx_table(db_manager)
            sig.update_output_tx_pairs_table(db_manager)

            db_manager.close()
        
        elif user_choice == CREATE_TRANSACTION_GRAPH:
            print('')
            db_manager = db.DatabaseManager(DB_PATH)
            hash = input('Enter transcation hash: ')
            hash = hash.strip()
            limit = graph_limit_user_logic()
            # root_hash = 'dea9c3c11cab362db2356e891cb3c8aff07ece7d71aff8a5a12d3e48929c8227'
            root = tg.create_transaction_graph_from_tx_id(hash, db_manager, limit)
            hash_adjacency_list = tg.transaction_graph_to_hash_graph(root)
            tg.visualise_dag(hash_adjacency_list, hash)

            db_manager.close()
        
        elif user_choice == EXIT_PROGRAM:
            print('Exitting program. Good bye.\n')
            sys.exit()