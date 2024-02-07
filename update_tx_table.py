import requests
import json
import time


def get_blockchain_height():
    url = "http://localhost:18081/json_rpc"
    data = {
        "jsonrpc": "2.0",
        "id": "0",
        "method": "get_block_count"
    }
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, data=json.dumps(data))
    latest_block_height = response.json().get("result", {}).get("count", None)
    # note that this is height, but index goes from 0 to height-1

    return latest_block_height


def update_tx_table(db_manager):
    print('--------------------------------')
    print('Updating tx table.')
    print('This might take a while.\n')
    # print('Getting number of rows in transaction table.')
    # print(f'Size of transactions table is {db_manager.get_table_row_count("tx")} rows.')

    last_block_compelted = db_manager.get_largest_block_value_from_tx_table() # number between 0 and height-1
    last_block_compelted = last_block_compelted if last_block_compelted is not None else -1
    blockchain_height = get_blockchain_height() # note that height is +1 above the largest index

    print(f'Last Block Completed: {last_block_compelted}')
    print(f'Last Block Height: {blockchain_height}')
    print('')

    tx_hashes = []
    total_hashes_saved = 0
    start = time.time()

    for block_idx in range((last_block_compelted+1), blockchain_height):
    # for block_idx in [3074780]:
        
        if block_idx <= 100_000:
            n_txs_per_save = 1000
        elif block_idx <= 1_000_000:
            n_txs_per_save = 10_000
        elif block_idx <= 2_500_000:
            n_txs_per_save = 100_000
        else:
            n_txs_per_save = 300_000

        headers = {'content-type': 'application/json'}
        url = "http://127.0.0.1:18081/json_rpc"
        payload = {
            "jsonrpc": "2.0",
            "id": "0",
            "method": "get_block",
            "params": {
                "height": block_idx
            }
        }

        # Send the request to the Monero daemon
        while True:
            try:
                response = requests.post(url, data=json.dumps(payload), headers=headers)
                break
            except KeyboardInterrupt:
                print('Operation interrupted by user.')
                raise
            except Exception as e:
                print(f'Request error encountered. Waiting 1 second.')
                time.sleep(1)
        
        result = response.json()

        # Check if the block data is available and extract transaction hashes
        tx_hashes_temp = []
        if result.get('result'):
            tx_hashes_temp.append(result['result']['miner_tx_hash'])

            if 'tx_hashes' in result['result']:
                tx_hashes_temp.extend(result['result']['tx_hashes'])
        
        if tx_hashes_temp:
            tx_hashes.extend([(bytes.fromhex(tx_hash), block_idx) for tx_hash in tx_hashes_temp])
        
        
        if len(tx_hashes) >= n_txs_per_save:
            db_manager.add_transactions(tx_hashes)
            total_hashes_saved += len(tx_hashes)
            tx_hashes = []
            # print(f'Size of transactions table is {db_manager.get_table_row_count("tx")} rows.')
            
        
        if block_idx % 100 == 0:
            block_duration = round((time.time() - start), 2)
            print(f'Completed={round(((block_idx+1)/blockchain_height)*100, 2)}% | Time per {100} Blocks={block_duration}s | Block={block_idx} | Buffer Size={len(tx_hashes)}')
            start = time.time()


    if len(tx_hashes) > 0:
        db_manager.add_transactions(tx_hashes)
        total_hashes_saved += len(tx_hashes)
        tx_hashes = []
        # print(f'Size of transactions table is {db_manager.get_table_row_count("tx")} rows.')
    
    print('--------------------------------\n')