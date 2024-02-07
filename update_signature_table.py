import requests
import json
import time

def relative_to_absolute(relative_indices):
    absolute_indices = []
    running_total = 0
    for offset in relative_indices:
        running_total += offset
        absolute_indices.append(running_total)
    return absolute_indices


def update_output_tx_pairs_table(db_manager):
    print('--------------------------------')
    print('Updating signature table.')
    print('This might take a while.\n')


    txs_per_commit = 1000

    
    print('Getting index of most recently saved transaction.')
    most_recent_tx_saved_in_signature_table = db_manager.get_largest_tx_value_from_signature_table()
    most_recent_tx_saved_in_signature_table = most_recent_tx_saved_in_signature_table if most_recent_tx_saved_in_signature_table is not None else 0
    print(f'Index of most recent transaction whose ring members were logged: {most_recent_tx_saved_in_signature_table}')

    largest_tx_key = db_manager.get_largest_key_from_tx_table()
    print(f'Largest key in tx table: {largest_tx_key}\n')

    start = most_recent_tx_saved_in_signature_table + 1
    end = start + txs_per_commit - 1
    
    while start <= largest_tx_key:
        print(f'Trying transactions from indicies {start} to {end}')

        txs = db_manager.retrieve_tx_keys_in_range(start, end)
        tx_indicies = [key for key, _, _ in txs]
        tx_hashes = [hash.hex() for _, hash, _ in txs]

        # for idx, tx_hashes in zip(tx_indicies, tx_hashes):
        #     print(idx, tx_hashes)
        
        headers = {'content-type': 'application/json'}
        url = "http://127.0.0.1:18081/get_transactions"
        payload = {
            "txs_hashes": tx_hashes,
            "decode_as_json": True
        }

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
                
        
        transactions = response.json()['txs']
        print(f'{len(transactions)} transactions retrived.')

        # pretty_json = json.dumps(transactions, indent=4)
        # print(pretty_json)

        output_tx_data = []

        for idx, transaction in zip(tx_indicies, transactions):
            # print(hash)
            ring_members = []
            for tx_input in json.loads(transaction['as_json'])['vin']:
                if 'key' in tx_input:
                    relative_offsets = tx_input['key']['key_offsets']
                    absolute_offsets = relative_to_absolute(relative_offsets)
                else:
                    absolute_offsets = []

                ring_members.extend(absolute_offsets)
        
            ring_members = list(set(ring_members))

            for member in ring_members:
                output_tx_data.append((member, idx))

        
        print(f'Number of output transaction pairs: {len(output_tx_data)}')
        # for each in output_tx_data:
        #     print(each)

        db_manager.add_output_tx_pair(output_tx_data)

        
        start = end + 1
        end = start + txs_per_commit - 1

    print('--------------------------------\n')