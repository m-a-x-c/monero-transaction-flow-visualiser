import requests
import json
import time
import database_manager as db
import networkx as nx
import matplotlib.pyplot as plt
from collections import deque


class TransactionNode:
    def __init__(self, transaction):
        self.tx_id = transaction['tx_id']
        self.outputs = transaction['outputs']
        self.transaction_full = transaction['full']
        self.children = []

    def add_child(self, node):
        self.children.append(node)

    def print_tree(self):
        print(f'Root: {self.tx_id}')
        for child in self.children:
            child.print_tree()


def get_transaction(tx_hash):
    headers = {'content-type': 'application/json'}
    url = "http://127.0.0.1:18081/get_transactions"
    payload = {
        "txs_hashes": [tx_hash],
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
            

    outputs = response.json()['txs'][0]['output_indices']

    # pretty_json = json.dumps(data, indent=4)
    # print(pretty_json)

    data = {
        'tx_id' : tx_hash,
        'outputs' : outputs,
        'full' : response.json()['txs'][0]
    }

    return data;


def transaction_graph_to_hash_graph(root_node):
    graph = {}
    nodes_to_check = [root_node]

    while nodes_to_check:
        transaction = nodes_to_check.pop(0)

        graph[transaction.tx_id] = []

        for child in transaction.children:
            graph[transaction.tx_id].append(child.tx_id)
            nodes_to_check.append(child)
    
    return graph


def create_transaction_graph_from_tx_id(tx_id, db_manager, limit):

    transaction = get_transaction(tx_id) # dictionary

    outputs_to_check = [] # queue

    transaction_dag_root = TransactionNode(transaction)

    output_to_transaction_in_dag = {}

    for output in transaction['outputs']:
        outputs_to_check.append(output)
        output_to_transaction_in_dag[output] = transaction_dag_root

    while outputs_to_check:
        # print(f'size of outputs={len(outputs_to_check)}')

        output = outputs_to_check.pop(0)

        current_node = output_to_transaction_in_dag[output]

        # transactions = get_transactions_where_output_is_used_in_ring(output)
        transactions = db_manager.find_hashes_by_output(output)

        for tx_id in transactions:
            transaction = get_transaction(tx_id)
            
            node = TransactionNode(transaction)
            current_node.add_child(node)
            
            for output in transaction['outputs']:
                outputs_to_check.append(output)
                output_to_transaction_in_dag[output] = node

        if len(outputs_to_check) > limit:
            break

    return transaction_dag_root


def visualise_dag(adjacency_list, tx_hash):
    G = nx.DiGraph()
    for source, targets in adjacency_list.items():
        for target in targets:
            G.add_edge(source, target)

    def determine_levels(graph):
        levels = {}
        level = 0
        queue = deque([node for node in graph.nodes() if graph.in_degree(node) == 0])
        while queue:
            level_size = len(queue)
            for _ in range(level_size):
                node = queue.popleft()
                levels[node] = level
                for successor in graph.successors(node):
                    if successor not in levels:
                        queue.append(successor)
            level += 1
        return levels

    levels = determine_levels(G)
    max_level = max(levels.values())

    # Define a list of distinct colors for each level.
    colors = plt.cm.get_cmap('viridis_r', max_level + 1)

    node_colors = {node: colors(levels[node]) for node in G.nodes()}
    labels = {node: f'{node[:3]}...' for node in G.nodes()}

    plt.figure(figsize=(15, 10))
    pos = nx.spring_layout(G)

    # Draw nodes with level-based colors and adjust opacity.
    nx.draw_networkx_nodes(G, pos, node_size=2000, node_color=[node_colors[node] for node in G.nodes()], alpha=0.5)

    for source, targets in adjacency_list.items():
        for target in targets:
            nx.draw_networkx_edges(G, pos, edgelist=[(source, target)], width=2, edge_color=[node_colors[source]],
                                   arrows=True, arrowstyle='-|>', arrowsize=10, connectionstyle="arc3,rad=0.1")

    # Draw labels
    for node, (x, y) in pos.items():
        plt.text(x, y, labels[node], fontsize=9, ha='center', va='center', fontweight='bold', color='white', alpha=1)

    # Create a legend for the levels
    # Generate patches for the legend
    from matplotlib.patches import Patch
    legend_patches = [Patch(color=colors(i), label=f'Level {i}') for i in range(max_level + 1)]
    plt.legend(handles=legend_patches, title="Node Levels", loc='best')

    plt.title(f'Monero Transaction Graph\nTX: {tx_hash}', fontweight='bold')
    plt.axis('off')
    plt.show()