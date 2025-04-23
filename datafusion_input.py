from fractions import Fraction

from partial_replication import TPCH, set_column_sizes, sigmod_greedy
from setup_benchmark import get_replica_configurations, add_implicitly_executable_queries


def get_allocation(number_of_nodes, query_costs):
    benchmark = TPCH()

    for i, line in enumerate(query_costs):
        benchmark._queries[i]._load_time = int(float(line) * 1000 * 1000)
    sum_load_time = sum([q._load_time for q in benchmark._queries])
    for q in benchmark._queries:
        q._load = Fraction(q._load_time, sum_load_time)

    for table in benchmark._tables:
        for column in table._columns:
            column._size = 1

    query_selection = []
    for i, q in enumerate(benchmark._queries):
        if q._load != 0:
            query_selection.append(i)

    benchmark._queries = [benchmark._queries[i] for i in query_selection]

    backend_configs = sigmod_greedy(benchmark, number_of_nodes)
    translated_backend_configs = []
    for backend in backend_configs:
        b = {}
        for query_id, query_share in backend.items():
            b[query_selection[query_id]] = query_share
        translated_backend_configs.append(b)

    add_implicitly_executable_queries(benchmark, translated_backend_configs)

    return translated_backend_configs


def datafusion_input():
    benchmark = TPCH()
    load_file = 'tpch/costs_fusion_pi_25.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)

    for number_of_nodes in range(1, 21):
        allocation_greedy = get_replica_configurations(benchmark, number_of_nodes, load_file, 'greedy')
        print("Number of nodes: ", number_of_nodes)
        for node_id, allocation in enumerate(allocation_greedy):
            a = sorted(list(allocation.keys()))
            print(f'  {node_id}:\t {[x+1 for x in a]}')
        print()


def main():
    datafusion_input()

    query_costs = [4.622320077510085, 0.511261371418368, 1.6671003796975128, 0.9821360152331181, 1.8962063617887908,
                   1.2496983020611834, 3.264654403482564, 1.8184883677167818, 3.2468853204045445, 2.1164685924188236,
                   0.5018533184775151, 1.56585509175571, 1.551166107207991, 1.1000117797171698, 2.134484094113577,
                   0.32447741826763377, 3.717839312425349, 4.70177540841978, 2.658032559044659, 1.6555288535426371,
                   4.270606431132182, 0.3403742035501637]

    for number_of_nodes in range(1, 21):
        allocation = get_allocation(number_of_nodes, query_costs)
        print("Number of nodes: ", number_of_nodes)
        for node_id, allocation in enumerate(allocation):
            a = sorted(list(allocation.keys()))
            print(f'  {node_id}:\t {[x + 1 for x in a]}')
        print()


if __name__ == '__main__':
    main()
