import copy
import glob
import itertools
import json
import math
import os
from fractions import Fraction

from partial_replication import Query, Accounting, TPCH, TPCDS, sigmod_greedy, set_column_sizes, Benchmark_accessed_columns_queries


def hungarian_configuration(benchmark, config1, config2):
    assert len(config1) == len(config2)
    matrix = []
    for config1_allocation in config1:
        allocation1_columns = Benchmark_accessed_columns_queries(benchmark, config1_allocation.keys())
        row = []
        for config2_allocation in config2:
            allocation2_columns = Benchmark_accessed_columns_queries(benchmark, config2_allocation.keys())
            additional_columns = allocation2_columns - allocation1_columns
            row.append(sum([column.size() for column in additional_columns]))
        matrix.append(row)
    return matrix


def get_replica_configurations_greedy(input_benchmark, num_backends, load_file, robust=False, version='default', failure_node=None, reallocation=None):
    # we need to create a new benchmark instance, because it is modified
    if input_benchmark.name == 'tpch':
        benchmark = TPCH()

        if len(input_benchmark._updates) != 0:
            columns_per_table = {}
            for table in benchmark._tables:
                columns_per_table[table._name] = table._columns

            u1 = Query(1, columns_per_table['LINEITEM'])
            u1._load = Fraction(9, 90)

            u2 = Query(2, columns_per_table['ORDERS'])
            u2._load = Fraction(1, 90)

            benchmark._updates = [u1, u2]
    elif input_benchmark.name == 'tpcds':
        benchmark = TPCDS()

        if len(input_benchmark._updates) != 0:
            columns_per_table = {}
            for table in benchmark._tables:
                columns_per_table[table._name] = table._columns

            update_information = [
                ('CATALOG_SALES', 20),
                ('CATALOG_RETURNS', 2),
                ('INVENTORY', 100),
                ('STORE_SALES', 40),
                ('STORE_RETURNS', 4),
                ('WEB_SALES', 10),
                ('WEB_RETURNS', 1),
            ]
            overall_update_costs = sum([costs for _, costs in update_information])

            for i, update in enumerate(update_information):
                query = Query(i + 1, columns_per_table[update[0]])
                query._load = Fraction(update[1], overall_update_costs) * Fraction(1, 9)
                benchmark._updates.append(query)
    else:
        assert(input_benchmark.name == 'accounting')
        benchmark = Accounting()
    columns_per_table = {}
    for table in benchmark._tables:
        columns_per_table[table._name] = table._columns

    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)
    query_selection = []
    for i, q in enumerate(benchmark._queries):
        if q._load != 0:
            query_selection.append(i)

    if load_file == 'accounting/query_costs_73.txt':
        query_selection = list(range(73))

    benchmark._queries = [benchmark._queries[i] for i in query_selection]

    backend_configs = sigmod_greedy(benchmark, num_backends)
    translated_backend_configs = []
    for backend in backend_configs:
        b = {}
        for query_id, query_share in backend.items():
            b[query_selection[query_id]] = query_share
        translated_backend_configs.append(b)

    if robust:
        if version == 'default':
            def query_accessed_size(query):
                accessed_size = 0
                for column in query._columns:
                    accessed_size += column.size()
                return accessed_size
            benchmark_queries_sorted_by_accessed_size = sorted(benchmark._queries, key=query_accessed_size, reverse=True)
            for query in benchmark_queries_sorted_by_accessed_size:
                differences = []
                for backend_id, backend in enumerate(translated_backend_configs):
                    difference = 0
                    backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
                    for column in query._columns:
                        if column not in backend_accessed_columns:
                            difference += column.size()
                    differences.append((backend_id, difference))
                    # explicitly mark query as executable if all required fragments are stored
                    if difference == 0 and (query._nr - 1) not in translated_backend_configs[backend_id]:
                        translated_backend_configs[backend_id][query._nr - 1] = 0
                differences.sort(key=lambda item: item[1])
                # query must be executable at one node
                assert(differences[0][1] == 0)
                # assign query only if it is not already executable at two nodes
                if differences[1][1] != 0:
                    translated_backend_configs[differences[1][0]][query._nr - 1] = 0

        else:
            assert version == 'chained'
            # start chaining with largest backend
            max_data = None
            max_data_backend_id = None
            for backend_id, backend in enumerate(translated_backend_configs):
                backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
                backend_accessed_size = sum([c.size() for c in backend_accessed_columns])
                if max_data_backend_id is None or backend_accessed_size > max_data:
                    max_data_backend_id = backend_id
                    max_data = backend_accessed_size

            backends_to_chain = set(range(len(translated_backend_configs)))
            backends_to_chain.remove(max_data_backend_id)
            current_backend_id = max_data_backend_id

            # chain all backends
            while len(backends_to_chain) > 0:
                current_backend = translated_backend_configs[current_backend_id]
                current_backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, current_backend.keys())
                min_data = None
                min_data_backend_id = None
                for backend_id in backends_to_chain:
                    backend = translated_backend_configs[backend_id]
                    required_data = 0
                    backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
                    for column in current_backend_accessed_columns:
                        if column not in backend_accessed_columns:
                            required_data += column.size()
                    if min_data_backend_id is None or required_data < min_data:
                        min_data_backend_id = backend_id
                        min_data = required_data

                # add required data via queries
                for query_id in current_backend.keys():
                    if translated_backend_configs[current_backend_id][query_id] == 0:
                        # query is not regularly assigned and, thus, do not have to be considered
                        continue
                    if query_id not in translated_backend_configs[min_data_backend_id]:
                        translated_backend_configs[min_data_backend_id][query_id] = 0
                backends_to_chain.remove(min_data_backend_id)
                current_backend_id = min_data_backend_id

            # complete chain to a ring with the largest backend
            current_backend = translated_backend_configs[current_backend_id]
            for query_id in current_backend.keys():
                if query_id not in translated_backend_configs[max_data_backend_id]:
                    translated_backend_configs[max_data_backend_id][query_id] = 0

    # only required for non robust setups
    add_implicitly_executable_queries(benchmark, translated_backend_configs)

    if failure_node is not None:
        # failure node is not allowed to execute queries -> clear assigned query shares
        translated_backend_configs[failure_node] = {}

    return translated_backend_configs


def add_implicitly_executable_queries(benchmark, replica_configurations):
    for replica_id, replica in enumerate(replica_configurations):
        backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, replica.keys())
        for query in benchmark._queries:
            if query._load == 0:
                continue
            query_is_executable = True
            for column in query._columns:
                if column not in backend_accessed_columns:
                    query_is_executable = False
                    break
            if query_is_executable and (query._nr - 1) not in replica_configurations[replica_id]:
                replica_configurations[replica_id][query._nr - 1] = 0
                # print(len(replica_configurations), replica_id, query._nr - 1)


def add_robustness_with_chaining(benchmark, replica_configurations):
    # start chaining with largest backend
    max_data = None
    max_data_backend_id = None
    for backend_id, backend in enumerate(replica_configurations):
        backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
        backend_accessed_size = sum([c.size() for c in backend_accessed_columns])
        if max_data_backend_id is None or backend_accessed_size > max_data:
            max_data_backend_id = backend_id
            max_data = backend_accessed_size

    backends_to_chain = set(range(len(replica_configurations)))
    backends_to_chain.remove(max_data_backend_id)
    current_backend_id = max_data_backend_id

    # chain all backends
    while len(backends_to_chain) > 0:
        current_backend = replica_configurations[current_backend_id]
        current_backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, current_backend.keys())
        min_data = None
        min_data_backend_id = None
        for backend_id in backends_to_chain:
            backend = replica_configurations[backend_id]
            required_data = 0
            backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
            for column in current_backend_accessed_columns:
                if column not in backend_accessed_columns:
                    required_data += column.size()
            if min_data_backend_id is None or required_data < min_data:
                min_data_backend_id = backend_id
                min_data = required_data

        # add required data via queries
        for query_id in current_backend.keys():
            if replica_configurations[current_backend_id][query_id] == 0:
                # query is not regularly assigned and, thus, do not have to be considered
                continue
            if query_id not in replica_configurations[min_data_backend_id]:
                replica_configurations[min_data_backend_id][query_id] = 0
        backends_to_chain.remove(min_data_backend_id)
        current_backend_id = min_data_backend_id

    # complete chain to a ring with the largest backend
    current_backend = replica_configurations[current_backend_id]
    for query_id in current_backend.keys():
        if query_id not in replica_configurations[max_data_backend_id]:
            replica_configurations[max_data_backend_id][query_id] = 0

    return replica_configurations


def get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='decomposition',
                                                 robust=False, failure_node=None, reallocation=None, data_modifications=None,
                                                 optimality_gap=None, time_limit=None, clustered_queries=None):
    if benchmark.name != 'accounting':
        benchmark.add_load(load_file)
    if benchmark.name in ['accounting', 'tpch', 'tpcds']:
        folder = load_file.split('/')[-1].split('.')[0] + '/'
    if version == 'decomposition':
        folder += 'decomposition'
    elif version == 'optimal':
        folder += 'optimal'
    elif version == 'partial_clustering':
        assert clustered_queries is not None
        folder += 'partial_clustering'
    elif version == 'full_clustering':
        assert clustered_queries is None
        folder += 'full_clustering'
    elif version == 'two-step':
        folder += 'two-step'
    elif version == 'three-step':
        folder += 'three-step'
    elif version == 'optimality_gap':
        assert optimality_gap is not None
        folder += 'optimality_gap'
    elif version == 'time_limit':
        assert time_limit is not None
        folder += 'time_limit'
    else:
        assert False, f'Unsupported version: {version}'
    if robust:
        assert reallocation is None, 'Currently support only robustness against node failures OR reallocation'
        folder += '/robust'
    elif reallocation:
        assert failure_node is None
        reallocation_goal, reallocation_approach = reallocation
        assert reallocation_goal in ['min_realloc', 'no_realloc']
        assert reallocation_approach in ['add_all', 'add_last', 'optimal']
        folder += f'/reallocation/{reallocation_goal}/{reallocation_approach}'
    elif data_modifications:
        folder += '/data_modifications'

    if robust:
        suffix = '_robust'
    elif reallocation:
        suffix = '_reallocation'
    else:
        suffix = ''

    if version == 'optimality_gap':
        suffix += f'_gap{optimality_gap}'
    elif version == 'time_limit':
        suffix += f'_time{time_limit}'

    search_string = os.path.join(os.path.dirname(__file__), f'{benchmark.name}/{folder}/sol_K{num_backends}_*_out{suffix}.txt')
    file_names = glob.glob(search_string)
    assert len(file_names) == 1, f'Files: {file_names} for: {benchmark.name}, version={version}, robust={robust}, {num_backends}\nin {search_string}'
    replica_configurations = [{} for _ in range(num_backends)]

    section_nr = 1
    if robust or reallocation:
        section_nr = 7
        if failure_node is not None:
            section_nr = 9 + 2 * failure_node

    with open(file_names[0]) as f:
        file_txt = f.read()
        sections = file_txt.split('\n\n')
        config_txt = sections[section_nr]
        lines = config_txt.split('\n')
        for line in lines[1:]:
            query_id = int(line.split()[0]) - 1
            ratios = [int(round(float(ratio), 4) * 1000) for ratio in line.split()[1:]]
            sum_ratios = sum(ratios)
            ratios = [Fraction(ratio, sum_ratios) if ratio != 0 else 0 for ratio in ratios]
            if robust and failure_node is not None:
                assert(len(ratios) == num_backends - 1)
            else:
                assert (len(ratios) == num_backends)
            assert(sum(ratios) == 1), f'{benchmark.name} K={num_backends}, {version}, robust={robust}, failure_node={failure_node}, query_id={query_id}'
            for replica_id, ratio in enumerate(ratios):
                if failure_node is not None and replica_id >= failure_node:
                    replica_id += 1
                if ratio != 0:
                    replica_configurations[replica_id][query_id] = ratio
        if robust or reallocation:
            query_feasibility_txt = sections[5]
            lines = query_feasibility_txt.split('\n')
            for line in lines[1:]:
                query_id = int(line.split()[0]) - 1
                feasibilities = [int(feasibility) for feasibility in line.split()[1:]]
                for replica_id, feasibility in enumerate(feasibilities):
                    if replica_id == failure_node:
                        continue
                    if feasibility == 1 and query_id not in replica_configurations[replica_id]:
                        replica_configurations[replica_id][query_id] = 0

    add_implicitly_executable_queries(benchmark, replica_configurations)
    if failure_node is not None:
        assert len(replica_configurations[failure_node]) == 0
    return replica_configurations


def get_replica_configurations(benchmark, num_backends, load_file, allocation, robust=False, failure_node=None, reallocation=None, optimality_gap=None, time_limit=None, clustered_queries=None, data_modifications=None):
    if failure_node is not None:
        assert failure_node < num_backends
    if allocation == 'greedy':
        return get_replica_configurations_greedy(benchmark, num_backends, load_file, robust=robust, version='default', failure_node=failure_node, reallocation=reallocation)
    elif allocation == 'chained':
        assert robust is True
        assert  reallocation is None
        return get_replica_configurations_greedy(benchmark, num_backends, load_file, robust=robust, version='chained', failure_node=failure_node)
    elif allocation == 'decomposition':
        assert reallocation is None
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='decomposition', robust=robust, failure_node=failure_node)
    elif allocation == 'chained_decomposition':
        assert robust is True
        assert reallocation is None
        allocation = get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='decomposition', robust=False, failure_node=failure_node)
        return add_robustness_with_chaining(benchmark, allocation)
    elif allocation == 'optimal':
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='optimal', robust=robust, failure_node=failure_node, reallocation=reallocation, data_modifications=data_modifications)
    elif allocation == 'three-step':
        assert robust is True
        assert reallocation is None
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='three-step', robust=robust, failure_node=failure_node)
    elif allocation == 'two-step':
        assert robust is True
        assert reallocation is None
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='two-step', robust=robust, failure_node=failure_node)
    elif allocation == 'partial_clustering':
        assert robust is False
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='partial_clustering', robust=robust, failure_node=failure_node, reallocation=reallocation, data_modifications=data_modifications, optimality_gap=optimality_gap, clustered_queries=clustered_queries)
    elif allocation == 'full_clustering':
        assert robust is False
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='full_clustering', robust=robust, failure_node=failure_node, reallocation=reallocation, data_modifications=data_modifications, optimality_gap=optimality_gap, clustered_queries=clustered_queries)
    elif allocation == 'optimality_gap':
        assert robust is False
        assert reallocation is None
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='optimality_gap', robust=robust, failure_node=failure_node, reallocation=reallocation, data_modifications=data_modifications, optimality_gap=optimality_gap)
    elif allocation == 'time_limit':
        return get_replica_configurations_decomposition(benchmark, num_backends, load_file, version='time_limit', robust=robust, failure_node=failure_node, reallocation=reallocation, data_modifications=data_modifications, time_limit=time_limit)
    else:
        raise Exception(f'Unsupported allocation: {allocation}')
