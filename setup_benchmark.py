import fractions
import glob
import os
import threading
import sys

from partial_replication import TPCH, sigmod_greedy, set_column_sizes, Benchmark_accessed_columns_queries


def get_replica_configurations_rabl(benchmark, num_backends, load_file, robust=False, version='default', failure_node=None):
    # we need to create a new benchmark instance, because it is modified
    if benchmark.name == 'tpch':
        benchmark = TPCH()
    elif benchmark.name == 'tpcds':
        benchmark = TPCDS()
    else:
        assert(benchmark.name == 'acdoca')
        benchmark = ACDOCA()
    benchmark.add_load(load_file)
    query_selection = []
    for i, q in enumerate(benchmark._queries):
        if q._load != 0:
            query_selection.append(i)
    if benchmark.name == 'acdoca':
        benchmark._queries = sorted(benchmark._queries, key=lambda query: query._load, reverse=True)
        for i in range(len(benchmark._queries)):
            benchmark._queries[i]._nr = i + 1
        if load_file == 'acdoca/query_costs_73.txt':
            query_selection = list(range(73))
    # print(query_selection)
    benchmark._queries = [benchmark._queries[i] for i in query_selection]

    if benchmark.name in ['tpch', 'tpcds']:
        set_column_sizes(benchmark._tables, load_file)

    backend_configs = sigmod_greedy(benchmark, num_backends)

    translated_backend_configs = []
    for backend in backend_configs:
        b = {}
        for query_id, query_share in backend.items():
            b[query_selection[query_id]] = query_share
        translated_backend_configs.append(b)

    # print(translated_backend_configs)
    if robust:
        if version == 'default':
            def query_accessed_size(query):
                accessed_size = 0
                for column in query._columns:
                    accessed_size += column.size()
                return accessed_size
            benchmark_queries_sorted_by_accessed_size = sorted(benchmark._queries, key=query_accessed_size, reverse=True)
            for query in benchmark_queries_sorted_by_accessed_size:
                # print('QUERY_NR', query._nr)
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
                # print(differences)
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


def get_replica_configurations_rainer(benchmark, num_backends, load_file, version='decomposition', robust=False, failure_node=None):
    # next line important for add_implicitly_executable_queries (TODO)
    if benchmark.name != 'acdoca':
        benchmark.add_load(load_file)
    if benchmark.name in ['acdoca', 'tpch', 'tpcds']:
        folder = load_file.split('/')[-1].split('.')[0] + '/'
    if version == 'decomposition':
        folder += 'rainer'
    elif version == 'optimal':
        folder += 'optimal'
    else:
        assert version == 'extend', version
        folder += 'extend'
    if robust:
        folder += '/robust'
    file_names = glob.glob(os.path.join(os.path.dirname(__file__), '%s/%s/sol_K%d_*_out%s.txt' % (benchmark.name, folder, num_backends, '_robust' if robust else '')))
    assert len(file_names) == 1, 'Files: %s for: %s, version=%s, robust=%s, %d' % (file_names, benchmark.name, version, robust, num_backends)
    # print(f'---rainer allocation file: {file_names[0]}')
    replica_configurations = [{} for _ in range(num_backends)]

    section_nr = 1
    if robust:
        if failure_node is None:
            section_nr = 7
        else:
            section_nr = 9 + 2 * failure_node

    with open(file_names[0]) as f:
        file_txt = f.read()
        sections = file_txt.split('\n\n')
        config_txt = sections[section_nr]
        lines = config_txt.split('\n')
        # print(lines, len(lines))
        if benchmark.name == 'tpch':
            assert(len(lines) == 21)
        else:
            pass
            # TODO
        for line in lines[1:]:
            query_id = int(line.split()[0]) - 1
            ratios = [int(round(float(ratio), 4) * 1000) for ratio in line.split()[1:]]
            sum_ratios = sum(ratios)
            ratios = [fractions.Fraction(ratio, sum_ratios) if ratio != 0 else 0 for ratio in ratios]
            if robust and failure_node is not None:
                assert(len(ratios) == num_backends - 1)
            else:
                assert (len(ratios) == num_backends)
            assert(sum(ratios) == 1)
            for replica_id, ratio in enumerate(ratios):
                if failure_node is not None and replica_id >= failure_node:
                    replica_id += 1
                if ratio != 0:
                    replica_configurations[replica_id][query_id] = ratio
        if robust:
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


def get_replica_configurations(benchmark, num_backends, load_file, allocation, robust=False, failure_node=None):
    if failure_node is not None:
        assert failure_node < num_backends
    if allocation == 'rabl':
        return get_replica_configurations_rabl(benchmark, num_backends, load_file, robust=robust, version='default', failure_node=failure_node)
    elif allocation == 'chained':
        assert robust is True
        return get_replica_configurations_rabl(benchmark, num_backends, load_file, robust=robust, version='chained', failure_node=failure_node)
    elif allocation == 'rainer':
        return get_replica_configurations_rainer(benchmark, num_backends, load_file, version='decomposition', robust=robust, failure_node=failure_node)
    elif allocation == 'optimal':
        return get_replica_configurations_rainer(benchmark, num_backends, load_file, version='optimal', robust=robust, failure_node=failure_node)
    elif allocation == 'extend':
        assert robust is True
        return get_replica_configurations_rainer(benchmark, num_backends, load_file, version='extend', robust=robust, failure_node=failure_node)
    else:
        raise Exception('Allocation must be "rabl" or "rainer"')
