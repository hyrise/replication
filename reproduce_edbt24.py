import copy
from fractions import Fraction
from munkres import Munkres

from partial_replication import TPCH, TPCDS, Accounting, Query, set_column_sizes, config_accessed_size, get_runtime_ampl, config_data_modification_costs, Benchmark_accessed_columns_queries
from setup_benchmark import get_replica_configurations, hungarian_configuration


def changing_workload_tpch():
    benchmark = TPCH()
    load_file = 'tpch/costs_edbt24.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)
    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))
    allocation_base = get_replica_configurations(benchmark, 1, load_file, 'greedy')

    workload_old = TPCH()
    workload_old_load_file = 'tpch/costs_edbt24_low.txt'
    workload_old.add_load(workload_old_load_file)
    set_column_sizes(workload_old._tables, workload_old_load_file)

    workload_new = TPCH()
    workload_new_load_file = 'tpch/costs_edbt24_high.txt'
    workload_new.add_load(workload_new_load_file)
    set_column_sizes(workload_new._tables, workload_new_load_file)

    for number_of_nodes in range(2, 17):
        allocation_old = get_replica_configurations(workload_old, number_of_nodes, workload_old_load_file, 'optimal')
        old_size = config_accessed_size(benchmark, allocation_old) / accessed_size

        allocation_new_greedy = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                           'greedy')
        new_size_greedy = config_accessed_size(benchmark, allocation_new_greedy) / accessed_size

        allocation_merge = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_greedy)
        indexes = m.compute(matrix)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_greedy[node_id]:
                if query_id not in allocation_merge[j]:
                    allocation_merge[j][query_id] = 0
        no_dealloc_greedy_size = config_accessed_size(benchmark, allocation_merge) / accessed_size

        # new min data
        allocation_new_optimal = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                            'optimal')
        new_size_optimal = config_accessed_size(benchmark, allocation_new_optimal) / accessed_size

        # Calculate realloc size for optimal
        allocation_merge2 = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_optimal)
        indexes = m.compute(matrix)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_optimal[node_id]:
                if query_id not in allocation_merge2[j]:
                    allocation_merge2[j][query_id] = 0
        no_dealloc_optimal_size = config_accessed_size(benchmark, allocation_merge2) / accessed_size

        # min realloc
        min_realloc = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                 'optimal', reallocation=('min_realloc', 'add_all'))
        min_realloc_size = config_accessed_size(benchmark, min_realloc) / accessed_size

        # Calculate realloc size for min_realloc
        allocation_merge3 = copy.deepcopy(allocation_old)
        assert old_size == config_accessed_size(benchmark, allocation_merge3) / accessed_size
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, min_realloc)
        indexes = m.compute(matrix)
        # print(indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in min_realloc[node_id]:
                if query_id not in allocation_merge3[j]:
                    allocation_merge3[j][query_id] = 0
        no_dealloc_realloc_size = config_accessed_size(benchmark, allocation_merge3) / accessed_size

        time_optimal = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file, version='optimal')
        memory_ratio_optimal_greedy = (new_size_optimal / new_size_greedy - 1) * 100
        reallocation_ratio_optimal_greedy = ((no_dealloc_optimal_size - old_size) / (
                no_dealloc_greedy_size - old_size) - 1) * 100

        time_reallocation = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file, version='optimal',
                                             reallocation=('min_realloc', 'add_all'))
        memory_ratio_realloc_greedy = (min_realloc_size / new_size_greedy - 1) * 100
        reallocation_ratio_realloc_greedy = ((no_dealloc_realloc_size - old_size) / (
                no_dealloc_greedy_size - old_size) - 1) * 100

        print(f'{number_of_nodes:4}', '&\t',
              f'{new_size_greedy:7.3f}', '&\t',
              f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
              f'\green{{{memory_ratio_optimal_greedy:7.1f}\%}}', '&\t',
              f'\green{{{reallocation_ratio_optimal_greedy:7.1f}\%}}', '&\t',
              f'{time_optimal:7.1f} s', '&\t',
              f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
              f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
              f'{time_reallocation:7.1f} s',
              '\\\\')


def changing_workload_tpcds():
    benchmark = TPCDS()
    load_file = 'tpcds/costs_edbt24.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)
    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))
    allocation_base = get_replica_configurations(benchmark, 1, load_file, 'greedy')

    workload_old = TPCDS()
    workload_old_load_file = 'tpcds/costs_edbt24_low.txt'
    workload_old.add_load(workload_old_load_file)
    set_column_sizes(workload_old._tables, workload_old_load_file)

    workload_new = TPCDS()
    workload_new_load_file = 'tpcds/costs_edbt24_high.txt'
    workload_new.add_load(workload_new_load_file)
    set_column_sizes(workload_new._tables, workload_new_load_file)

    greedy_memory = []
    greedy_reallocation = []

    optimal_basic_memory = []
    optimal_basic_reallocation = []

    ILP_heuristic_basic_memory = []
    ILP_heuristic_basic_reallocation = []

    optimal_reallocation_memory = []
    optimal_reallocation_reallocation = []

    ILP_heuristic_reallocation_memory = []
    ILP_heuristic_reallocation_reallocation = []

    for number_of_nodes in range(2, 17):
        data = []
        allocation_old = get_replica_configurations(workload_old, number_of_nodes, workload_old_load_file, 'optimal')
        old_size = config_accessed_size(benchmark, allocation_old) / accessed_size

        allocation_new_greedy = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                           'greedy')
        new_size_greedy = config_accessed_size(benchmark, allocation_new_greedy) / accessed_size

        allocation_merge = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_greedy)
        indexes = m.compute(matrix)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_greedy[node_id]:
                if query_id not in allocation_merge[j]:
                    allocation_merge[j][query_id] = 0
        no_dealloc_greedy_size = config_accessed_size(benchmark, allocation_merge) / accessed_size

        greedy_memory.append((number_of_nodes, new_size_greedy))
        greedy_reallocation.append((number_of_nodes, no_dealloc_greedy_size - old_size))

        # new min data
        allocation_new_optimal = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                            'optimal')
        new_size_optimal = config_accessed_size(benchmark, allocation_new_optimal) / accessed_size

        # Calculate realloc size for decomposition
        allocation_merge2 = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_optimal)
        indexes = m.compute(matrix)
        # print(indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_optimal[node_id]:
                if query_id not in allocation_merge2[j]:
                    allocation_merge2[j][query_id] = 0
        no_dealloc_optimal_size = config_accessed_size(benchmark, allocation_merge2) / accessed_size

        time_optimal = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file, version='optimal')
        memory_ratio_optimal_greedy = (new_size_optimal / new_size_greedy - 1) * 100
        reallocation_ratio_optimal_greedy = ((no_dealloc_optimal_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

        optimal_basic_memory.append((number_of_nodes, new_size_optimal))
        optimal_basic_reallocation.append((number_of_nodes, no_dealloc_optimal_size - old_size))

        if number_of_nodes > 5:
            allocation_new_decomp = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                               'decomposition')
            new_size_decomp = config_accessed_size(benchmark, allocation_new_decomp) / accessed_size

            # Calculate realloc size for decomposition
            allocation_merge3 = copy.deepcopy(allocation_old)
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_decomp)
            indexes = m.compute(matrix)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in allocation_new_decomp[node_id]:
                    if query_id not in allocation_merge3[j]:
                        allocation_merge3[j][query_id] = 0
            no_dealloc_decomp_size = config_accessed_size(benchmark, allocation_merge3) / accessed_size

            time_decomposition = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                                  version='decomposition')
            memory_ratio_decomp_greedy = (new_size_decomp / new_size_greedy - 1) * 100
            reallocation_ratio_decomp_greedy = ((no_dealloc_decomp_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            ILP_heuristic_basic_memory.append((number_of_nodes, new_size_decomp))
            ILP_heuristic_basic_reallocation.append((number_of_nodes, no_dealloc_decomp_size - old_size))

        # min realloc
        min_realloc = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                 'optimal', reallocation=('min_realloc', 'add_all'))
        min_realloc_size = config_accessed_size(benchmark, min_realloc) / accessed_size

        # Calculate realloc size for min_realloc
        allocation_merge4 = copy.deepcopy(allocation_old)
        assert old_size == config_accessed_size(benchmark, allocation_merge4) / accessed_size
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, min_realloc)
        indexes = m.compute(matrix)
        # print(indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in min_realloc[node_id]:
                if query_id not in allocation_merge4[j]:
                    allocation_merge4[j][query_id] = 0
        no_dealloc_realloc_size = config_accessed_size(benchmark, allocation_merge4) / accessed_size

        time_reallocation = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file, version='optimal',
                                             reallocation=('min_realloc', 'add_all'))
        memory_ratio_realloc_greedy = (min_realloc_size / new_size_greedy - 1) * 100
        reallocation_ratio_realloc_greedy = ((no_dealloc_realloc_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

        optimal_reallocation_memory.append((number_of_nodes, min_realloc_size))
        optimal_reallocation_reallocation.append((number_of_nodes, no_dealloc_realloc_size - old_size))

        if number_of_nodes > 11:
            min_realloc_time_limit = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                                'time_limit', reallocation=('min_realloc', 'add_all'),
                                                                time_limit=10)
            min_realloc_time_limit_size = config_accessed_size(benchmark, min_realloc_time_limit) / accessed_size

            # Calculate realloc size for min_realloc
            allocation_merge5 = copy.deepcopy(allocation_old)
            assert old_size == config_accessed_size(benchmark, allocation_merge5) / accessed_size
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, min_realloc_time_limit)
            indexes = m.compute(matrix)
            # print(indexes)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in min_realloc_time_limit[node_id]:
                    if query_id not in allocation_merge5[j]:
                        allocation_merge5[j][query_id] = 0
            no_dealloc_realloc_time_limit_size = config_accessed_size(benchmark, allocation_merge5) / accessed_size

            time_reallocation_time_limit = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                                            version='time_limit',
                                                            reallocation=('min_realloc', 'add_all'), time_limit=10)
            memory_ratio_realloc_time_limit_greedy = (min_realloc_time_limit_size / new_size_greedy - 1) * 100
            reallocation_ratio_realloc_time_limit_greedy = ((no_dealloc_realloc_time_limit_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            ILP_heuristic_reallocation_memory.append((number_of_nodes, min_realloc_time_limit_size))
            ILP_heuristic_reallocation_reallocation.append(
                (number_of_nodes, no_dealloc_realloc_time_limit_size - old_size))

        if number_of_nodes < 6:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\green{{{memory_ratio_optimal_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_optimal_greedy:7.1f}\%}}', '&\t',
                  f'{time_optimal:7.1f} s', '&\t',
                  f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'{time_reallocation:7.1f} s',
                  '\\\\')
        elif number_of_nodes < 12:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{D}}\green{{{memory_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}\green{{{reallocation_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}{time_decomposition:7.1f} s', '&\t',
                  f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'{time_reallocation:7.1f} s',
                  '\\\\')
        else:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{D}}\green{{{memory_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}\green{{{reallocation_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}{time_decomposition:7.1f} s', '&\t',
                  f'\\textsubscript{{T}}\green{{{memory_ratio_realloc_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{T}}\green{{{reallocation_ratio_realloc_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{T}}{time_reallocation_time_limit:7.1f} s',
                  '\\\\')


def changing_workload_accounting():
    benchmark = Accounting()
    load_file = 'accounting/query_costs_full.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)
    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    # print(access_all_config)
    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))
    allocation_base = get_replica_configurations(benchmark, 1, load_file, 'greedy')

    workload_old = Accounting()
    workload_old_load_file = 'accounting/query_costs_full_old_workload.txt'
    workload_old.add_load(workload_old_load_file)
    set_column_sizes(workload_old._tables, workload_old_load_file)

    workload_new = Accounting()
    workload_new_load_file = 'accounting/query_costs_full_new_workload.txt'
    workload_new.add_load(workload_new_load_file)
    set_column_sizes(workload_new._tables, workload_new_load_file)

    greedy_memory = []
    greedy_reallocation = []

    optimal_basic_memory = []
    optimal_basic_reallocation = []

    ILP_heuristic_basic_memory = []
    ILP_heuristic_basic_reallocation = []

    optimal_reallocation_memory = []
    optimal_reallocation_reallocation = []

    ILP_heuristic_reallocation_memory = []
    ILP_heuristic_reallocation_reallocation = []

    for number_of_nodes in range(2, 17):
        data = []
        allocation_old = get_replica_configurations(workload_old, number_of_nodes, workload_old_load_file,
                                                    'partial_clustering', clustered_queries=4361)
        old_size = config_accessed_size(benchmark, allocation_old) / accessed_size

        allocation_new_greedy = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                           'greedy')
        new_size_greedy = config_accessed_size(benchmark, allocation_new_greedy) / accessed_size

        allocation_merge = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_greedy)
        indexes = m.compute(matrix)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_greedy[node_id]:
                if query_id not in allocation_merge[j]:
                    allocation_merge[j][query_id] = 0
        no_dealloc_greedy_size = config_accessed_size(benchmark, allocation_merge) / accessed_size

        greedy_memory.append((number_of_nodes, new_size_greedy))
        greedy_reallocation.append((number_of_nodes, no_dealloc_greedy_size - old_size))

        # new min data
        if number_of_nodes < 5:
            allocation_new_optimal = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                                'optimal')
            new_size_optimal = config_accessed_size(benchmark, allocation_new_optimal) / accessed_size

            # Calculate reallocation size for optimal
            allocation_merge2 = copy.deepcopy(allocation_old)
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_optimal)
            indexes = m.compute(matrix)
            # print(indexes)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in allocation_new_optimal[node_id]:
                    if query_id not in allocation_merge2[j]:
                        allocation_merge2[j][query_id] = 0
            no_dealloc_optimal_size = config_accessed_size(benchmark, allocation_merge2) / accessed_size

            time_optimal = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file, version='optimal')
            memory_ratio_optimal_greedy = (new_size_optimal / new_size_greedy - 1) * 100
            reallocation_ratio_optimal_greedy = ((no_dealloc_optimal_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            optimal_basic_memory.append((number_of_nodes, new_size_optimal))
            optimal_basic_reallocation.append((number_of_nodes, no_dealloc_optimal_size - old_size))

        allocation_new_clustering = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                               'partial_clustering', clustered_queries=4361)
        new_size_clustering = config_accessed_size(benchmark, allocation_new_clustering) / accessed_size

        # Calculate reallocation size for clustering
        allocation_merge3 = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_clustering)
        indexes = m.compute(matrix)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_clustering[node_id]:
                if query_id not in allocation_merge3[j]:
                    allocation_merge3[j][query_id] = 0
        no_dealloc_clustering_size = config_accessed_size(benchmark, allocation_merge3) / accessed_size

        time_clustering = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                           version='partial_clustering', clustered_queries=4361)
        memory_ratio_clustering_greedy = (new_size_clustering / new_size_greedy - 1) * 100
        reallocation_ratio_clustering_greedy = ((no_dealloc_clustering_size - old_size) / (
                no_dealloc_greedy_size - old_size) - 1) * 100

        ILP_heuristic_basic_memory.append((number_of_nodes, new_size_clustering))
        ILP_heuristic_basic_reallocation.append((number_of_nodes, no_dealloc_clustering_size - old_size))

        # min realloc
        if number_of_nodes < 8:
            min_realloc = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                     'optimal', reallocation=('min_realloc', 'add_all'))
            min_realloc_size = config_accessed_size(benchmark, min_realloc) / accessed_size

            # Calculate realloc size for min_realloc
            allocation_merge4 = copy.deepcopy(allocation_old)
            assert old_size == config_accessed_size(benchmark, allocation_merge4) / accessed_size
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, min_realloc)
            indexes = m.compute(matrix)
            # print(indexes)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in min_realloc[node_id]:
                    if query_id not in allocation_merge4[j]:
                        allocation_merge4[j][query_id] = 0
            no_dealloc_realloc_size = config_accessed_size(benchmark, allocation_merge4) / accessed_size

            time_reallocation = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file, version='optimal',
                                                 reallocation=('min_realloc', 'add_all'))
            memory_ratio_realloc_greedy = (min_realloc_size / new_size_greedy - 1) * 100
            reallocation_ratio_realloc_greedy = ((no_dealloc_realloc_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            optimal_reallocation_memory.append((number_of_nodes, min_realloc_size))
            optimal_reallocation_reallocation.append((number_of_nodes, no_dealloc_realloc_size - old_size))

        min_realloc_clustering = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                            'partial_clustering',
                                                            reallocation=('min_realloc', 'add_all'),
                                                            clustered_queries=4361)
        min_realloc_clustering_size = config_accessed_size(benchmark, min_realloc_clustering) / accessed_size

        # Calculate realloc size for min_realloc
        allocation_merge5 = copy.deepcopy(allocation_old)
        assert old_size == config_accessed_size(benchmark, allocation_merge5) / accessed_size
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, min_realloc_clustering)
        indexes = m.compute(matrix)
        # print(indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in min_realloc_clustering[node_id]:
                if query_id not in allocation_merge5[j]:
                    allocation_merge5[j][query_id] = 0
        no_dealloc_realloc_clustering_size = config_accessed_size(benchmark, allocation_merge5) / accessed_size

        time_reallocation_clustering = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                                        version='partial_clustering',
                                                        reallocation=('min_realloc', 'add_all'), clustered_queries=4361)
        memory_ratio_realloc_clustering_greedy = (min_realloc_clustering_size / new_size_greedy - 1) * 100
        reallocation_ratio_realloc_clustering_greedy = ((no_dealloc_realloc_clustering_size - old_size) / (
                no_dealloc_greedy_size - old_size) - 1) * 100

        ILP_heuristic_reallocation_memory.append((number_of_nodes, min_realloc_clustering_size))
        ILP_heuristic_reallocation_reallocation.append((number_of_nodes, no_dealloc_realloc_clustering_size - old_size))

        if number_of_nodes < 6:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{C}}\green{{{memory_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}\green{{{reallocation_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}{time_clustering:7.1f} s', '&\t',
                  f'\\textsubscript{{C}}\green{{{memory_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}\green{{{reallocation_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}{time_reallocation_clustering:7.1f} s',
                  '\\\\')
        elif number_of_nodes < 9:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{memory_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{reallocation_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}{time_clustering:7.1f} s', '&\t',
                  f'\\textsubscript{{C}}\green{{{memory_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}\green{{{reallocation_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}{time_reallocation_clustering:7.1f} s',
                  '\\\\')
        else:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{memory_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{reallocation_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}{time_clustering:7.1f} s', '&\t',
                  f'\\textsubscript{{CT}}\green{{{memory_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CT}}\green{{{reallocation_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CT}}{time_reallocation_clustering:7.1f} s',
                  '\\\\')


def add_node_tpch():
    benchmark = TPCH()
    load_file = 'tpch/costs_edbt24.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)
    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))
    allocation_base = get_replica_configurations(benchmark, 1, load_file, 'greedy')

    workload_old = TPCH()
    workload_old_load_file = load_file
    workload_old.add_load(workload_old_load_file)
    set_column_sizes(workload_old._tables, workload_old_load_file)

    workload_new = TPCH()
    workload_new_load_file = load_file
    workload_new.add_load(workload_new_load_file)
    set_column_sizes(workload_new._tables, workload_new_load_file)

    greedy_memory = []
    greedy_reallocation = []

    optimal_basic_memory = []
    optimal_basic_reallocation = []

    ILP_heuristic_basic_memory = []
    ILP_heuristic_basic_reallocation = []

    optimal_reallocation_memory = []
    optimal_reallocation_reallocation = []

    for number_of_nodes in range(2, 17):
        data = []
        if number_of_nodes == 2:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'greedy')
        else:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'optimal')
        allocation_old.append({})
        old_size = config_accessed_size(benchmark, allocation_old) / accessed_size

        # start = time.time()
        allocation_new_greedy = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                           'greedy')
        new_size_greedy = config_accessed_size(benchmark, allocation_new_greedy) / accessed_size

        allocation_merge = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_greedy)
        indexes = m.compute(matrix)
        # print('greedy', indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_greedy[node_id]:
                if query_id not in allocation_merge[j]:
                    allocation_merge[j][query_id] = 0
        # print('calctime:', time.time() - start)
        no_dealloc_greedy_size = config_accessed_size(benchmark, allocation_merge) / accessed_size

        greedy_memory.append((number_of_nodes, new_size_greedy))
        greedy_reallocation.append((number_of_nodes, no_dealloc_greedy_size - old_size))

        # new min data
        allocation_new_optimal = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                            'optimal')
        new_size_optimal = config_accessed_size(benchmark, allocation_new_optimal) / accessed_size

        # Calculate realloc size for optimal
        allocation_merge2 = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_optimal)
        indexes = m.compute(matrix)
        # print('decomp', indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_optimal[node_id]:
                if query_id not in allocation_merge2[j]:
                    allocation_merge2[j][query_id] = 0
        no_dealloc_optimal_size = config_accessed_size(benchmark, allocation_merge2) / accessed_size

        time_optimal = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal')
        memory_ratio_optimal_greedy = (new_size_optimal / new_size_greedy - 1) * 100
        reallocation_ratio_optimal_greedy = ((no_dealloc_optimal_size - old_size) / (
                no_dealloc_greedy_size - old_size) - 1) * 100

        optimal_basic_memory.append((number_of_nodes, new_size_optimal))
        optimal_basic_reallocation.append((number_of_nodes, no_dealloc_optimal_size - old_size))

        if number_of_nodes > 10:
            allocation_new_decomp = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                               'decomposition')
            new_size_decomp = config_accessed_size(benchmark, allocation_new_decomp) / accessed_size

            # Calculate realloc size for decomposition
            allocation_merge3 = copy.deepcopy(allocation_old)
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_decomp)
            indexes = m.compute(matrix)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in allocation_new_decomp[node_id]:
                    if query_id not in allocation_merge3[j]:
                        allocation_merge3[j][query_id] = 0
            no_dealloc_decomp_size = config_accessed_size(benchmark, allocation_merge3) / accessed_size

            time_decomposition = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                                  version='decomposition')
            memory_ratio_decomp_greedy = (new_size_decomp / new_size_greedy - 1) * 100
            reallocation_ratio_decomp_greedy = ((no_dealloc_decomp_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            ILP_heuristic_basic_memory.append((number_of_nodes, new_size_decomp))
            ILP_heuristic_basic_reallocation.append((number_of_nodes, no_dealloc_decomp_size - old_size))

        # min realloc
        min_realloc = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                 'optimal', reallocation=('min_realloc', 'add_all'))
        min_realloc_size = config_accessed_size(benchmark, min_realloc) / accessed_size

        # Calculate realloc size for min_realloc
        allocation_merge5 = copy.deepcopy(allocation_old)
        assert old_size == config_accessed_size(benchmark, allocation_merge5) / accessed_size
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, min_realloc)
        indexes = m.compute(matrix)
        # print('min_realloc', indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in min_realloc[node_id]:
                if query_id not in allocation_merge5[j]:
                    allocation_merge5[j][query_id] = 0
        no_dealloc_realloc_size = config_accessed_size(benchmark, allocation_merge5) / accessed_size

        time_reallocation = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal',
                                             reallocation=('min_realloc', 'add_all'))
        memory_ratio_realloc_greedy = (min_realloc_size / new_size_greedy - 1) * 100
        reallocation_ratio_realloc_greedy = ((no_dealloc_realloc_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

        optimal_reallocation_memory.append((number_of_nodes, min_realloc_size))
        optimal_reallocation_reallocation.append((number_of_nodes, no_dealloc_realloc_size - old_size))

        if number_of_nodes < 11:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\green{{{memory_ratio_optimal_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_optimal_greedy:7.1f}\%}}', '&\t',
                  f'{time_optimal:7.1f} s', '&\t',
                  f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'{time_reallocation:7.1f} s',
                  '\\\\')
        else:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{D}}\green{{{memory_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}\green{{{reallocation_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}{time_decomposition:7.1f} s', '&\t',
                  f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'{time_reallocation:7.1f} s',
                  '\\\\')


def add_node_tpcds():
    benchmark = TPCDS()
    load_file = 'tpcds/costs_edbt24.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)
    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))
    allocation_base = get_replica_configurations(benchmark, 1, load_file, 'greedy')

    workload_old = TPCDS()
    workload_old_load_file = load_file
    workload_old.add_load(workload_old_load_file)
    set_column_sizes(workload_old._tables, workload_old_load_file)

    workload_new = TPCDS()
    workload_new_load_file = load_file
    workload_new.add_load(workload_new_load_file)
    set_column_sizes(workload_new._tables, workload_new_load_file)

    greedy_memory = []
    greedy_reallocation = []

    optimal_basic_memory = []
    optimal_basic_reallocation = []

    ILP_heuristic_basic_memory = []
    ILP_heuristic_basic_reallocation = []

    optimal_reallocation_memory = []
    optimal_reallocation_reallocation = []

    ILP_heuristic_reallocation_memory = []
    ILP_heuristic_reallocation_reallocation = []

    for number_of_nodes in range(2, 17):
        allocation_new_greedy = allocation_new_optimal = allocation_new_decomp = allocation_new_decomp_time_limit = min_realloc = min_realloc_time_limit = None
        time_optimal = time_decomposition = time_decomposition_time_limit = time_reallocation = time_reallocation_time_limit = None

        data = []
        if number_of_nodes == 2:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'greedy')
        elif number_of_nodes == 3:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'optimal')
        else:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'decomposition')
        allocation_old.append({})
        old_size = config_accessed_size(benchmark, allocation_old) / accessed_size

        # start = time.time()
        allocation_new_greedy = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                           'greedy')
        new_size_greedy = config_accessed_size(benchmark, allocation_new_greedy) / accessed_size

        allocation_merge = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_greedy)
        indexes = m.compute(matrix)
        # print('greedy', indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_greedy[node_id]:
                if query_id not in allocation_merge[j]:
                    allocation_merge[j][query_id] = 0
        # print('calctime:', time.time() - start)
        no_dealloc_greedy_size = config_accessed_size(benchmark, allocation_merge) / accessed_size

        greedy_memory.append((number_of_nodes, new_size_greedy))
        greedy_reallocation.append((number_of_nodes, no_dealloc_greedy_size - old_size))

        # new min data
        if number_of_nodes < 12:
            allocation_new_optimal = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                                'optimal')
            new_size_optimal = config_accessed_size(benchmark, allocation_new_optimal) / accessed_size

            # Calculate realloc size for optimal
            allocation_merge2 = copy.deepcopy(allocation_old)
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_optimal)
            indexes = m.compute(matrix)
            # print('decomp', indexes)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in allocation_new_optimal[node_id]:
                    if query_id not in allocation_merge2[j]:
                        allocation_merge2[j][query_id] = 0
            no_dealloc_optimal_size = config_accessed_size(benchmark, allocation_merge2) / accessed_size

            time_optimal = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal')
            memory_ratio_optimal_greedy = (new_size_optimal / new_size_greedy - 1) * 100
            reallocation_ratio_optimal_greedy = ((no_dealloc_optimal_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            optimal_basic_memory.append((number_of_nodes, new_size_optimal))
            optimal_basic_reallocation.append((number_of_nodes, no_dealloc_optimal_size - old_size))

        if number_of_nodes in [4, 5, 6, 9]:
            allocation_new_decomp = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                               'decomposition')
            new_size_decomp = config_accessed_size(benchmark, allocation_new_decomp) / accessed_size

            # Calculate realloc size for decomposition
            allocation_merge3 = copy.deepcopy(allocation_old)
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_decomp)
            indexes = m.compute(matrix)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in allocation_new_decomp[node_id]:
                    if query_id not in allocation_merge3[j]:
                        allocation_merge3[j][query_id] = 0
            no_dealloc_decomp_size = config_accessed_size(benchmark, allocation_merge3) / accessed_size

            time_decomposition = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                                  version='decomposition')
            memory_ratio_decomp_greedy = (new_size_decomp / new_size_greedy - 1) * 100
            reallocation_ratio_decomp_greedy = ((no_dealloc_decomp_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            ILP_heuristic_basic_memory.append((number_of_nodes, new_size_decomp))
            ILP_heuristic_basic_reallocation.append((number_of_nodes, no_dealloc_decomp_size - old_size))

        if number_of_nodes in [7, 8, 10, 11, 12, 13, 14, 15, 16]:
            allocation_new_decomp_time_limit = get_replica_configurations(workload_new, number_of_nodes,
                                                                          workload_new_load_file,
                                                                          'time_limit', time_limit=5)
            new_size_decomp_time_limit = config_accessed_size(benchmark,
                                                              allocation_new_decomp_time_limit) / accessed_size

            # Calculate realloc size for decomposition
            allocation_merge4 = copy.deepcopy(allocation_old)
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_decomp_time_limit)
            indexes = m.compute(matrix)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in allocation_new_decomp_time_limit[node_id]:
                    if query_id not in allocation_merge4[j]:
                        allocation_merge4[j][query_id] = 0
            no_dealloc_decomp_size_time_limit = config_accessed_size(benchmark, allocation_merge4) / accessed_size

            time_decomposition_time_limit = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                                             version='time_limit', time_limit=5)
            memory_ratio_decomp_time_limit_greedy = (new_size_decomp_time_limit / new_size_greedy - 1) * 100
            reallocation_ratio_decomp_time_limit_greedy = ((no_dealloc_decomp_size_time_limit - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            ILP_heuristic_basic_memory.append((number_of_nodes, new_size_decomp_time_limit))
            ILP_heuristic_basic_reallocation.append((number_of_nodes, no_dealloc_decomp_size_time_limit - old_size))

        # min realloc
        min_realloc = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                 'optimal', reallocation=('min_realloc', 'add_all'))
        min_realloc_size = config_accessed_size(benchmark, min_realloc) / accessed_size

        # Calculate realloc size for min_realloc
        allocation_merge5 = copy.deepcopy(allocation_old)
        assert old_size == config_accessed_size(benchmark, allocation_merge5) / accessed_size
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, min_realloc)
        indexes = m.compute(matrix)
        # print('min_realloc', indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in min_realloc[node_id]:
                if query_id not in allocation_merge5[j]:
                    allocation_merge5[j][query_id] = 0
        no_dealloc_realloc_size = config_accessed_size(benchmark, allocation_merge5) / accessed_size

        time_reallocation = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal',
                                             reallocation=('min_realloc', 'add_all'))
        memory_ratio_realloc_greedy = (min_realloc_size / new_size_greedy - 1) * 100
        reallocation_ratio_realloc_greedy = ((no_dealloc_realloc_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

        optimal_reallocation_memory.append((number_of_nodes, min_realloc_size))
        optimal_reallocation_reallocation.append((number_of_nodes, no_dealloc_realloc_size - old_size))

        if number_of_nodes > 8:
            # time_limit
            min_realloc_time_limit = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                                'time_limit', reallocation=('min_realloc', 'add_all'),
                                                                time_limit=10)
            min_realloc_time_limit_size = config_accessed_size(benchmark, min_realloc_time_limit) / accessed_size

            # Calculate realloc size for min_realloc
            allocation_merge6 = copy.deepcopy(allocation_old)
            assert old_size == config_accessed_size(benchmark, allocation_merge6) / accessed_size
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, min_realloc_time_limit)
            indexes = m.compute(matrix)
            # print(indexes)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in min_realloc_time_limit[node_id]:
                    if query_id not in allocation_merge6[j]:
                        allocation_merge6[j][query_id] = 0
            no_dealloc_realloc_time_limit_size = config_accessed_size(benchmark, allocation_merge6) / accessed_size

            time_reallocation_time_limit = get_runtime_ampl(benchmark, number_of_nodes, workload_new_load_file,
                                                            version='time_limit',
                                                            reallocation=('min_realloc', 'add_all'), time_limit=10)
            memory_ratio_realloc_time_limit_greedy = (min_realloc_time_limit_size / new_size_greedy - 1) * 100
            reallocation_ratio_realloc_time_limit_greedy = ((no_dealloc_realloc_time_limit_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            ILP_heuristic_reallocation_memory.append((number_of_nodes, min_realloc_time_limit_size))
            ILP_heuristic_reallocation_reallocation.append(
                (number_of_nodes, no_dealloc_realloc_time_limit_size - old_size))

        if number_of_nodes < 4:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\green{{{memory_ratio_optimal_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_optimal_greedy:7.1f}\%}}', '&\t',
                  f'{time_optimal:7.1f} s', '&\t',
                  f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'{time_reallocation:7.1f} s',
                  '\\\\')
        elif number_of_nodes < 7:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{D}}\green{{{memory_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}\green{{{reallocation_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}{time_decomposition:7.1f} s', '&\t',
                  f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'{time_reallocation:7.1f} s',
                  '\\\\')
        elif number_of_nodes < 9:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{DT}}\green{{{memory_ratio_decomp_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{DT}}\green{{{reallocation_ratio_decomp_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{DT}}{time_decomposition_time_limit:7.1f} s', '&\t',
                  f'\green{{{memory_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{reallocation_ratio_realloc_greedy:7.1f}\%}}', '&\t',
                  f'{time_reallocation:7.1f} s',
                  '\\\\')
        elif number_of_nodes == 9:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{D}}\green{{{memory_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}\green{{{reallocation_ratio_decomp_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{D}}{time_decomposition:7.1f} s', '&\t',
                  f'\\textsubscript{{T}}\green{{{memory_ratio_realloc_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{T}}\green{{{reallocation_ratio_realloc_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{T}}{time_reallocation_time_limit:7.1f} s',
                  '\\\\')
        else:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{DT}}\green{{{memory_ratio_decomp_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{DT}}\green{{{reallocation_ratio_decomp_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{DT}}{time_decomposition_time_limit:7.1f} s', '&\t',
                  f'\\textsubscript{{T}}\green{{{memory_ratio_realloc_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{T}}\green{{{reallocation_ratio_realloc_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{T}}{time_reallocation_time_limit:7.1f} s',
                  '\\\\')


def add_node_accounting():
    benchmark = Accounting()
    load_file = 'accounting/query_costs_full.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)
    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))
    allocation_base = get_replica_configurations(benchmark, 1, load_file, 'greedy')

    workload_old = Accounting()
    workload_old_load_file = 'accounting/query_costs_full.txt'
    workload_old.add_load(workload_old_load_file)
    set_column_sizes(workload_old._tables, workload_old_load_file)

    workload_new = Accounting()
    workload_new_load_file = 'accounting/query_costs_full.txt'
    workload_new.add_load(workload_new_load_file)
    set_column_sizes(workload_new._tables, workload_new_load_file)

    greedy_memory = []
    greedy_reallocation = []

    optimal_basic_memory = []
    optimal_basic_reallocation = []

    ILP_heuristic_basic_memory = []
    ILP_heuristic_basic_reallocation = []

    optimal_reallocation_memory = []
    optimal_reallocation_reallocation = []

    ILP_heuristic_reallocation_memory = []
    ILP_heuristic_reallocation_reallocation = []

    for number_of_nodes in range(2, 17):
        data = []
        if number_of_nodes == 2:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'greedy')
        elif number_of_nodes == 3:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'optimal')
        else:
            allocation_old = get_replica_configurations(workload_old, number_of_nodes - 1, workload_old_load_file,
                                                        'decomposition')
        allocation_old.append({})
        old_size = config_accessed_size(benchmark, allocation_old) / accessed_size

        allocation_new_greedy = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                           'greedy')
        new_size_greedy = config_accessed_size(benchmark, allocation_new_greedy) / accessed_size

        allocation_merge = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_greedy)
        indexes = m.compute(matrix)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_greedy[node_id]:
                if query_id not in allocation_merge[j]:
                    allocation_merge[j][query_id] = 0
        no_dealloc_greedy_size = config_accessed_size(benchmark, allocation_merge) / accessed_size

        greedy_memory.append((number_of_nodes, new_size_greedy))
        greedy_reallocation.append((number_of_nodes, no_dealloc_greedy_size - old_size))

        # new min data
        if number_of_nodes < 6:
            allocation_new_optimal = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                                'optimal')
            new_size_optimal = config_accessed_size(benchmark, allocation_new_optimal) / accessed_size

            # Calculate realloc size for optimal
            allocation_merge2 = copy.deepcopy(allocation_old)
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_optimal)
            indexes = m.compute(matrix)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in allocation_new_optimal[node_id]:
                    if query_id not in allocation_merge2[j]:
                        allocation_merge2[j][query_id] = 0
            no_dealloc_optimal_size = config_accessed_size(benchmark, allocation_merge2) / accessed_size

            time_optimal = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal')
            memory_ratio_optimal_greedy = (new_size_optimal / new_size_greedy - 1) * 100
            reallocation_ratio_optimal_greedy = ((no_dealloc_optimal_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            optimal_basic_memory.append((number_of_nodes, new_size_optimal))
            optimal_basic_reallocation.append((number_of_nodes, no_dealloc_optimal_size - old_size))

        allocation_new_clustering = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                               'partial_clustering', clustered_queries=4361)
        new_size_clustering = config_accessed_size(benchmark, allocation_new_clustering) / accessed_size

        # Calculate reallocation size for clustering
        allocation_merge3 = copy.deepcopy(allocation_old)
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, allocation_new_clustering)
        indexes = m.compute(matrix)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in allocation_new_clustering[node_id]:
                if query_id not in allocation_merge3[j]:
                    allocation_merge3[j][query_id] = 0
        no_dealloc_clustering_size = config_accessed_size(benchmark, allocation_merge3) / accessed_size

        time_clustering = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='partial_clustering',
                                           clustered_queries=4361)
        memory_ratio_clustering_greedy = (new_size_clustering / new_size_greedy - 1) * 100
        reallocation_ratio_clustering_greedy = ((no_dealloc_clustering_size - old_size) / (
                no_dealloc_greedy_size - old_size) - 1) * 100

        ILP_heuristic_basic_memory.append((number_of_nodes, new_size_clustering))
        ILP_heuristic_basic_reallocation.append((number_of_nodes, no_dealloc_clustering_size - old_size))

        # min realloc
        if number_of_nodes < 7:
            min_realloc = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                     'optimal', reallocation=('min_realloc', 'add_all'))
            min_realloc_size = config_accessed_size(benchmark, min_realloc) / accessed_size

            # Calculate realloc size for min_realloc
            allocation_merge4 = copy.deepcopy(allocation_old)
            assert old_size == config_accessed_size(benchmark, allocation_merge4) / accessed_size
            m = Munkres()
            matrix = hungarian_configuration(benchmark, allocation_old, min_realloc)
            indexes = m.compute(matrix)
            # print('min_realloc', indexes)
            for j in range(number_of_nodes):
                node_id = indexes[j][1]
                for query_id in min_realloc[node_id]:
                    if query_id not in allocation_merge4[j]:
                        allocation_merge4[j][query_id] = 0
            no_dealloc_realloc_size = config_accessed_size(benchmark, allocation_merge4) / accessed_size

            time_reallocation = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal',
                                                 reallocation=('min_realloc', 'add_all'))
            memory_ratio_realloc_greedy = (min_realloc_size / new_size_greedy - 1) * 100
            reallocation_ratio_realloc_greedy = ((no_dealloc_realloc_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

            optimal_reallocation_memory.append((number_of_nodes, min_realloc_size))
            optimal_reallocation_reallocation.append((number_of_nodes, no_dealloc_realloc_size - old_size))

        min_realloc_clustering = get_replica_configurations(workload_new, number_of_nodes, workload_new_load_file,
                                                            'partial_clustering',
                                                            reallocation=('min_realloc', 'add_all'),
                                                            clustered_queries=4361)
        min_realloc_clustering_size = config_accessed_size(benchmark, min_realloc_clustering) / accessed_size

        # Calculate realloc size for min_realloc
        allocation_merge3 = copy.deepcopy(allocation_old)
        assert old_size == config_accessed_size(benchmark, allocation_merge3) / accessed_size
        m = Munkres()
        matrix = hungarian_configuration(benchmark, allocation_old, min_realloc_clustering)
        indexes = m.compute(matrix)
        # print(indexes)
        for j in range(number_of_nodes):
            node_id = indexes[j][1]
            for query_id in min_realloc_clustering[node_id]:
                if query_id not in allocation_merge3[j]:
                    allocation_merge3[j][query_id] = 0
        no_dealloc_realloc_clustering_size = config_accessed_size(benchmark, allocation_merge3) / accessed_size

        time_reallocation_clustering = get_runtime_ampl(benchmark, number_of_nodes, load_file,
                                                        version='partial_clustering',
                                                        reallocation=('min_realloc', 'add_all'), clustered_queries=4361)
        memory_ratio_realloc_clustering_greedy = (min_realloc_clustering_size / new_size_greedy - 1) * 100
        reallocation_ratio_realloc_clustering_greedy = ((no_dealloc_realloc_clustering_size - old_size) / (
                    no_dealloc_greedy_size - old_size) - 1) * 100

        ILP_heuristic_reallocation_memory.append((number_of_nodes, min_realloc_clustering_size))
        ILP_heuristic_reallocation_reallocation.append((number_of_nodes, no_dealloc_realloc_clustering_size - old_size))

        if number_of_nodes < 6:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{C}}\green{{{memory_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}\green{{{reallocation_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}{time_clustering:7.1f} s', '&\t',
                  f'\\textsubscript{{C}}\green{{{memory_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}\green{{{reallocation_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}{time_reallocation_clustering:7.1f} s',
                  '\\\\')
        elif number_of_nodes < 9:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{memory_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{reallocation_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}{time_clustering:7.1f} s', '&\t',
                  f'\\textsubscript{{C}}\green{{{memory_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}\green{{{reallocation_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}{time_reallocation_clustering:7.1f} s',
                  '\\\\')
        else:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{new_size_greedy:7.3f}', '&\t',
                  f'{no_dealloc_greedy_size - old_size:7.3f}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{memory_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}\green{{{reallocation_ratio_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CD}}{time_clustering:7.1f} s', '&\t',
                  f'\\textsubscript{{C}}\green{{{memory_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}\green{{{reallocation_ratio_realloc_clustering_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{C}}{time_reallocation_clustering:7.1f} s',
                  '\\\\')


def data_modifications_tpch():
    benchmark = TPCH()
    load_file = 'tpch/costs_edbt24.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)

    columns_per_table = {}
    # print(benchmark._tables)
    for table in benchmark._tables:
        # print(table._name)
        # print(table._columns)
        columns_per_table[table._name] = table._columns

    u1 = Query(1, columns_per_table['LINEITEM'])
    u1._load = Fraction(9, 100)

    u2 = Query(2, columns_per_table['ORDERS'])
    u2._load = Fraction(1, 100)

    benchmark._updates = [u1, u2]

    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    # print('V', access_all_config)
    assert len(access_all_config) == 1
    backend = access_all_config[0]
    backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
    for update in benchmark._updates:
        if set(update._columns).intersection(backend_accessed_columns) != set():
            backend_accessed_columns |= set(update._columns)
    backend_accessed_size = sum([c.size() for c in backend_accessed_columns])
    accessed_size_per_table = {}
    for column in backend_accessed_columns:
        if column._table._name not in accessed_size_per_table:
            accessed_size_per_table[column._table._name] = 0
        accessed_size_per_table[column._table._name] += column.size()

    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))

    read_share_per_table = {}
    overall_load = 0
    for query in benchmark._queries:
        accessed_tables = set()
        overall_load += query._load
        for column in query._columns:
            if column._table._name not in read_share_per_table:
                read_share_per_table[column._table._name] = 0
            if column._table._name not in accessed_tables:
                accessed_tables.add(column._table._name)
                read_share_per_table[column._table._name] += query._load

    greedy_memory = []
    greedy_mod_costs = []

    optimal_memory = []
    optimal_mod_costs = []

    for number_of_nodes in range(2, 17):
        allocation_greedy = get_replica_configurations(benchmark, number_of_nodes, load_file, 'greedy')
        # allocation_greedy = sigmod_greedy(benchmark, number_of_nodes)
        greedy_size = config_accessed_size(benchmark, allocation_greedy) / accessed_size
        greedy_modification_costs = config_data_modification_costs(benchmark, allocation_greedy) / number_of_nodes

        time_optimal = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal',
                                        data_modifications=10)
        allocation_optimal = get_replica_configurations(benchmark, number_of_nodes, load_file, 'optimal',
                                                        data_modifications=10)
        # for backend in allocation_optimal:
        #     print(backend)
        optimal_size = config_accessed_size(benchmark, allocation_optimal) / accessed_size
        optimal_modification_costs = config_data_modification_costs(benchmark, allocation_optimal) / number_of_nodes

        memory_ratio_greedy = (optimal_size / greedy_size - 1) * 100
        modification_costs_ratio_greedy = (optimal_modification_costs / greedy_modification_costs - 1) * 100
        # print(number_of_nodes, greedy_size, optimal_size, memory_ratio_greedy, float(greedy_modification_costs), float(optimal_modification_costs))

        print(f'{number_of_nodes:4}', '&\t',
              f'{greedy_size:7.3f}', '&\t',
              f'{float(100*greedy_modification_costs):7.1f}\%', '&\t',
              f'\green{{{memory_ratio_greedy:7.1f}\%}}', '&\t',
              f'\green{{{float(modification_costs_ratio_greedy):7.1f}\%}}', '&\t',
              f'{time_optimal:7.1f} s', '\\\\')


def data_modifications_tpcds():
    benchmark = TPCDS()
    load_file = 'tpcds/costs_edbt24.txt'
    benchmark.add_load(load_file)
    set_column_sizes(benchmark._tables, load_file)

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
        query._load = Fraction(update[1], overall_update_costs) * Fraction(1, 10)
        benchmark._updates.append(query)

    access_all_config = get_replica_configurations(benchmark, 1, load_file, 'greedy')
    assert len(access_all_config) == 1
    backend = access_all_config[0]
    backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
    for update in benchmark._updates:
        if set(update._columns).intersection(backend_accessed_columns) != set():
            backend_accessed_columns |= set(update._columns)
    backend_accessed_size = sum([c.size() for c in backend_accessed_columns])
    accessed_size_per_table = {}
    for column in backend_accessed_columns:
        if column._table._name not in accessed_size_per_table:
            accessed_size_per_table[column._table._name] = 0
        accessed_size_per_table[column._table._name] += column.size()

    accessed_size = config_accessed_size(benchmark, get_replica_configurations(benchmark, 1, load_file, 'greedy'))

    read_share_per_table = {}
    overall_load = 0
    for query in benchmark._queries:
        accessed_tables = set()
        overall_load += query._load
        for column in query._columns:
            if column._table._name not in read_share_per_table:
                read_share_per_table[column._table._name] = 0
            if column._table._name not in accessed_tables:
                accessed_tables.add(column._table._name)
                read_share_per_table[column._table._name] += query._load

    greedy_memory = []
    greedy_mod_costs = []

    optimal_memory = []
    optimal_mod_costs = []

    heuristic_memory = []
    heuristic_mod_costs = []

    for number_of_nodes in range(2, 17):

        # start = time.time()
        allocation_greedy = get_replica_configurations(benchmark, number_of_nodes, load_file, 'greedy')
        # print('calctime:', time.time() - start)
        # allocation_greedy = sigmod_greedy(benchmark, number_of_nodes)
        greedy_size = config_accessed_size(benchmark, allocation_greedy) / accessed_size
        greedy_modification_costs = config_data_modification_costs(benchmark, allocation_greedy) / number_of_nodes

        greedy_memory.append((number_of_nodes, greedy_size))
        greedy_mod_costs.append((number_of_nodes, float(greedy_modification_costs)))

        time_optimal = get_runtime_ampl(benchmark, number_of_nodes, load_file, version='optimal',
                                        data_modifications=10)
        allocation_optimal = get_replica_configurations(benchmark, number_of_nodes, load_file, 'optimal',
                                                        data_modifications=10)
        # for backend in allocation_optimal:
        #     print(backend)
        optimal_size = config_accessed_size(benchmark, allocation_optimal) / accessed_size
        optimal_modification_costs = config_data_modification_costs(benchmark, allocation_optimal) / number_of_nodes

        memory_ratio_greedy = (optimal_size / greedy_size - 1) * 100
        modification_costs_ratio_greedy = (optimal_modification_costs / greedy_modification_costs - 1) * 100

        optimal_memory.append((number_of_nodes, optimal_size))
        optimal_mod_costs.append((number_of_nodes, float(optimal_modification_costs)))

        if number_of_nodes >= 7:
            time_optimal_time_limit = get_runtime_ampl(benchmark, number_of_nodes, load_file, 'time_limit',
                                                       time_limit=10,
                                                       data_modifications=10)
            allocation_optimal_time_limit = get_replica_configurations(benchmark, number_of_nodes, load_file,
                                                                       'time_limit',
                                                                       time_limit=10, data_modifications=10)
            # for backend in allocation_optimal:
            #     print(backend)
            optimal_time_limit_size = config_accessed_size(benchmark, allocation_optimal_time_limit) / accessed_size
            optimal_time_limit_modification_costs = config_data_modification_costs(benchmark,
                                                                                   allocation_optimal_time_limit) / number_of_nodes

            memory_ratio_time_limit_greedy = (optimal_time_limit_size / greedy_size - 1) * 100
            modification_costs_ratio_time_limit_greedy = (
                                                                     optimal_time_limit_modification_costs / greedy_modification_costs - 1) * 100
            # print(number_of_nodes, greedy_size, optimal_size, memory_ratio_greedy, float(greedy_modification_costs), float(optimal_modification_costs))

            heuristic_memory.append((number_of_nodes, optimal_time_limit_size))
            heuristic_mod_costs.append((number_of_nodes, float(optimal_time_limit_modification_costs)))

        if number_of_nodes < 7:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{greedy_size:7.3f}', '&\t',
                  f'{float(100 * greedy_modification_costs):7.1f}\%', '&\t',
                  f'\green{{{memory_ratio_greedy:7.1f}\%}}', '&\t',
                  f'\green{{{float(modification_costs_ratio_greedy):7.1f}\%}}', '&\t',
                  f'{time_optimal:7.1f}s', '\\\\')
        else:
            print(f'{number_of_nodes:4}', '&\t',
                  f'{greedy_size:7.3f}', '&\t',
                  f'{float(100 * greedy_modification_costs):7.1f}\%', '&\t',
                  f'\\textsubscript{{CT}}\green{{{memory_ratio_time_limit_greedy:7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CT}}\green{{{float(modification_costs_ratio_time_limit_greedy):7.1f}\%}}', '&\t',
                  f'\\textsubscript{{CT}}{time_optimal_time_limit:7.1f} s', '\\\\')



def reproduce_table_2():
    print('Table 2')

    changing_workload_tpch()
    print('(a) TPC-H\n')

    changing_workload_tpcds()
    print('(b) TPC-DS\n')

    changing_workload_accounting()
    print('(c) Accounting workload\n')


def reproduce_table_3():
    print('Table 3')

    add_node_tpch()
    print('(a) TPC-H\n')

    add_node_tpcds()
    print('(b) TPC-DS\n')

    add_node_accounting()
    print('(c) Accounting workload\n')

def reproduce_table_5():
    print('Table 5')

    data_modifications_tpch()
    print('(a) TPC-H\n')

    data_modifications_tpcds()
    print('(b) TPC-DS\n')


def main():
    reproduce_table_2()
    reproduce_table_3()
    reproduce_table_5()

if __name__ == '__main__':
    main()