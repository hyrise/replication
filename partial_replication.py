import copy
import glob
import itertools
import json
import math
import os
from fractions import Fraction

from tpcds.stream_orders import tpcds_stream_orders
from tpch.stream_orders import tpch_stream_orders
from accounting.stream_orders import accounting_stream_orders


class Column:
    def __init__(self, identifier, name, size, table):
        self._id = identifier
        self._name = name.upper()
        self._data_type_size = size
        self._table = table
        self._size = None

    def size(self):
        if self._size is not None:
            return self._size
        return self._data_type_size * self._table.row_count()

    def __str__(self):
        return "C%d %s" % (self._id, self._name)

    @staticmethod
    def size_of_type(s):
        # Default sizes as fallback if column sizes are not queried from Postgres
        s = s.lower()
        if s == 'integer':
            return 4
        elif s == 'date':
            return 8
        elif s == 'time':
            return 8
        elif s.startswith('decimal'):
            return 8
        elif s.startswith('varchar'):
            return int(s.strip('varchar()'))
        elif s.startswith('char'):
            return int(s.strip('char()'))
        elif s.startswith('nvarchar'):
            return int(s.strip('nvarchar()'))
        elif s.startswith('varbinary'):
            return int(s.strip('varbinary()'))
        else:
            raise Exception("Unknown column type: %s", s)


class Table:
    def __init__(self, name):
        self._name = name.upper()
        self._columns = []
        self._benchmark = None

    def size(self):
        return sum([col.size() for col in self._columns])

    def row_size(self):
        return sum([col._data_type_size for col in self._columns])

    def row_count(self):
        return self._benchmark.row_count(self._name)

    def __unicode__(self):
        return self._name

    def __str__(self):
        return self._name


class Query:
    def __init__(self, nr, columns):
        self._nr = nr
        self._columns = columns
        self._load = None       # load share
        self._load_time = None  # processing time in micro seconds

    def __str__(self):
        return "Q%d" % self._nr


class TPCH:
    SIZE = {'PART': 200*1000, 'SUPPLIER': 10*1000, 'PARTSUPP': 800*1000, 'CUSTOMER': 150*1000, 'ORDERS': 1500*1000, 'LINEITEM': 6000*1000, 'NATION': 25, 'REGION': 5}
    name = 'tpch'
    stream_orders = tpch_stream_orders

    def __init__(self, SF=1):
        self._SF = SF
        self._tables = TPCH.parse_tables('tpch/attribute_sizes.txt')
        for t in self._tables:
            t._benchmark = self
        self._queries = TPCH.parse_queries(self._tables)
        self._updates = []

    def row_count(self, table_name):
        if table_name in ['REGION', 'NATION']:
            return self.SIZE[table_name]
        else:
            return self.SIZE[table_name] * self._SF

    @staticmethod
    def parse_tables(table_file):
        with open(os.path.join(os.path.dirname(__file__), table_file)) as f:
            table_definitions = f.read().split('\n\n')

        tables = []

        column_identifier = 0
        for table_def in table_definitions:
            tokens = table_def.split('\n')
            table_name = tokens[0]
            table = Table(table_name)

            for attribute in tokens[1:]:
                attribute_name, attribute_size = attribute.split(',')
                attribute_size = int(attribute_size)
                col = Column(column_identifier, attribute_name, attribute_size, table)
                column_identifier += 1
                table._columns.append(col)
            tables.append(table)
            # print(table._name)

        return tables

    @staticmethod
    def parse_queries(tables):
        queries = []
        for q_nr in range(1, 23):
            with open(os.path.join(os.path.dirname(__file__), 'tpch/queries/%d.sql' % q_nr)) as f:
                q_text = f.read().upper()
            columns = []
            for t in tables:
                for c in t._columns:
                    if c._name in q_text:
                        columns.append(c)
            queries.append(Query(q_nr, columns))
        return queries

    def add_load(self, load_file):
        with open(os.path.join(os.path.dirname(__file__), load_file)) as f:
            lines = f.read().split('\n')
            for i, line in enumerate(lines):
                self._queries[i]._load_time = int(float(line) * 1000 * 1000)
            sum_load_time = sum([q._load_time for q in self._queries])
            for q in self._queries:
                q._load = Fraction(q._load_time, sum_load_time)


def tpcds_parse_row_counts():
    sizes = {}
    with open(os.path.join(os.path.dirname(__file__), 'tpcds_row_counts.txt')) as f:
        row_counts = f.read().split('\n\n')
        assert(len(row_counts) == 25)
        scale_factors = row_counts[0].split('\n')
        assert(len(scale_factors) == 6)
        for table_row_count in row_counts[1:]:
            tokens = table_row_count.split('\n')
            table_name = tokens[0].upper()
            sizes[table_name] = {}
            for i, row_count in enumerate(tokens[2:]):
                #print(i, table_name)
                sizes[table_name][scale_factors[i]] = int(row_count.replace(',', ''))
    return sizes


class TPCDS:
    SIZE = tpcds_parse_row_counts()
    name = 'tpcds'
    stream_orders = tpcds_stream_orders

    def __init__(self, SF='1GB', schema_file='tpcds/schema.sql', query_files='tpcds/queries/%d.sql'):
        self._SF = SF
        self._tables = TPCDS.parse_tables(schema_file)
        for t in self._tables:
            t._benchmark = self
        self._queries = TPCDS.parse_queries(self._tables, query_files)
        self._updates = []

    def row_count(self, table_name):
        return self.SIZE[table_name][self._SF]

    @staticmethod
    def parse_tables(table_file):
        with open(os.path.join(os.path.dirname(__file__), table_file)) as f:
            table_definitions = f.read().split(';')

        tables = []
        column_identifier = 0
        for table_def in table_definitions:
            table_def_t = table_def.split('create table')
            if len(table_def_t) != 2:
                # print('No table def')
                continue
            # else
            table_def = table_def_t[1]
            lines = table_def.split('\n')
            table_name = lines[0].strip()
            #print(table_name)
            if table_name == 'dbgen_version':
                continue
            table = Table(table_name)
            for line in lines[1:]:
                tokens = line.strip().split()
                if len(tokens) < 2 or line.strip().startswith('primary key'):
                    #print('No attribute def')
                    continue
                # else
                attribute_name, attribute_type = tokens[0], tokens[1]
                col = Column(column_identifier, attribute_name, Column.size_of_type(attribute_type), table)
                column_identifier += 1
                table._columns.append(col)
                #print(attribute_name, attribute_type)
            tables.append(table)
        return tables

    @staticmethod
    def parse_queries(tables, query_files):
        queries = []
        for q_nr in range(1, 100):
            with open(os.path.join(os.path.dirname(__file__), query_files % q_nr)) as f:
                q_text = f.read().upper()
            columns = []
            for t in tables:
                for c in t._columns:
                    if c._name in q_text:
                        columns.append(c)
            queries.append(Query(q_nr, columns))
        return queries

    def add_load(self, load_file):
        with open(os.path.join(os.path.dirname(__file__), load_file)) as f:
            lines = f.read().split('\n')
            for i, line in enumerate(lines):
                self._queries[i]._load_time = int(float(line) * 1000 * 1000)
            sum_load_time = sum([q._load_time for q in self._queries])
            for q in self._queries:
                q._load = Fraction(q._load_time, sum_load_time)


class Accounting:
    name = 'accounting'
    stream_orders = accounting_stream_orders

    def __init__(self, SF=1):
        self._SF = SF
        self._tables = Accounting.parse_tables('accounting/attributes_queried_sizes.txt')
        for t in self._tables:
            t._benchmark = self
        self._queries = Accounting.parse_queries(self._tables)
        self._updates = []

    @staticmethod
    def row_count(table_name):
        if table_name == 'Accounting':
            return 1000*1000
        raise Exception('Unknown size of table: %s' % table_name)

    @staticmethod
    def parse_tables(table_file):
        table = Table('Accounting')
        with open(os.path.join(os.path.dirname(__file__), table_file)) as f:
            column_identifier = 0
            for line in f:
                attribute_name, attribute_size = line.split()
                col = Column(column_identifier, attribute_name, None, table)
                col._size = int(attribute_size)
                column_identifier += 1

                table._columns.append(col)
                #print(attribute_name, attribute_type)
        return [table]

    @staticmethod
    def parse_queries(tables):
        queries = []
        q_id = 0
        with open(os.path.join(os.path.dirname(__file__), 'accounting/queries.txt')) as f:
            for line in f:
                _, _, _, _, attribute_str = line.split()
                columns = []
                attributes = attribute_str.split(';')
                number_of_attributes = len(attributes)
                if '$rowid$' in attributes:
                    number_of_attributes -= 1
                for t in tables:
                    for c in t._columns:
                        if c._name in attributes:
                            columns.append(c)
                assert number_of_attributes == len(columns)
                query = Query(q_id + 1, columns)
                q_id += 1
                queries.append(query)
        return queries

    def add_load(self, load_file):
        with open(os.path.join(os.path.dirname(__file__), load_file)) as f:
            lines = f.read().split('\n')
            for i, line in enumerate(lines):
                self._queries[i]._load_time = int(float(line))
            sum_load_time = sum([q._load_time for q in self._queries])
            for q in self._queries:
                q._load = Fraction(q._load_time, sum_load_time)

    def normalize_load(self):
        sum_load = sum([q._load for q in self._queries])
        for i, q in enumerate(self._queries):
            # print(i+1, q._load)
            q._load = Fraction(q._load, sum_load)


def Benchmark_accessed_columns(benchmark):
    accessed_columns = set()
    for q in benchmark._queries:
        for c in q._columns:
            accessed_columns.add(c)
    return accessed_columns


def Benchmark_accessed_columns_queries(benchmark, query_ids):
    accessed_columns = set()
    for query in benchmark._queries:
        if query._nr - 1 in query_ids:
            for column in query._columns:
                accessed_columns.add(column)
    return accessed_columns


def config_get_benchmarks(benchmark, config):
    #print(len(benchmark._queries))
    #print(config)
    benchmarks = []
    for backend in config:
        new_bench = copy.copy(benchmark)
        queries = []
        for q_id in backend.keys():
            #print(q_id)
            q = Query(benchmark._queries[q_id]._nr, benchmark._queries[q_id]._columns)
            q._load = benchmark._queries[q_id]._load * backend[q_id]
            queries.append(q)
        new_bench._queries = queries
        benchmarks.append(new_bench)
    return benchmarks


def config_accessed_size(benchmark, config):
    config_accessed_size = 0
    for backend in config:
        # print(sorted(backend.keys()))
        backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
        for update in benchmark._updates:
            if set(update._columns).intersection(backend_accessed_columns) != set():
                backend_accessed_columns |= set(update._columns)
        backend_accessed_size = sum([c.size() for c in backend_accessed_columns])
        # print(backend_accessed_size)
        config_accessed_size += backend_accessed_size
    return config_accessed_size


def config_data_modification_costs(benchmark, config):
    data_modification_costs = 0
    for backend in config:
        # print(sorted(backend.keys()))
        backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
        for update in benchmark._updates:
            if set(update._columns).intersection(backend_accessed_columns) != set():
                data_modification_costs += update._load
    return data_modification_costs


def get_runtime_ampl(benchmark, number_of_nodes, load_file, version='decomposition', robust=False,
                         number_of_scenarios=None, clustered_queries=None, optimality_gap=None, time_limit=None,
                         reallocation=None, data_modifications=None):
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
    elif version == 'uncertain':
        assert clustered_queries is not None
        folder += f'uncertain/F{clustered_queries}'
    elif version == 'two-step':
        folder += 'two-step'
    elif version == 'three-step':
        folder += 'three-step'
    elif version == 'optimality_gap':
        assert optimality_gap is not None
        folder += 'optimality_gap'
    elif version == 'time_limit':
        folder += 'time_limit'
        assert time_limit is not None
    else:
        assert False, f'Unsupported version: {version}'
    if robust:
        folder += '/robust'
    elif reallocation:
        reallocation_goal, reallocation_approach = reallocation
        assert reallocation_goal in ['min_realloc', 'no_realloc']
        assert reallocation_approach in ['add_all', 'add_last', 'optimal']
        folder += f'/reallocation/{reallocation_goal}/{reallocation_approach}'
    elif data_modifications:
        folder += '/data_modifications'
    if version == 'uncertain':
        search_string = f'{benchmark.name}/{folder}/sol_K{number_of_nodes}_*_S{number_of_scenarios}_*_out.txt'
        file_names = glob.glob(os.path.join(os.path.dirname(__file__), search_string))
    else:
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

        search_string = f'{benchmark.name}/{folder}/sol_K{number_of_nodes}_*_out{suffix}.txt'
        file_names = glob.glob(os.path.join(os.path.dirname(__file__), search_string))
    assert len(file_names) == 1, f'Files: {file_names} for: {benchmark.name}, version={version}, robust={robust}, {number_of_nodes}\n{search_string}'

    with open(file_names[0]) as f:
        file_txt = f.read()
        lines = file_txt.split('\n')
        if version == 'uncertain':
            summary_line = lines[2]
        else:
            summary_line = lines[0]
        runtime = float(summary_line.split(':')[1])

    return runtime


def sigmod_greedy(benchmark, num_backends, load=None):
    assert num_backends > 0, f'Number of nodes (= {num_backends}) must be positive'
    def updates_for_query(query):
        updates = []
        for update in benchmark._updates:
            for col in update._columns:
                if col in query._columns:
                    updates.append(update)
                    break # test next update
        return updates

    query_weights = []
    query_sizes = []

    # Normalize query load
    sum_of_query_load = sum(query._load for query in benchmark._queries + benchmark._updates)
    assert type(sum_of_query_load) == Fraction

    for query in benchmark._queries + benchmark._updates:
        query_weights.append(query._load / sum_of_query_load)
        query_sizes.append(sum([column.size() for column in query._columns]))
    # print(query_sizes)
    # print(query_weights)
    queries = list(range(len(benchmark._queries)))

    rest_weights = list(query_weights)

    def get_key(i):
        query = benchmark._queries[i]
        w = rest_weights[i]
        s = query_sizes[i]
        for update in updates_for_query(query):
            w += update._load
            for col in update._columns:
                if col not in query._columns:
                    s += col.size()
        return w * s, -i
    queries.sort(key=get_key, reverse=True)
    # print(queries)
    current_load = [0 for _ in range(num_backends)]

    if load is None:
        load = [Fraction(1, num_backends) for _ in range(num_backends)]
    else:
        assert(len(load) == num_backends)
    scaled_load = list(load)

    backend_fragments = [[] for _ in range(num_backends)]
    backend_q = [[] for _ in range(num_backends)]
    backend_u = [[] for _ in range(num_backends)]

    backend_q_costs = [{} for _ in range(num_backends)]

    # print(sum(rest_weights))
    # print(scaled_load)
    # print(current_load)
    # print(rest_weights)

    while len(queries) > 0:
        # print('Q-Rest weights: %s' % rest_weights)
        query = benchmark._queries[queries[0]]
        # print('Assign query %d' % queries[0])

        def all_backends_full():
            for i in range(num_backends):
                if current_load[i] < scaled_load[i]:
                    return False
            return True

        if all_backends_full():
            for i in range(num_backends):
                scaled_load[i] = current_load[i] + load[i] * query_weights[queries[0]]
            # print('(%d)New scaled load %s' % (queries[0], scaled_load))
        # print(queries)
        # print(query)
        # print(scaled_load)
        # print(current_load)
        # print(rest_weights)
        # print('-----------')
        # time.sleep(1)

        # find best fitting backend for query
        differences = []
        for b in range(num_backends):
            if current_load[b] == scaled_load[b]:
                differences.append(math.inf)
            elif current_load[b] == 0:
                differences.append(0)
            else:
                difference = 0
                for column in query._columns:
                    if column not in backend_fragments[b]:
                        difference += column.size()
                differences.append(difference)
        backend = differences.index(min(differences))
        # print('Choose backend %d' % backend)
        # assign query to backend
        if queries[0] not in backend_q[backend]:
            backend_q[backend].append(queries[0])
            backend_q_costs[backend][queries[0]] = 0

            # add fragments of query
            for col in query._columns:
                if col not in backend_fragments[backend]:
                    backend_fragments[backend].append(col)
            # add fragments of related updates
            for i, update in enumerate(benchmark._updates):
                if i not in backend_u[backend]:
                    for col in update._columns:
                        if col in backend_fragments[backend]:
                            # update is related to query
                            backend_u[backend].append(i)
                            current_load[backend] += update._load
                            for c in update._columns:
                                if c not in backend_fragments[backend]:
                                    backend_fragments[backend].append(col)
                            break # test next update query
        # print('Fragments for %d: %s' % (queries[0], [col._table._name for col in backend_fragments[backend]]))

        if current_load[backend] >= scaled_load[backend]:
            scaled_load[backend] = current_load[backend] + load[backend] * query_weights[queries[0]]

        if rest_weights[queries[0]] > scaled_load[backend] - current_load[backend]:
            backend_q_costs[backend][queries[0]] += (scaled_load[backend] - current_load[backend]) / query_weights[queries[0]] #new
            rest_weights[queries[0]] -= scaled_load[backend] - current_load[backend]
            current_load[backend] = scaled_load[backend]
            queries.sort(key=get_key, reverse=True)

        else:
            current_load[backend] += rest_weights[queries[0]]
            backend_q_costs[backend][queries[0]] += rest_weights[queries[0]] / query_weights[queries[0]] #new
            queries = queries[1:]
        # print('Current load %s' % current_load)
        # print('-------------------------------')

    # test for equal load distribution of backends
    if benchmark._updates is None:
        for b_id, b in enumerate(backend_q):
            s = 0
            for q_id in b:
                s += backend_q_costs[b_id][q_id] * query_weights[q_id]
            assert s == Fraction(1, num_backends)

    return backend_q_costs


def set_column_sizes(tables, load_file):
    """Set queried column sizes (by measure_fragment_sizes.py) from Postgres"""
    with open(os.path.join(os.path.dirname(__file__), load_file[:-4] + '/fragment_sizes.json')) as f:
        column_sizes = json.load(f)
    for table in tables:
        for column in table._columns:
            column._size = column_sizes[column._name]
