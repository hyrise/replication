import os
import sqlite3


create_result_table_stmt = '''
create table result(
    allocation              text,
    query_scheduling        text,
    number_of_replicas      int,
    connections_per_replica int,
    number_of_streams       int,
    duration                int,
    run_id                  int,
    robust                  boolean,
    failure_node_id         int,
    thread_id               int,
    queue_id                int,
    time_stamp              real,
    stream_id               int,
    query_id                int,
    operation               text
)
'''


def create_result_table(file_name, db_name=None):
    if db_name is None:
        conn = sqlite3.connect(':memory:')
    else:
        conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(create_result_table_stmt)
    with open(file_name) as f:
        for line in f:
            if line.startswith('---') or line.startswith('allocation') or line.startswith('canc') \
                    or line.startswith(' ') or line.startswith('Q') or line.startswith('REPLICA') \
                    or line.startswith('FINISH'):
                continue
            values = line.strip().split(';')
            if len(values) == 13:
                # Append robust information for non-robust benchmarks
                values = values[:7] + ['False', 'None'] + values[7:]
            else:
                assert len(values) == 15
            for i in [2, 3, 4, 5, 6, 9, 10, 12, 13]:
                values[i] = int(values[i])
            values[7] = True if values[7] == 'True' else False
            values[8] = None if values[8] == 'None' else int(values[8])
            values[11] = float(values[11])
            values = tuple(values)
            c.execute('insert into result(allocation, query_scheduling, number_of_replicas, connections_per_replica, '
                      'number_of_streams, duration, run_id, robust, failure_node_id, thread_id, queue_id, time_stamp, '
                      'stream_id, query_id, operation) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', values)
    conn.commit()
    return conn


def open_result_table(db_name):
    conn = sqlite3.connect(db_name)
    return conn


def get_queue_actions(connection, allocation, number_of_replicas, connections_per_replica=1, query_scheduling='dynamic', run_id=0, failure_node_id=None, thread_id='ANY', operation='ANY', interval=None):
    assert operation in ['O', 'I', 'ANY']
    if interval is not None:
        assert len(interval) == 2
    if failure_node_id is None:
        robust_filter = ' and failure_node_id is NULL '
    else:
        robust_filter = f' and failure_node_id = {failure_node_id} '
    operation_filter = ''
    if operation != 'ANY':
        operation_filter = f' and operation = "{operation}" '
    interval_filter = ''
    if interval is not None:
        interval_filter = f' and time_stamp > {interval[0]} and time_stamp <= {interval[1]} '
    thread_id_filter = ''
    if thread_id != 'ANY':
        thread_id_filter = f' and thread_id = {thread_id} '
    c = connection.cursor()
    c.execute(f'select queue_id, time_stamp, stream_id, query_id, operation from result where allocation = "{allocation}" and query_scheduling = "{query_scheduling}"'
              f' {robust_filter} {operation_filter} {interval_filter} {thread_id_filter} and number_of_replicas = {number_of_replicas} and connections_per_replica = {connections_per_replica} and run_id = {run_id} order by time_stamp, stream_id')
    r = c.fetchall()
    return r


def get_stream_orders(connection, allocation, number_of_replicas, failure_node_id=None, connections_per_replica=1, query_scheduling='dynamic', run_id=0):
    c = connection.cursor()
    if failure_node_id is None:
        robust_filter = ' and failure_node_id is NULL '
    else:
        robust_filter = f' and failure_node_id = {failure_node_id} '
    c.execute(
        f'select distinct stream_id from result where allocation = "{allocation}" and query_scheduling = "{query_scheduling}"'
        f' {robust_filter} and number_of_replicas = {number_of_replicas} and connections_per_replica = {connections_per_replica} and run_id = {run_id} order by stream_id')
    stream_order_ids = [r[0] for r in c.fetchall()]
    print(stream_order_ids)
    stream_orders = []
    for stream_order_id in stream_order_ids:
        c.execute(
            f'select query_id, time_stamp from result where allocation = "{allocation}" and query_scheduling = "{query_scheduling}" and stream_id = {stream_order_id}'
            f' {robust_filter} and number_of_replicas = {number_of_replicas} and operation = "I" and connections_per_replica = {connections_per_replica} and run_id = {run_id} order by time_stamp')
        stream_order = [r[0] for r in c.fetchall()[:20]]
        assert len(stream_order) == 20
        # print(stream_order)
        stream_orders.append(stream_order)
    return stream_orders
