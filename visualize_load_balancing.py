import curses

from queue_analyser import create_result_table, get_queue_actions, get_stream_orders, open_result_table
from partial_replication import TPCH, Benchmark_accessed_columns_queries
from setup_benchmark import get_replica_configurations

LOAD_FILE = 'tpch/costs_20190916.txt'

REPLICA_INFO_OFFSET = 10, 37
STREAM_INFO_OFFSET = 3, 1

LINES_PER_REPLICA = 4


def get_query_shares(number_of_replicas, allocation):
    QUERY_COSTS = []
    with open(LOAD_FILE) as f:
        for line in f:
            QUERY_COSTS.append(int(float(line) * 1000))
    OVERALL_QUERY_COSTS = sum(QUERY_COSTS)
    config = get_replica_configurations(benchmark=TPCH(), num_backends=number_of_replicas, load_file=LOAD_FILE,
                                        allocation=allocation, robust=True, failure_node=None)
    print(config)
    all_query_shares = []
    for replica_id in range(len(config)):
        con = config[replica_id]
        query_shares = []
        for query_id, share in con.items():
            query_shares.append((query_id, round(
                100 * float(number_of_replicas * share * QUERY_COSTS[query_id] / OVERALL_QUERY_COSTS))))
        all_query_shares.append(query_shares)
    return all_query_shares


def visualize_queues_curses(stdscr, connection, allocation, number_of_replicas, connections_per_replica=1,  failure_node_id=None, query_scheduling='dynamic', run_id=0):
    curses.curs_set(0)
    stdscr.addstr(REPLICA_INFO_OFFSET[0] + number_of_replicas * 5 + 2, 0, ' ')
    for i in range(1, curses.COLORS):
        curses.init_pair(i, curses.COLOR_BLACK, i)
        stdscr.addstr(str(i), curses.color_pair(i))

    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_BLACK, curses.COLOR_YELLOW)
    curses.init_pair(5, curses.COLOR_WHITE, curses.COLOR_BLUE)
    curses.init_pair(17, curses.COLOR_BLACK, 238)

    #                                   #00                  #05                 #10                      #15                      #20
    for query_id, color_id in enumerate([22, 203, 32, 14, 46, 56, 58, 77, 89, 95, 125, 161, 179, 190, 197, 200, 23, 214, 230, 255, 40, 248]):
        curses.init_pair(100 + query_id, curses.COLOR_BLACK, color_id)

    QUERY_SHARES = get_query_shares(number_of_replicas, allocation)

    STREAM_ORDERS = get_stream_orders(connection, allocation, number_of_replicas, failure_node_id)

    DATA_SHARES = []
    q_list = list(range(22))
    q_list.remove(16)
    q_list.remove(19)
    overall_accessed_columns = Benchmark_accessed_columns_queries(TPCH(), q_list)
    overall_accessed_size = sum([c.size() for c in overall_accessed_columns])
    for replica_id in range(number_of_replicas):
        queries = [query_id for query_id, _ in QUERY_SHARES[replica_id]]
        accessed_columns = Benchmark_accessed_columns_queries(TPCH(), queries)
        accessed_size = sum([c.size() for c in accessed_columns])
        DATA_SHARES.append(int(accessed_size/overall_accessed_size * 100))

    QUERY_COSTS = []
    with open(LOAD_FILE) as f:
        for line in f:
            QUERY_COSTS.append(int(float(line) * 1000))

    QUEUES = [[] for _ in range(number_of_replicas)]
    IN_EXECUTION = [None for _ in range(number_of_replicas)]
    EXECUTED_QUEUES = [[] for _ in range(number_of_replicas)]

    # Initialize UI
    stdscr.addstr(1, REPLICA_INFO_OFFSET[1], 'Exploration of Dynamic Query-Based Load-Balancing for Partially Replicated Database Systems', curses.A_BOLD)
    stdscr.addstr(3, REPLICA_INFO_OFFSET[1],
                  f'Benchmark: TPC-H     #Streams: {len(STREAM_ORDERS)}     #Nodes: {number_of_replicas}     Failed Node: {failure_node_id + 1}     Fragment Allocation Strategy: Rabl and Jacobsen')
    stdscr.addstr(5, REPLICA_INFO_OFFSET[1],
                  f'Benchmark Queries and their Workload Shares: ')
    for query_id, query_costs in enumerate(QUERY_COSTS):
        if query_costs != 0:
            stdscr.addstr(f'{query_id + 1}{" " * (query_costs//320 - len(str(query_id + 1)))}', curses.color_pair(100 + query_id))

    stdscr.addstr(8, REPLICA_INFO_OFFSET[1],
                  f'Data Replication Factor:         Progress:            Queries per Second:        Average Utilization (Executed/Planned Load):', curses.A_BOLD)
    stdscr.addstr(8, REPLICA_INFO_OFFSET[1] + 25, '%.2f' % (sum(DATA_SHARES) / 100))

    REPLICA_BOX_WIDTH = 130

    for queue_id, queue in enumerate(QUEUES):
        # Draw borders
        if queue_id == failure_node_id:
            color = curses.color_pair(3)
        else:
            color = curses.A_NORMAL
        stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA, REPLICA_INFO_OFFSET[1] + 0, curses.ACS_ULCORNER | color)
        stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA, REPLICA_INFO_OFFSET[1] + REPLICA_BOX_WIDTH - 1, curses.ACS_URCORNER | color)
        stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 3, REPLICA_INFO_OFFSET[1] + REPLICA_BOX_WIDTH - 1, curses.ACS_LRCORNER | color)
        stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 3, REPLICA_INFO_OFFSET[1] + 0, curses.ACS_LLCORNER | color)
        for x in range(1, REPLICA_BOX_WIDTH - 1):
            stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA, REPLICA_INFO_OFFSET[1] + x, curses.ACS_HLINE | color)
            stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 3, REPLICA_INFO_OFFSET[1] + x, curses.ACS_HLINE | color)
        for y in range(1, 3):
            stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + y, REPLICA_INFO_OFFSET[1], curses.ACS_VLINE | color)
            stdscr.addch(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + y, REPLICA_INFO_OFFSET[1] + REPLICA_BOX_WIDTH - 1, curses.ACS_VLINE | color)

        # Draw labels
        if queue_id == failure_node_id:
            stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA, REPLICA_INFO_OFFSET[1] + 8, f'REPLICA NODE {queue_id + 1} (FAILED)', curses.A_BOLD | color)
        else:
            stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA, REPLICA_INFO_OFFSET[1] + 8,
                          f'REPLICA NODE {queue_id + 1}', curses.A_BOLD)
        stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 1, REPLICA_INFO_OFFSET[1] + 1, f'Executable with     Data:')
        stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 1, REPLICA_INFO_OFFSET[1] + 17, f'{DATA_SHARES[queue_id]}%', curses.A_BOLD)
        stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 1, REPLICA_INFO_OFFSET[1] + 56, f'Planned Load:')
        stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + 1, f'Queued:')
        stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + 38, f'In Execution:')
        stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + 56, f'Executed Load:')

        # Draw assigned load
        current_offset = 71
        current_offset_added = 27
        for query_id, share in sorted(QUERY_SHARES[queue_id], key=lambda i: i[0]):
            stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 1,
                          REPLICA_INFO_OFFSET[1] + current_offset_added,
                          f'{query_id + 1}', curses.color_pair(100 + query_id))
            current_offset_added += len(str(query_id + 1))
            if share != 0:
                share //= 2
                stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 1, REPLICA_INFO_OFFSET[1] + current_offset,
                              f'{query_id + 1}{" " * (share - len(str(query_id + 1)))}', curses.color_pair(100 + query_id))
                current_offset += share

    stdscr.addstr(STREAM_INFO_OFFSET[0], STREAM_INFO_OFFSET[1] + 12, f'QUERY STREAMS', curses.A_BOLD)
    for stream_id, stream_order in enumerate(STREAM_ORDERS):
        current_offset = 3
        for query_id in stream_order[::-1]:
            stdscr.addstr(STREAM_INFO_OFFSET[0] + stream_id + 1, STREAM_INFO_OFFSET[1], '%2d' % (stream_id + 1), curses.A_BOLD)
            stdscr.addstr(STREAM_INFO_OFFSET[0] + stream_id + 1, STREAM_INFO_OFFSET[1] + current_offset,
                          f'{query_id + 1}', curses.color_pair(100 + query_id))
            current_offset += len(str(query_id + 1))

    stdscr.refresh()

    queue_actions = get_queue_actions(connection, allocation=allocation, number_of_replicas=number_of_replicas, connections_per_replica=connections_per_replica, query_scheduling=query_scheduling, run_id=run_id, failure_node_id=failure_node_id)
    current_position = 0
    while True:
        key = stdscr.getch()
        # stdscr.addstr(REPLICA_INFO_OFFSET[0] + number_of_replicas * 5 + 1, REPLICA_INFO_OFFSET[1] + 0, f'key pressed: {key}')
        update_stream_orders = False
        execution_queues_to_redraw = []
        if key in (curses.KEY_RIGHT, 258):
            if current_position == len(queue_actions) - 1:
                continue
            queue_id, time_stamp, stream_id, query_id, operation = queue_actions[current_position]
            if operation == 'I':
                QUEUES[queue_id].append(query_id)
                update_stream_orders = True
                previous_query_id = query_id

                for _queue_id, query_info in enumerate(IN_EXECUTION):
                    if query_info and query_info[1] == stream_id:
                        IN_EXECUTION[_queue_id] = None
                        stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 2,
                                      REPLICA_INFO_OFFSET[1] + 52, 2 * ' ')
                        EXECUTED_QUEUES[_queue_id] = [query_info] + EXECUTED_QUEUES[_queue_id]
                        execution_queues_to_redraw.append(_queue_id)
                        break

            else:
                assert operation == 'O'
                QUEUES[queue_id].remove(query_id)
                # EXECUTED_QUEUES[queue_id] = [query_id] + EXECUTED_QUEUES[queue_id]

                assert IN_EXECUTION[queue_id] is None
                IN_EXECUTION[queue_id] = query_id, stream_id
                # Visualize "In Execution"
                stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2,
                              REPLICA_INFO_OFFSET[1] + 52, str(query_id + 1), curses.color_pair(100 + query_id))

            current_position += 1
        elif key in (curses.KEY_LEFT, 259):
            if current_position == 0:
                continue
            current_position -= 1
            queue_id, time_stamp, stream_id, query_id, operation = queue_actions[current_position]
            # time_stamp = queue_actions[current_position - 1][1]
            if operation == 'I':
                assert QUEUES[queue_id][-1] == query_id
                QUEUES[queue_id] = QUEUES[queue_id][:-1]
                update_stream_orders = True
                previous_query_id = STREAM_ORDERS[stream_id][STREAM_ORDERS[stream_id].index(query_id) - 1]

                for _queue_id, _executed_queue in enumerate(EXECUTED_QUEUES):
                    if len(_executed_queue) == 0:
                        continue
                    query_info = _executed_queue[0]
                    if query_info and query_info == (previous_query_id, stream_id):
                        q_id = query_info[0]
                        IN_EXECUTION[_queue_id] = q_id, stream_id
                        stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 2,
                                      REPLICA_INFO_OFFSET[1] + 52, str(q_id + 1), curses.color_pair(100 + q_id))
                        EXECUTED_QUEUES[_queue_id] = EXECUTED_QUEUES[_queue_id][1:]
                        execution_queues_to_redraw.append(_queue_id)
                        break

            else:
                assert operation == 'O'
                QUEUES[queue_id] = [query_id] + QUEUES[queue_id]
                # EXECUTED_QUEUES[queue_id].remove(query_id)

                for _queue_id, query_info in enumerate(IN_EXECUTION):
                    if query_info and query_info[1] == stream_id:
                        IN_EXECUTION[_queue_id] = None
                        stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 2,
                                      REPLICA_INFO_OFFSET[1] + 52, 2 * ' ')
        else:
            continue

        if update_stream_orders:
            current_offset = 3
            color = True
            for stream_order_query_id in STREAM_ORDERS[stream_id][::-1]:
                if stream_order_query_id != STREAM_ORDERS[stream_id][-1] and stream_order_query_id == previous_query_id:
                    color = False
                if not color:
                    stdscr.addstr(STREAM_INFO_OFFSET[0] + stream_id + 1, STREAM_INFO_OFFSET[1] + current_offset,
                                  f'{stream_order_query_id + 1}', curses.color_pair(237))
                else:
                    stdscr.addstr(STREAM_INFO_OFFSET[0] + stream_id + 1, STREAM_INFO_OFFSET[1] + current_offset,
                                  f'{stream_order_query_id + 1}', curses.color_pair(100 + stream_order_query_id))
                current_offset += len(str(stream_order_query_id + 1))

        overall_costs = 0
        for queue in QUEUES:
            overall_costs += sum([QUERY_COSTS[query_id] for query_id in queue])

        number_of_executed_queries = 0
        for queue in EXECUTED_QUEUES:
            number_of_executed_queries += len(queue)

        # Draw execution summary
        stdscr.addstr(8, REPLICA_INFO_OFFSET[1] + 43, ' ' * 8)
        stdscr.addstr(8, REPLICA_INFO_OFFSET[1] + 43, '%.2fs' % time_stamp)

        stdscr.addstr(8, REPLICA_INFO_OFFSET[1] + 74, ' ' * 6)
        if time_stamp > 0:
            stdscr.addstr(8, REPLICA_INFO_OFFSET[1] + 74, '%.2f' % (number_of_executed_queries/time_stamp))

        queue = QUEUES[queue_id]
        # Draw queue summary
        # queue_costs = sum([QUERY_COSTS[query_id] for query_id in queue])
        # color = curses.color_pair(1)
        # if queue_costs < 0.25 * avg_costs or queue_costs > 1.75 * avg_costs:
        #     color = curses.color_pair(3)
        # elif queue_costs < 0.5 * avg_costs or queue_costs > 1.5 * avg_costs:
        #     color = curses.color_pair(2)
        #
        # stdscr.addstr(queue_id * LINES_PER_REPLICA + 3, 1, ' ' * (200 - 2))
        # stdscr.addstr(queue_id * LINES_PER_REPLICA + 3, 1, str(sum([QUERY_COSTS[query_id] for query_id in queue])), color)
        #
        # if len(queue) > 10:
        #     str_queue = queue[:3] + ['...'] + queue[-3:]
        # else:
        #     str_queue = queue
        # stdscr.addstr(queue_id * LINES_PER_REPLICA + 3, 8, str(str_queue))

        # Visualize queue
        current_offset = 9
        stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + current_offset, ' ' * (36 - current_offset))
        visualized_queue = list(queue)
        while len(''.join([(str(query_id + 1)) for query_id in visualized_queue])) > ((38 - current_offset) - 2):
            middle = len(visualized_queue) // 2
            visualized_queue = visualized_queue[:middle] + visualized_queue[middle + 1:]
        if visualized_queue != queue:
            visualized_queue[len(visualized_queue) // 2] = None
        for query_id in visualized_queue[::-1]:
            if query_id is None:
                stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + current_offset, f'*')
                current_offset += 1
            else:
                stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + current_offset,
                          f'{query_id + 1}', curses.color_pair(100 + query_id))
                current_offset += len(str(query_id + 1))

        # print execution queue
        # stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, 0, ' ' * curses.COLS)
        # stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, 0, str(sum([QUERY_COSTS[query_id] for query_id in EXECUTED_QUEUES[queue_id]])))
        # for _queue_id in execution_queues_to_redraw:

        overall_executed = 0
        for _queue_id in range(number_of_replicas):
            # stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 3, REPLICA_INFO_OFFSET[1] + 8, ' ' * 200)
            # stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 3, REPLICA_INFO_OFFSET[1] + 8, str(EXECUTED_QUEUES[_queue_id][:10]))

            execution_counts = {}
            for query_id, _ in EXECUTED_QUEUES[_queue_id]:
                if query_id not in execution_counts:
                    execution_counts[query_id] = 0
                execution_counts[query_id] += 1
            # stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 2, current_offset, str(execution_counts))
            execution_shares = []
            for query_id, execution_counts in execution_counts.items():
                execution_shares.append((query_id, round(execution_counts * (QUERY_COSTS[query_id]/10) / time_stamp)))
            # stdscr.addstr(REPLICA_INFO_OFFSET[0] + queue_id * LINES_PER_REPLICA + 3, current_offset, str(execution_shares))

            # Display execution share
            overall_execution_share = sum([execution_share for _, execution_share in execution_shares])
            overall_executed += overall_execution_share
            stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 2,
                          REPLICA_INFO_OFFSET[1] + REPLICA_BOX_WIDTH - 5, ' ' * 4)
            stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 2,
                          REPLICA_INFO_OFFSET[1] + REPLICA_BOX_WIDTH - 5, f'{overall_execution_share:3}%', curses.A_BOLD)

            current_offset = 71
            # print(execution_shares)
            stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + current_offset, ' ' * 54)
            for query_id, share in sorted(execution_shares, key=lambda i: i[0]):
                share //= 2
                stdscr.addstr(REPLICA_INFO_OFFSET[0] + _queue_id * LINES_PER_REPLICA + 2, REPLICA_INFO_OFFSET[1] + current_offset,
                              f'{query_id + 1}{" " * (share - len(str(query_id + 1)))}', curses.color_pair(100 + query_id))
                current_offset += share

        stdscr.addstr(8, REPLICA_INFO_OFFSET[1] + REPLICA_BOX_WIDTH - 5, ' ' * 4)
        stdscr.addstr(8, REPLICA_INFO_OFFSET[1] + REPLICA_BOX_WIDTH - 5, f'{overall_executed//number_of_replicas:3}%', curses.A_BOLD)


def main(stdscr):
    # Use cached table (which is faster)
    # file_name = 'results_1571561461.csv'
    # connection = create_result_table(file_name, 'example17.db')
    connection = open_result_table('example17.db')
    visualize_queues_curses(stdscr, connection, 'greedy', 5, failure_node_id=0)


if __name__ == '__main__':
    curses.wrapper(main)
