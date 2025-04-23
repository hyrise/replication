# Fragment Allocation and Workload Distribution

Given a partitioned database consisting of disjoint fragments/partitions.
Each fragment has a size.
Given a set of queries, classified by the fragments they access.
Each query accounts for a workload share, derived from query costs and frequency.

We want to load balance queries to multiple processing nodes in a way that balances the load evenly while minimizing the overall memory consumption of the accessed fragments at each node.

For our research, we use the TPC-H, TPC-DS, and a real-world workload as reproducible examples.
For the real-world workload, we have anonymized metadata, which allows us to directly derive the model inputs.
For TPC-H and TPC-DS, we obtain model inputs in the following way

### Fragment Sizes
Fragment/column sizes are derived from the database catalog.
For PostgreSQL, we use the function pg_column_size().
In case there is a single column index on an attribute, i.e., the attribute is part of a primary key, the corresponding fragment size is increased by the index size. We use the command pg_table_size(index name) to calculate index sizes.
Fragment sizes depend, for example, on the scale factor, encoding scheme, and the used database system.
Hence, there may be multiple versions for the same workload in the repository.

### Query Costs
We modeled query costs as the average processing time for a query with random template parameters.
Query costs depend, for example, on the hardware and used database system.
Hence, there may be multiple versions for the same workload in the repository.

## TPC-H
[TPC-H fragment sizes](https://github.com/hyrise/replication/blob/main/tpch/tpch_colum_sizes_postgres_single_index.py)

The TPC-H benchmark consists of 22 queries.
20 of 22 TPC-H queries could be executed without a timeout (120 s).
[TPC-H execution costs](https://github.com/hyrise/replication/blob/main/tpch/tpch_load_postgres_index_single.txt) (A '0' indicates a timeout, i.e., the omission of the query).

## TPC-DS
[TPC-DS fragment sizes](https://github.com/hyrise/replication/blob/main/tpcds/tpcds_colum_sizes_postgres_single_index.py)

The TPC-DS benchmark consists of 99 queries.
80 of 99 TPC-DS queries could be executed.
[TPC-DS execution costs](https://github.com/hyrise/replication/blob/main/tpcds/tpcds_load_postgres_index_single.txt) (A '0' indicates a timeout, i.e., the omission of the query).

## TPC-DS for EDBT 2021
[TPC-DS model input](https://github.com/hyrise/replication/blob/main/tpcds_edbt/tpcds_2021edbt_ampl_input.txt)

The TPC-DS benchmark consists of 99 queries.
94 of 99 TPC-H queries could be executed without a timeout (120 s).

## Real-World Workload
Anonymized [model input](https://github.com/hyrise/replication/blob/main/real_world_workload/real_world_workload.txt) of a real-world workload, consisting of 4461 queries accessing subsets of 344 fragments:
* fragment sizes a_i, i=1,...,344
* query costs c_j, j=1,...4461
* query frequencies f_j, j=1,...4461
* accessed fragments per query q_j, j=1,...4461


# Reproducibility of Paper Results

## Fragment Allocations for Partially Replicated Databases Considering Data Modifications and Changing Workloads

Run `python reproduce_modification_reallocation.py` to generate the content of the LaTeX tables and the figures' plotted data.


# Dynamic Query-Based Load-Balancing

A terminal application using curses to visualize the load-balancing in database clusters on a query level.
Logged query queue events are stored in the SQLite3 database file `example17.db` and in CSV format `results_1571561461.csv`.

Run `python visualize_load_balancing.py` for an example. The visualisation requires some space (see screenshot below). Please ensure that your terminal width is at least 170 and height is above 50.

![Screenshot of the curses application](https://github.com/hyrise/replication/blob/main/screenshot.png)


# Index Selection

## TPC-DS

[Model input](https://github.com/hyrise/replication/blob/main/index_selection/tpcds_3.txt) for index selection:
* 8343 index candidates i with one, two, or three attributes
* query costs c_j(i) (third column) for query j (first column) and index i (second column) if index lowers the costs (i=0 specifies query costs without applying indexes)
* index sizes m_i, i=0,...,8343
