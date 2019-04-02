# Fragment Allocation and Workload Distribution

Given a partitioned database consisting of disjoint fragments.
Each fragment has a size.
Given a set of queries, classified by the fragments they access.
Each query accounts for a workload share, derived by query costs and frequency.

We want to load balance queries to multiple processing nodes in a way that balances the load evenly while minimizing the overall memory consumption of the accessed fragments at each node.

For our research, we use the TPC-H and TPC-DS as reproducible examples. We obtain model inputs in the following way

### Fragment Sizes
Fragment/column sizes are modeled by using the PostgreSQL function pg_column_size().
In case there is a single column index on an attribute, i.e., the attribute is part of a primary key, the according fragment size is increased by the index size. We use the command pg_table_size(index name) to calculate index sizes.

### Query Costs
We modeled query costs as average processing time for a query in PostgreSQL with random template parameters.

## TPC-H
[TPC-H fragment sizes](https://github.com/hyrise/replication/blob/master/tpch/tpch_colum_sizes_postgres_single_index.py)

The TPC-H benchmark consists of 22 queries.
20 of 22 TPC-H queries could be executed without a timeout (120 s).
[TPC-H execution costs](https://github.com/hyrise/replication/blob/master/tpch/tpch_load_postgres_index_single.txt) (A '0' indicates a timeout, i.e., ommission of the query).

## TPC-DS
[TPC-DS fragment sizes](https://github.com/hyrise/replication/blob/master/tpcds/tpcds_colum_sizes_postgres_single_index.py)

The TPC-DS benchmark consists of 99 queries.
80 of 99 TPC-DS queries could be executed.
[TPC-DS execution costs](https://github.com/hyrise/replication/blob/master/tpcds/tpcds_load_postgres_index_single.txt) (A '0' indicates a timeout, i.e., ommission of the query).
