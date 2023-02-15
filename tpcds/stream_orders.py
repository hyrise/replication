import random

random.seed(17)

queries = list(range(1, 100))

tpcds_stream_orders = [random.sample(queries, k=len(queries)) for _ in range(256)]
