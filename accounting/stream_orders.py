import random
import os

random.seed(17)

frequencies = []
with open(os.path.join(os.path.dirname(__file__), 'query_frequencies_full.txt')) as f:
    for line in f:
        frequencies.append(int(line))
assert len(frequencies) == 4461

queries = []
for query_id, frequency in enumerate(frequencies):
    for _ in range(frequency):
        queries.append(query_id + 1)

# print(len(queries))

# print('generate streams...')
accounting_stream_orders = [queries for _ in range(10)]
# accounting_stream_orders = [random.sample(queries, k=len(queries)) for _ in range(10)]
# print('streams generated...')
