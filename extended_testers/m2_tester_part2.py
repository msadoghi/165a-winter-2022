from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 10000
number_of_aggregates = 300
number_of_updates = 15

seed(6589503)
for i in range(0, number_of_records):
    key = 92106429 + i
    records[key] = [key,
                    randint(0, 140),
                    randint(0, 240),
                    randint(0, 940),
                    randint(0, 840),
                    randint(0, 740),
                    randint(0, 640),
                    randint(0, 540),
                    randint(0, 440),
                    randint(0, 340)]

# Simulate updates
keys = sorted(list(records.keys()))
for _ in range(number_of_updates):
    for key in keys:
        for j in range(2, grades_table.num_columns):
            value = randint(0, 2000)
            records[key][j] = value
keys = sorted(list(records.keys()))

# Check records that were presisted in part 1
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
print("Select finished")


for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(
        map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]],
              ']: ', result, ', correct: ', column_sum)
print("Aggregate finished")

deleted_keys = sample(keys, 100)
for key in deleted_keys:
    query.delete(key)
    records.pop(key, None)

db.close()
