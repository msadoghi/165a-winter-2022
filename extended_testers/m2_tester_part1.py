from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')
# Create a table  with 10 columns
#   Student Id and 9 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 10, 0)

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
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

# Check inserted records using select query
for key in keys:
    q = [
        [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        [1, 0, 1, 0, 1, 0, 1, 1, 1, 1],
        [1, 0, 1, 0, 1, 0, 1, 0, 1, 0],
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0]]
    record = query.select(key, 0, q[key % len(q)])[0]
    error = False
    r = records[key].copy()
    if len(r) != len(record.columns):
        r = [v for i, v in enumerate(r) if q[key % len(q)][i] != 0]
    for i, column in enumerate(record.columns):
        if q[key % len(q)][i] and column != r[i]:
            error = True
    if error:
        print(q[key % len(q)])
        print('select error on', key, ':', record.columns, ', correct:', r)
    else:
        pass
        # print('select on', key, ':', record)
print("Select finished")

# x update on every column
for _ in range(number_of_updates):
    for key in keys:
        updated_columns = [None, None, None, None,
                           None, None, None, None, None, None]
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 2000)
            updated_columns[i] = value
            # copy record to check
            original = records[key].copy()
            # update our test directory
            records[key][i] = value
            query.update(key, *updated_columns)
            record = query.select(key, 0, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])[0]
            error = False
            for j, column in enumerate(record.columns):
                if column != records[key][j]:
                    error = True
            if error:
                print('update error on', original)
                print('\t\t updated', updated_columns)
                print('\t\t yours', record.columns)
                print('\t\t correct', records[key])
            else:
                pass
                # print('update on', original, 'and', updated_columns, ':', record)
            updated_columns[i] = None
print("Update finished")

for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]],
              ']: ', result, ', correct: ', column_sum)
    else:
        pass
        # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
print("Aggregate finished")
db.close()
