from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
# Create a table  with 5 columns
#   Student Id and 4 grades
#   The first argument is name of the table
#   The second argument is the number of columns
#   The third argument is determining the which columns will be primay key
#       Here the first column would be student id and primary key
grades_table = db.create_table('Grades', 7, 0)

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 10000
number_of_aggregates = 100
seed(3589901)

for i in range(0, number_of_records):
    key = 92106429 + randint(0, number_of_records)

    # skip duplicate keys
    while key in records:
        key = 92106429 + randint(0, number_of_records)

    records[key] = [key,
                    randint(0, 20),
                    randint(0, 20),
                    randint(0, 20),
                    randint(0, 20),
                    randint(0, 20),
                    randint(0, 20)]
    query.insert(*records[key])
    # print('inserted', records[key])
print("Insert finished")

# Check inserted records using select query
for key in records:
    q = [[1, 1, 1, 1, 1, 1, 1], [1, 0, 1, 0, 1, 0, 1], [0, 0, 0, 0, 0, 0, 1]]
    record = query.select(key, 0, q[key % 3])[0]
    error = False
    r = records[key].copy()
    if len(r) != len(record.columns):
        r = [v for i, v in enumerate(r) if q[key % 3][i] != 0]
    for i, column in enumerate(record.columns):
        if q[key % 3][i] and column != r[i]:
            error = True
    if error:
        print(q[key % 3])
        print('select error on', key, ':', record.columns, ', correct:', r)
    else:
        pass
    

for key in records:
    updated_columns = [None, None, None, None, None, None, None]
    for i in range(2, grades_table.num_columns):
        # updated value
        value = randint(0, 20)
        updated_columns[i] = value
        # copy record to check
        original = records[key].copy()
        # update our test directory
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns,
                  ':', record, ', correct:', records[key])
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)
        updated_columns[i] = None

keys = sorted(list(records.keys()))
# aggregate on every column
for c in range(0, grades_table.num_columns):
    for i in range(0, number_of_aggregates):
        r = sorted(sample(range(0, len(keys)), 2))
        # calculate the sum form test directory
        column_sum = sum(
            map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]],
                  ']: ', result, ', correct: ', column_sum)
        else:
            pass
            # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
