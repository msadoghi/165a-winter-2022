from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

normal_p1 = True
normal_p2 = True
extended_p1 = True
extended_p2 = True
score = 0
def normal_tester():
    print("Checking exam M2 normal tester");
    global score
    global normal_p1, normal_p2, extended_p1, extended_p2
    
    if normal_p1:
        db = Database()
        db.open('./ECS165')
        # Create a table  with 5 columns
        #   Student Id and 4 grades
        #   The first argument is name of the table
        #   The second argument is the number of columns
        #   The third argument is determining the which columns will be primay key
        #       Here the first column would be student id and primary key
        grades_table = db.create_table('Grades', 5, 0)

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_aggregates = 100
        number_of_updates = 1

        seed(3562901)

        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
            query.insert(*records[key])
        keys = sorted(list(records.keys()))
        print("Insert finished")

        # Check inserted records using select query
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                print('select error on', key, ':', record, ', correct:', records[key])
            else:
                pass
                # print('select on', key, ':', record)
        print("Select finished")

        # x update on every column
        for _ in range(number_of_updates):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                # copy record to check
                original = records[key].copy()
                for i in range(2, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    records[key][i] = value
                query.update(key, *updated_columns)
                record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
                error = False
                for j, column in enumerate(record.columns):
                    if column != records[key][j]:
                        error = True
                if error:
                    raise Exception('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
                else:
                    pass
                    # print('update on', original, 'and', updated_columns, ':', record)
        print("Update finished")

        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
            result = query.sum(keys[r[0]], keys[r[1]], 0)
            if column_sum != result:
                print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            else:
                pass
                # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
        print("Aggregate finished")
        db.close()


    if normal_p2:
        db = Database()
        db.open('./ECS165')

        # Getting the existing Grades table
        grades_table = db.get_table('Grades')

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_aggregates = 100
        number_of_updates = 1

        seed(3562901)
        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

        # Simulate updates
        updated_records = {}
        keys = sorted(list(records.keys()))
        for _ in range(number_of_updates):
            for key in keys:
                updated_records[key] = records[key].copy()
                for j in range(2, grades_table.num_columns):
                    value = randint(0, 20)
                    updated_records[key][j] = value
        keys = sorted(list(records.keys()))

        # Check records that were presisted in part 1
        for key in keys:
            record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                raise Exception('select error on', key, ':', record, ', correct:', records[key])
                break
        print("Select for version -1 finished")

        # Check records that were presisted in part 1
        for key in keys:
            record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                raise Exception('select error on', key, ':', record, ', correct:', records[key])
        print("Select for version -2 finished")

        for key in keys:
            record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != updated_records[key][i]:
                    error = True
            if error:
                raise Exception('select error on', key, ':', record, ', correct:', records[key])
        print("Select for version 0 finished")
        score = score + 15

        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
            if column_sum != result:
                raise Exception('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        print("Aggregate version -1 finished")

        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
            if column_sum != result:
                raise Exception('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        print("Aggregate version -2 finished")
        

        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            updated_column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
            updated_result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
            if updated_column_sum != updated_result:
                raise Exception('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ', updated_column_sum)
        print("Aggregate version 0 finished")
        score = score + 15

        db.close()


def extended_tester():
    print("\n\nChecking exam M2 extended tester");
    global score
    global normal_p1, normal_p2, extended_p1, extended_p2
    if extended_p1:
        db = Database()
        db.open('./ECS165')
        # Create a table  with 5 columns
        #   Student Id and 4 grades
        #   The first argument is name of the table
        #   The second argument is the number of columns
        #   The third argument is determining the which columns will be primay key
        #       Here the first column would be student id and primary key
        grades_table = db.create_table('Grades', 5, 0)

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_aggregates = 100
        number_of_updates = 4

        seed(3562901)

        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
            query.insert(*records[key])
        keys = sorted(list(records.keys()))
        print("Insert finished")

        # Check inserted records using select query
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                print('select error on', key, ':', record, ', correct:', records[key])
            else:
                pass
                # print('select on', key, ':', record)
        print("Select finished")

        # x update on every column
        for _ in range(number_of_updates):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                # copy record to check
                original = records[key].copy()
                for i in range(2, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    records[key][i] = value
                query.update(key, *updated_columns)
                record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
                error = False
                for j, column in enumerate(record.columns):
                    if column != records[key][j]:
                        error = True
                if error:
                    raise Exception('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
                else:
                    pass
                    # print('update on', original, 'and', updated_columns, ':', record)
        print("Update finished")

        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
            result = query.sum(keys[r[0]], keys[r[1]], 0)
            if column_sum != result:
                print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            else:
                pass
                # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
        print("Aggregate finished")
        db.close()


    if extended_p2:
        db = Database()
        db.open('./ECS165')

        # Getting the existing Grades table
        grades_table = db.get_table('Grades')

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_aggregates = 100
        number_of_updates = 4

        seed(3562901)
        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

        # Simulate updates
        all_updates = []
        keys = sorted(list(records.keys()))
        for i in range(number_of_updates):
            all_updates.append({})
            for key in keys:
                all_updates[i][key] = records[key].copy()
                for j in range(2, grades_table.num_columns):
                    value = randint(0, 20)
                    all_updates[i][key][j] = value
        keys = sorted(list(records.keys()))
        
        try:
            # Check records that were presisted in part 1
            for version in range(-number_of_updates-1, 1, 1): # -number_of_updates-1 to 0
                expected_update = records if version <= -number_of_updates else all_updates[version + number_of_updates - 1]
                for key in keys:
                    record = query.select_version(key, 0, [1, 1, 1, 1, 1], version)[0]
                    error = False
                    for k, column in enumerate(record.columns):
                        if column != expected_update[key][k]:
                            error = True
                    if error:
                        raise Exception('select error on', key, ':', record.columns, ', correct:', expected_update[key])
                        break
                print("Select version ", version, "finished")
            score = score + 35
        except Exception as e:
            print("Something went wrong during select_version")
            print(e)

        try:
            for version in range(-number_of_updates-1, 1, 1): # -number_of_updates-1 to 0
                expected_update = records if version <= -number_of_updates else all_updates[version + number_of_updates - 1]
                for j in range(0, number_of_aggregates):
                    r = sorted(sample(range(0, len(keys)), 2))
                    column_sum = sum(map(lambda x: expected_update[x][0] if x in expected_update else 0, keys[r[0]: r[1] + 1]))
                    result = query.sum_version(keys[r[0]], keys[r[1]], 0, version)
                    if column_sum != result:
                        raise Exception('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
                print("Aggregate version ", version, "finished")
            score = score + 35
        except Exception as e:
            print("Something went wrong during sum_version")
            print(e)
        db.close()

from timeit import default_timer as timer
from decimal import Decimal

import sys
import os
import glob
import traceback
import shutil   
    
def run_test():
    global normal_p1, normal_p2, extended_p1, extended_p2
    if len(sys.argv) > 1:
        normal_p1 = "np1" in sys.argv
        normal_p2 = "np2" in sys.argv
        extended_p1 = "ep1" in sys.argv
        extended_p2 = "ep2" in sys.argv
    start = timer()
    try:
        if normal_p1:
            # Remove all files before executing extended tester
            files = glob.glob(os.path.join(os.getcwd(), "ECS165*"))
            for f in files:
                if os.path.isfile(f):
                    os.remove(f)
                else: shutil.rmtree(f)
        normal_tester()
        # Remove all files before executing extended tester
        if extended_p1:
            files = glob.glob(os.path.join(os.getcwd(), "ECS165*"))
            for f in files:
                if os.path.isfile(f):
                    os.remove(f)
                else: shutil.rmtree(f)
        extended_tester()
    except Exception as e:
        print("Something went wrong")
        print(e)
        traceback.print_exc()
    end = timer()
    print("\n------------------------------------")
    print("Time taken: ", Decimal(end - start).quantize(Decimal('0.01')), "seconds")
    print("Total score: ", score)
    print("Normalized total Score for M2:", score*0.9)
    print("--------------------------------------\n")

run_test();        