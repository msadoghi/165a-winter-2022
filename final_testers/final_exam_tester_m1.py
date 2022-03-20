from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

score = 0
def normal_tester():
    print("Checking exam M1 normal tester");
    global score
    db = Database()
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
    seed(3562901)

    for i in range(0, number_of_records):
        key = 92106429 + randint(0, number_of_records)

        #skip duplicate keys
        while key in records:
            key = 92106429 + randint(0, number_of_records)

        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
        # print('inserted', records[key])
    print("Insert finished")

    # Check inserted records using select query
    for key in records:
        # select function will return array of records 
        # here we are sure that there is only one record in t hat array
        # check for retreiving version -1. Should retreive version 0 since only one version exists.
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            raise Exception('select error on', key, ':', record.columns, ', correct:', records[key])
        else:
            pass
            # print('select on', key, ':', record)
    
    
    updated_records = {}
    for key in records:
        updated_columns = [None, None, None, None, None]
        updated_records[key] = records[key].copy()
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # update our test directory
            updated_records[key][i] = value
        query.update(key, *updated_columns)

        #check version -1 for record
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', records[key], 'and', updated_columns, ':', record.columns, ', correct:', records[key])
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)

        #check version -2 for record
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            raise Exception('update error on', records[key], 'and', updated_columns, ':', record.columns, ', correct:', records[key])
        else:
            pass
            # print('update on', original, 'and', updated_columns, ':', record)
        
        #check version 0 for record
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != updated_records[key][j]:
                error = True
        if error:
            raise Exception('update error on', records[key], 'and', updated_columns, ':', record.columns, ', correct:', updated_records[key])
    score = score + 15
    
    keys = sorted(list(records.keys()))
    # aggregate on every column 
    for c in range(0, grades_table.num_columns):
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            # calculate the sum form test directory
            # version -1 sum
            column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], c, -1)
            if column_sum != result:
                print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            else:
                pass
                # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
            # version -2 sum
            column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], c, -2)
            if column_sum != result:
                print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            else:
                pass
            # version 0 sum
            updated_column_sum = sum(map(lambda key: updated_records[key][c], keys[r[0]: r[1] + 1]))
            updated_result = query.sum_version(keys[r[0]], keys[r[1]], c, 0)
            if updated_column_sum != updated_result:
                raise Exception('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ', updated_column_sum)
            else:
                pass
    score = score + 15


def extended_tester():
    print("\n\nChecking exam M1 extended tester");
    global score
    db = Database()
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
    number_of_updates = 5
    seed(3562901)

    for i in range(0, number_of_records):
        key = 92106429 + randint(0, number_of_records)

        #skip duplicate keys
        while key in records:
            key = 92106429 + randint(0, number_of_records)

        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
        # print('inserted', records[key])
    print("Insert finished")

    # Check inserted records using select query
    for key in records:
        # select function will return array of records 
        # here we are sure that there is only one record in t hat array
        # check for retreiving version -1. Should retreive version 0 since only one version exists.
        record = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            raise Exception('select error on', key, ':', record.columns, ', correct:', records[key])
        else:
            pass
            # print('select on', key, ':', record)
    
    
    all_updates = []
    keys = sorted(list(records.keys()))
    for i in range(number_of_updates):
        all_updates.append({})
        for key in records:
            updated_columns = [None, None, None, None, None]
            all_updates[i][key] = records[key].copy()
            for j in range(2, grades_table.num_columns):
                # updated value
                value = randint(0, 20)
                updated_columns[j] = value
                # update our test directory
                all_updates[i][key][j] = value
            query.update(key, *updated_columns)
    
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

import os
import glob
import traceback
import shutil   
    
def run_test():
    start = timer()
    try:
        normal_tester()
        extended_tester()
    except Exception as e:
        print("Something went wrong")
        print(e)
        traceback.print_exc()
    end = timer()
    print("\n------------------------------------")
    print("Time taken: ", Decimal(end - start).quantize(Decimal('0.01')), "seconds")
    print("Total score: ", score)
    print("Normalized total Score for M1:", score*0.8)
    print("--------------------------------------\n")

run_test();        