from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed


normal_p1 = True
normal_p2 = True
extended_p1 = True
extended_p2 = True

score = 0
def normal_tester():
    print("Checking exam M3 normal tester");
    global score
    global normal_p1, normal_p2, extended_p1, extended_p2
    if normal_p1:
        db = Database()
        db.open('./ECS165')

        # creating grades table
        grades_table = db.create_table('Grades', 5, 0)

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_transactions = 100
        num_threads = 8

        # create index on the non primary columns
        try:
            grades_table.index.create_index(2)
            grades_table.index.create_index(3)
            grades_table.index.create_index(4)
        except Exception as e:
            print('Index API not implemented properly, tests may fail.')

        keys = []
        records = {}
        seed(3562901)

        # array of insert transactions
        insert_transactions = []

        for i in range(number_of_transactions):
            insert_transactions.append(Transaction())

        for i in range(0, number_of_records):
            key = 92106429 + i
            keys.append(key)
            records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
            t = insert_transactions[i % number_of_transactions]
            t.add_query(query.insert, grades_table, *records[key])

        transaction_workers = []
        for i in range(num_threads):
            transaction_workers.append(TransactionWorker())
            
        for i in range(number_of_transactions):
            transaction_workers[i % num_threads].add_transaction(insert_transactions[i])



        # run transaction workers
        for i in range(num_threads):
            transaction_workers[i].run()

        # wait for workers to finish
        for i in range(num_threads):
            transaction_workers[i].join()


        # Check inserted records using select query in the main thread outside workers
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
        number_of_transactions = 100
        number_of_operations_per_record = 1
        num_threads = 8

        keys = []
        records = {}
        seed(3562901)

        # re-generate records for testing
        for i in range(0, number_of_records):
            key = 92106429 + i
            keys.append(key)
            records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]

        transaction_workers = []
        transactions = []

        for i in range(number_of_transactions):
            transactions.append(Transaction())

        for i in range(num_threads):
            transaction_workers.append(TransactionWorker())




        updated_records = {}
        # x update on every column
        for j in range(number_of_operations_per_record):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                updated_records[key] = records[key].copy()
                for i in range(2, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    updated_records[key][i] = value
                transactions[key % number_of_transactions].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
                transactions[key % number_of_transactions].add_query(query.update, grades_table, key, *updated_columns)
        print("Update finished")


        # add trasactions to transaction workers  
        for i in range(number_of_transactions):
            transaction_workers[i % num_threads].add_transaction(transactions[i])



        # run transaction workers
        for i in range(num_threads):
            transaction_workers[i].run()

        # wait for workers to finish
        for i in range(num_threads):
            transaction_workers[i].join()


        v1_score = len(keys)
        for key in keys:
            correct = records[key]
            query = Query(grades_table)
            
            result = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0].columns
            if correct != result:
                #raise Exception('select error on primary key', key, ':', result, ', correct:', correct)
                v1_score -= 1
        print('Version -1 Score:', v1_score, '/', len(keys))

        v2_score = len(keys)
        for key in keys:
            correct = records[key]
            query = Query(grades_table)
            
            result = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0].columns
            if correct != result:
                #raise Exception('select error on primary key', key, ':', result, ', correct:', correct)
                v2_score -= 1
        print('Version -2 Score:', v2_score, '/', len(keys))
        if v1_score != v2_score:
            raise Exception('Failure: Version -1 and Version -2 scores must be same')
        
        v0_score = len(keys)
        for key in keys:
            correct = updated_records[key]
            query = Query(grades_table)
            
            result = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0].columns
            if correct != result:
                raise Exception('select error on primary key', key, ':', result, ', correct:', correct)
                v0_score -= 1
        print('Version 0 Score:', v0_score, '/', len(keys))
        score = score + 15
        
        number_of_aggregates = 100
        valid_sums = 0
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
            if column_sum == result:
                valid_sums += 1
        print("Aggregate version -1 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)

        v2_valid_sums = 0
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
            if column_sum == result:
                v2_valid_sums += 1
        print("Aggregate version -2 finished. Valid Aggregations: ", v2_valid_sums, '/', number_of_aggregates)
        if valid_sums != v2_valid_sums:
            raise Exception('Failure: Version -1 and Version -2 aggregation scores must be same.')
        
        valid_sums = 0
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
            result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
            if column_sum == result:
                valid_sums += 1
        print("Aggregate version 0 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)
        score = score + 15
        db.close()


def extended_tester():
    print("Checking exam M3 extended tester");
    global score
    global normal_p1, normal_p2, extended_p1, extended_p2
    if extended_p1:
        db = Database()
        db.open('./ECS165')

        # creating grades table
        grades_table = db.create_table('Grades', 5, 0)

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        number_of_records = 1000
        number_of_transactions = 100
        num_threads = 8

        # create index on the non primary columns
        try:
            grades_table.index.create_index(2)
            grades_table.index.create_index(3)
            grades_table.index.create_index(4)
        except Exception as e:
            print('Index API not implemented properly, tests may fail.')

        keys = []
        records = {}
        seed(3562901)

        # array of insert transactions
        insert_transactions = []

        for i in range(number_of_transactions):
            insert_transactions.append(Transaction())

        for i in range(0, number_of_records):
            key = 92106429 + i
            keys.append(key)
            records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
            t = insert_transactions[i % number_of_transactions]
            t.add_query(query.insert, grades_table, *records[key])

        transaction_workers = []
        for i in range(num_threads):
            transaction_workers.append(TransactionWorker())
            
        for i in range(number_of_transactions):
            transaction_workers[i % num_threads].add_transaction(insert_transactions[i])



        # run transaction workers
        for i in range(num_threads):
            transaction_workers[i].run()

        # wait for workers to finish
        for i in range(num_threads):
            transaction_workers[i].join()


        # Check inserted records using select query in the main thread outside workers
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
        number_of_transactions = 100
        number_of_updates = 4
        number_of_aggregates = 5

        keys = []
        records = {}
        seed(3562901)

        # re-generate records for testing
        for i in range(0, number_of_records):
            key = 92106429 + i
            keys.append(key)
            records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
            
        all_updates = []    
        for i in range(number_of_updates):
            all_updates.append({})
            transactionWorker = TransactionWorker()
            for key in keys:
                transaction = Transaction()
                updated_columns = [None, None, None, None, None]
                # copy record to check
                all_updates[i][key] = records[key].copy()
                for j in range(2, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[j] = value
                    # update our test directory
                    all_updates[i][key][j] = value
                transaction.add_query(query.update, grades_table, key, *updated_columns)
                transactionWorker.add_transaction(transaction)
            transactionWorker.run()
            transactionWorker.join()
        print("Update finished")


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
import sys   
    
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
    print("Normalized total Score for M3:", score)
    print("--------------------------------------\n")

run_test();        