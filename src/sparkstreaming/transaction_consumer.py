from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
import mysql.connector as sqlcon
import json

# JSON->dictionary
def extractor(json_string):
    json_obj = json.loads(json_string)
    try:
        transaction_id = json_obj['transaction_id']
        user_id = json_obj['user_id']
        time_stamp = json_obj['time_stamp']
        credit_card_number = json_obj['credit_card_number']
        amount = float(json_obj['amount'])
        is_local = json_obj['is_local']
        local_store_name = json_obj['local_store_name']
        local_address = json_obj['local_address']
        local_zipcode = json_obj['local_zipcode']
        online_store_name = json_obj['online_store_name']
        billing_zipcode = json_obj['billing_zipcode']
        category = json_obj['category']
        description = json_obj['description']
        alerted = json_obj['alerted']
    except:
        return None
    data = {'transaction_id': transaction_id,
            'user_id': user_id,
            'time_stamp': time_stamp,
            'credit_card_number': credit_card_number,
            'amount': amount,
            'is_local': is_local,
            'local_store_name': local_store_name,
            'local_address': local_address,
            'local_zipcode': local_zipcode,
            'online_store_name': online_store_name,
            'billing_zipcode': billing_zipcode,
            'category': category,
            'description':description,
            'alerted': alerted}
    return data

def filter_nones(transaction_data):
    if transaction_data is not None:
        return True
    return False

def update_alert(transaction, alert_msg):
    transaction['alerted'] = alert_msg
    transaction_to_insert = (transaction['transaction_id'], transaction['user_id'],transaction['time_stamp'],
        transaction['credit_card_number'], transaction['amount'], transaction['is_local'],
        transaction['local_store_name'], transaction['local_address'], transaction['local_zipcode'], transaction['online_store_name'],
        transaction['billing_zipcode'], transaction['category'], transaction['description'], transaction['alerted'])
    return transaction_to_insert 

def check_and_insert(partition):
    connection = sqlcon.connect(host = h,user = u, passwd = pwd, db = db)
    cursor = connection.cursor()

    limit_error_msg = 'limit reached'
    local_zipcode_error_msg = 'remote local'
    billing_error_msg = 'wrong billing'
    
    customers = []
    transactions_by_user_id = {}
    transactions_by_transaction_id = {}
    user_ids = []
    for info in partition:
        transaction_id = info['transaction_id']
        user_id = info['user_id']
        user_ids.append(user_id)
        
        if user_id in transactions_by_user_id:
            transactions_by_user_id[user_id].append(info)
        else:
            transactions_by_user_id[user_id] = []
            transactions_by_user_id[user_id].append(info)
        transactions_by_transaction_id[transaction_id] = info
    query = 'SELECT * FROM customers_1000_filtered WHERE user_id in (' + ','.join(map(str, user_ids)) + ')'
    try:
        cursor.execute(query)
        customers = cursor.fetchall()      
    except:
        print('selection exception')

    transactions_to_insert = []
    customers_to_update = []
    customers_to_update_user_ids = []

    for customer in customers:
        cx_user_id = customer[0]
        cx_zipcode = customer[1]
        cx_credit_card_limit = customer[2]
        cx_current_balance = customer[3]
        cx_is_traveling = customer[4]
        cx_total_purchase = customer[5]
        transactions = transactions_by_user_id[cx_user_id]
        for transaction in transactions:
            tnx_amount = transaction['amount']
            tnx_is_local = transaction['is_local']
            tnx_local_zipcode = transaction['local_zipcode']
            tnx_billing_zipcode = transaction['billing_zipcode']

            if tnx_is_local == 'YES': # local purchase
                if abs(int(cx_zipcode) - int(tnx_local_zipcode)) > 10000 and cx_is_traveling is 'NO':
                    transaction_to_insert = update_alert(transaction, local_zipcode_error_msg)
                    transactions_to_insert.append(transaction_to_insert)
                    continue
            else: # online purchase
                if tnx_billing_zipcode != cx_zipcode:
                    transaction_to_insert = update_alert(transaction, billing_error_msg)
                    transactions_to_insert.append(transaction_to_insert)
                    continue           
            if cx_current_balance + tnx_amount > cx_credit_card_limit:
                transaction_to_insert = update_alert(transaction, limit_error_msg)
                transactions_to_insert.append(transaction_to_insert)
                continue
            cx_current_balance += tnx_amount
            cx_total_purchase = cx_current_balance
            transaction_to_insert = update_alert(transaction, 'NO')
            transactions_to_insert.append(transaction_to_insert)
        customers_to_update.append((cx_current_balance, cx_total_purchase, cx_user_id))
            
    
    insert = 'INSERT IGNORE INTO transactions_accumulating VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    update = 'UPDATE customers_1000_filtered SET current_balance = %s, total_purchase = %s WHERE user_id = %s'

    try:
        print(transactions_to_insert)
        cursor.executemany(insert, transactions_to_insert)
        connection.commit()
    except:
        connection.rollback()
    
    try:
        cursor.executemany(update, customers_to_update)
        connection.commit()
    except:
        connection.rollback()
    connection.close()
    return transactions_to_insert

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: sparkstreaming_transaction.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="PythonStreamingDirectKafkaTransaction")
    ssc = StreamingContext(sc, 1)

    brokers, topic = sys.argv[1:]
    kvmsgs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    msgs = kvmsgs.map(lambda x: x[1]).map(lambda x: extractor(x)).filter(lambda x: filter_nones(x))
    inserted_transactions = msgs.mapPartitions(check_and_insert)
    inserted_transactions.count().pprint()
    
    ssc.start()
    ssc.awaitTermination()
