import random
import sys
import string
import json
from datetime import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def randomstring(self, n):
        s = string.letters + string.digits
        return ''.join(random.sample(s,n))
    
    def purchase_info(self, probablity, n):
        purchase = {}
        is_local = 'NO'
        local_store_name = 'NA'
        local_address = 'NA'
        local_zipcode = 'NA'
        online_store_name = 'NA'
        billing_zipcode = 'NA'
        category = 'NA'
        purchase_type = self.probablity(random.randint(0, 100))
        #local transaction
        if purchase_type is 'YES':
            is_local = 'YES'
            local_store_name = 'local_store_name_' + str(random.randint(1, 100))
            local_address = 'local_address_' + str(random.randint(1, 100))
            local_zipcode = str(n + random.randint(-11000, 11000))
            online_store_name = 'NA'
            billing_zipcode = 'NA'
            category = 'local_category_' + str(random.randint(1, 100))
        else:
            list = [0,0,0,0,0,0,1]
            is_local = 'NO'
            locl_store_name= 'NA'
            local_address = 'NA'
            local_zipcode = 'NA'
            online_store_name = 'online_store_name_' + str(random.randint(1, 100))
            billing_zipcode = str(n + (random.choice(list)))
            category = 'online_category_' + str(random.randint(1, 100))
        purchase['is_local'] = is_local
        purchase['local_store_name'] = local_store_name
        purchase['local_address'] = local_address
        purchase['local_zipcode'] = local_zipcode
        purchase['online_store_name'] = online_store_name
        purchase['billing_zipcode'] = billing_zipcode
        purchase['category'] = category
        return purchase
    def probablity(self, n):
        i = random.randint(0,100)
        if i < 100 - n:
            return 'NO'
        else:
            return 'YES'
    def produce_msgs(self, source_symbol):
        msg_cnt = 0
        while True:
            n = 1000000 + random.randint(0, 1000000)
            transaction_id = str(100000000 + msg_cnt)
            user_id = str(n)
            time_stamp = datetime.now().strftime("%Y%m%d %H%M%S")
            credit_card_number = 'some_credit_card_number'
            amount = (random.randint(1, 400000)) / 100.0
            purchase = self.purchase_info(self.probablity, n)
            is_local = purchase['is_local']
            local_store_name = purchase['local_store_name']
            local_address = purchase['local_address']
            local_zipcode = purchase['local_zipcode']
            online_store_name = purchase['online_store_name']
            billing_zipcode = purchase['billing_zipcode']
            category = purchase['category']
            description = self.randomstring(5)
            alerted = 'NO'
            msgobj = {'transaction_id': transaction_id,
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
                      'description': description,
                      'alerted': alerted
                      }
            self.producer.send_messages('transaction_producer', source_symbol, json.dumps(msgobj))
            msg_cnt += 1

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 