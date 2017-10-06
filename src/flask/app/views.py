from app import app
import MySQLdb
from flask import jsonify
from flask import render_template, request
import copy
import time
import datetime
from  collections import Counter
import json

@app.route('/')

@app.route('/index')
def index():
 return render_template("index.html")

@app.route('/about_me')
def aboutme():
  return render_template("about_me.html")

@app.route('/technical_pipeline')
def technical_pipeline():
  return render_template("technical_pipeline.html")

@app.route('/detection_algorithm')
def detection_algorithm():
  return render_template("detection_algorithm.html")

@app.route('/demonstration')
def demonstration():
  connection = sqlcon.connect(host = h,user = u, passwd = pwd, db = db)
  cursor = connection.cursor()
  sql= "SELECT * FROM transactions_accumulating tx, customers_filtered cx WHERE tx.user_id = cx.user_id AND tx.alerted != 'NO' order by tx.transaction_id desc limit 10"
  response = cursor.execute(sql)
  response = cursor.fetchall()
  list = []
  for element in response:
        list.append(element)
  jsonresponse = [{"time": x[2], "c_id": x[1], "c_zip": x[15], "c_balance":x[17], 
			 "c_limit":  x[16], "c_tvl": x[18], "t_id": x[0], "t_amount": x[4],
       "lc_t_zip": x[8], "ol_bill_zip": x[10], "msg": x[13]} for x in list]
  return render_template("demonstration.html", output = jsonresponse)



