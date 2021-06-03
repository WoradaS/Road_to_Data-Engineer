import pymysql.cursors
import pandas as pd
# Package requests ใช้สำหรับการยิง REST API
import requests
import json

class Config:
  MYSQL_HOST = ''
  MYSQL_PORT = 3306  # default port ของ MySQL คือ 3306
  MYSQL_USER = ''
  MYSQL_PASSWORD = ''
  MYSQL_DB = ''
  MYSQL_CHARSET = 'utf8mb4'
  
# Connect to the database
connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)


with connection.cursor() as cursor:
  # Read a single record
  sql = "select * from online_retail"
  cursor.execute(sql)
  result = cursor.fetchall()

  
retail = pd.DataFrame(result)

# Rest API
url = "https://de-training-2020-7au6fmnprq-de.a.run.app/currency_gbp/all"
response = requests.get(url)

#response.text
#response.json()   

# string JSON >> dictionary
result = response.json()
#type(result)

## OR ##

# Python Requests แปลงเป็น json แทน
result_conversion_rate = json.loads(response.text)

#เช็คประเภทข้อมูล (ถ้าไม่ใช่ dict จะ raise exception ขึ้นมา)
print(type(result_conversion_rate))
assert isinstance(result_conversion_rate, dict)

# Convert to Pandas
conversion_rate = pd.DataFrame.from_dict(result_conversion_rate)
conversion_rate[:3]

# เปลี่ยนชื่อ index เป็น date
conversion_rate = conversion_rate.reset_index().rename(columns={"index":"date"})
conversion_rate[:3]

# Join the data

# cp column InvoiceDate เก็บเอาไว้ เผื่อได้ใช้ในอนาคต >> ไม่งั้นข้อมูล timestamp จะหายไป
retail['InvoiceTimestamp'] = retail['InvoiceDate']
retail.head()

# แปลงให้ InvoiceDate ใน retail กับ date ใน conversion_rate มีเฉพาะส่วน date ก่อน
#marge แบบ pd 
retail['InvoiceDate'] = pd.to_datetime(retail['InvoiceDate']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

# รวม 2 dataframe เข้าด้วยกันด้วยคำสั่ง merge
# left join
final_df = retail.merge(conversion_rate, how="left", left_on="InvoiceDate", right_on="date")

# -- join ข้อมูลได้แล้ว --

#  เพิ่ม column 'THBPrice' ที่เกิดจาก column UnitPrice * Rate
# วิธี 1
final_df["THBPrice"] = final_df["unitPrice"] * final_df["Rate"]
final_df.head()

# วิธี 2
# apply function ของ pandas
final_df["THBPrice"] = final_df.apply(lambda x: x["unitPrice"] * x["Rate"],axis=1)
final_df.head()

# วิธี 3 
def conversion_rate(price, rate):
  return price * rate

final_df["THBPrice"] = final_df.apply(lambda x: conversion_rate(x["unitPrice"] ,x["Rate"]),axis=1)

# save "to csv" 
final_df.to_csv("output_csv",index=False)
