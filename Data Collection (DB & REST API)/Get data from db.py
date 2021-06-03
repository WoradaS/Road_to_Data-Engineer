import pymysql.cursors
import pandas as pd

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
