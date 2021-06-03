# ลง Spark ใน Google Colab
!apt-get update
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
!tar xzvf spark-2.4.5-bin-hadoop2.7.tgz
!pip install -q findspark==1.3.0

# Set enviroment variable ให้รู้จัก Spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.5-bin-hadoop2.7"

# ลง pyspark ผ่านคำสั่ง pip
!pip install pyspark==2.4.5

# สร้าง Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Get Python version
import sys
sys.version_info
spark.version

# เชื่อมต่อ Google colab กับ Google Drive
from google.colab import drive
drive.mount('/content/drive')

# Load data
dt = spark.read.csv('/content/drive/MyDrive/Data Engineer/WS2DataFile/Online Retail WS2 (1).csv', header = True, inferSchema = True, )

dt.show()
# Show Schema
dt.dtypes
# Show Schema (อีกแบบ)
dt.printSchema()

# นับจำนวนแถวและ column
print((dt.count(), len(dt.columns)))

# สรุปข้อมูลสถิติ
dt.describe().show()

# สรุปข้อมูลสถิติ
# มีข้อมูล Q1-4
dt.summary().show()

# สรุปข้อมูลสถิติเฉพาะ column ที่ระบุ
dt.select("Quantity", "UnitPrice").describe().show()

# Median = 50% Q
dt.select("Quantity").summary().show()

# Non-Graphical EDA

# Select text-based information
dt.where(dt['Quantity'] > 0).show()

# ลองเลือก Quantity ระหว่าง 50 - 120
#dt.where( dt['Quantity'].between(50, 120) ).show()
dt.where( (dt['Quantity'] > 50 ) & (dt['Quantity'] < 120) ).show()

# ลองเลือก UnitPrice ระหว่าง 0.1 - 0.5
dt.where( dt['UnitPrice'].between(0.1, 0.5) ).show()

# Quantity ระหว่าง 50 - 120 และ UnitPrice ระหว่าง 0.1 - 0.5
dt.where( (dt['Quantity'].between(50,120)) & (dt['unitprice'].between(0.1, 0.5))  ).show()

# Graphical EDA

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# แปลง Spark Dataframe เป็น Pandas Dataframe
dt_pd = dt.toPandas()

# เลือกข้อมูล 500 แถวแรกเพื่อความรวดเร็วและความเรียบง่ายในการ visualize ข้อมูล
dt_pd_subset = dt_pd[0:500]

# Boxplot
sns.boxplot(dt_pd_subset['UnitPrice'])

# Histogram
sns.distplot(dt_pd_subset['UnitPrice']) 
plt.show()

# Scatterplot
dt_pd_subset.plot.scatter('UnitPrice', 'Quantity')

# Plotly - interactive chart
import plotly.express as px
fig = px.scatter(dt_pd_subset, 'UnitPrice', 'Quantity')
fig.show()

# Show Schema
dt.printSchema()

# Show unique Invoice Date
dt.select("InvoiceDate").distinct().show()

# แปลง string เป็น date
from pyspark.sql import functions as f

# dt_temp = dt.withColumn('InvoiceDateTime', functions.to_date(
#     functions.unix_timestamp('InvoiceDate', 'dd/MM/yyyy HH:mm').cast('timestamp')
# ))
            # สร้าง col
dt_temp = dt.withColumn('InvoiceDateTime', 
    f.unix_timestamp('InvoiceDate', 'dd/MM/yyyy HH:mm').cast('timestamp')
)
dt_temp.show()

dt_temp.printSchema()

dt_final = dt_temp.drop('InvoiceDate')
dt_final.show()

dt_final.printSchema()


## Data Cleansing with Spark

# Syntactical Anomalies ( error sytex )
# Lexical errors เช่น พิมพ์ผิด

# Check country distinct values. Find something interesting?
# ลองมาดูชื่อประเทศกัน เจออะไรบ้าง ?
dt_final.select("Country").distinct().show()

dt_final.where(dt_final['Country'] == 'EIREs').show()

# เปลี่ยน EIREs เป็น EIRE
from pyspark.sql.functions import when

dt_temp_eire = dt_final.withColumn("CountryUpdate", when(dt_final['Country'] == 'EIREs', 'EIRE').otherwise(dt_final['Country']))

# Check the result
dt_temp_eire.select("CountryUpdate").distinct().show()

# Create final Dataframe
dt_final_eire = dt_temp_eire.drop("Country").withColumnRenamed('CountryUpdate', 'Country')

# Semantic Anomalies
# Integrity constraints: ค่าอยู่นอกเหนือขอบเขตของค่าที่รับได้ เช่น Stockcode: ค่าจะต้องเป็นตัวเลข 5 ตัว

dt_final_eire.select("Stockcode").show(50)

dt_final_eire.count()

# filter เลือกข้อมูลตามเงื่อนไข | rlike = like | ^[0-9] เลข 0-9  {5} จำนวน 5 ตัว  $ จบห้ามมีอะไรอยู่ข้างหลัง
dt_final_eire.filter(dt_final_eire["Stockcode"].rlike("^[0-9]{5}$")).count()

# ลองดูข้อมูลที่ถูกต้อง
dt_final_eire.filter(dt_final_eire["Stockcode"].rlike("^[0-9]{5}$")).show(5)

# ลองดูข้อมูลที่ไม่ถูกต้อง
dt_correct_stockcode = dt_final_eire.filter(dt_final_eire["Stockcode"].rlike("^[0-9]{5}$"))
dt_incorrect_stockcode = dt_final_eire.subtract(dt_correct_stockcode)
# .subtract = ข้อมูล dt_final_eire [ทั้งหมด] - ข้อมูล dt_correct_stockcode [ถูกต้อง]
dt_incorrect_stockcode.show(10)

# ลบตัวอักษรตัวสุดท้ายออกจาก stock code
from pyspark.sql.functions import regexp_replace
# regexp_replace = serch และ ลบทิ้งตาม pattern ที่เลือกมา
dt_temp_stockcode = dt_final_eire.withColumn("StockcodeUpdate", regexp_replace(dt_final_eire['Stockcode'], r'[A-Z]', ''))
                                                                               # เสิร์จ r'[A-Z]' และ replace ''
                                                                               # Check the result
dt_temp_stockcode.show()

# Create final Dataframe
dt_final_stockcode = dt_temp_stockcode.drop("Stockcode").withColumnRenamed('StockcodeUpdate', 'StockCode')

# Missing values
# Check จำนวน missing values ในแต่ละ column
from pyspark.sql.functions import col,sum

dt_final_stockcode.select(*[sum(col(c).isNull().cast("int")).alias(c) for c in dt_final_stockcode.columns]).show()
                        # อ่านขวา - ซ้าย  | for c in dt_final_stockcode.columns = ในแต่ละคอลัม ให้ตั้งชื่อว่า C >> loop ที่ละตัว
                        # sum(col(c).isNull().cast("int") | หาผลรวม 
                        # col(c) = เลือก col c | isNull() = นับว่า null มีกี่ค่า | cast("int") = แปลงเป็น int | .alias(c) = ตั้งชื่อ col ตาม c
                        
# Check ว่ามีแถวไหนที่ description เป็น null บ้าง
dt_final_stockcode.where( dt_final_stockcode['Description'].isNull() ).show()

# Check ว่ามีแถวไหนที่ customerID เป็น null บ้าง
dt_final_stockcode.where( dt_final_stockcode['customerID'].isNull() ).show()

# แทน Customer ID ที่เป็น NULL ด้วย -1
# Write code here
dt_notnull = dt_final_stockcode.withColumn("customerID_notnull", when(dt_final_stockcode['customerID'].isNull() , '-1').otherwise(dt_final_stockcode['customerID']))
dt_notnull.show()

# Clean ข้อมูลด้วย Spark SQL
dt_final_stockcode.createOrReplaceTempView("sales")  # สร้าง tempview ก่อน
dt_sql = spark.sql("SELECT * FROM sales")
dt_sql.show()

dt_sql_count = spark.sql("SELECT count(*) as cnt_row FROM sales")
dt_sql_count.show(5)

dt_sql_count = spark.sql("SELECT count(*) as cnt_row, country FROM sales GROUP BY Country ORDER BY cnt_row DESC")
dt_sql_count.show(5)

dt_sql_valid_price = spark.sql("SELECT count(*) as cnt_row FROM sales WHERE UnitPrice > 0 AND Quantity > 0")
dt_sql_valid_price.show(5)

dt_sql_valid_price = spark.sql("SELECT * FROM sales WHERE UnitPrice > 0 AND Quantity > 0")
dt_sql_valid_price.show()

# Country USA ที่มี InvoiceDateTime ตั้งแต่วันที่ 2010-12-01 เป็นต้นไป และ UnitPrice เกิน 3.5
dt_sql_usa = spark.sql("""
SELECT * FROM sales
  WHERE InvoiceDateTime >= '2010-12-01'
  AND UnitPrice > 3.5
  AND Country='USA'
""").show()

# Country France ที่มี InvoiceDateTime ตังแต่วันที่ 2010-12-05 เป็นต้นไป และ UnitPrice เกิน 5.5 และ Description มีคำว่า Box
dt_sql_france = spark.sql("""
SELECT * FROM sales
  WHERE UnitPrice > 5.5
  AND InvoiceDateTime >= '2010-12-05'
  AND Country = 'France'
  AND LOWER(Description) LIKE '%box%'
""").show()

# Save cleaned data เป็น CSV
# Write as partitioned files (use multiple workers)
dt_sql_valid_price.write.csv('Cleaned_Data_Now_Final.csv')

#เราสามารถบังคับให้ Spark ใช้เครื่องเดียวได้

# Write as 1 file (use single worker)
dt_sql_valid_price.coalesce(1).write.csv('Cleaned_Data_Now_Final_Single.csv')
                  # coalesce(1) = เพื่อรวมเป็นเครื่องเดียว ไฟล์เดียว
                  
# อ่านไฟล์ที่มีหลาย Part

# แก้โค้ดด้านล่างเป็นชื่อไฟล์ที่ Spark สร้างขึ้นมาใน Google Drive ของนะครับ เพราะชื่อไฟล์จะสุ่มสร้างออกมา ถ้ารันโดยไม่แก้เลยจะ Error
# อ่าน CSV ไฟล์ที่ 1
part1 = spark.read.csv('/content/Cleaned_Data_Now_Final.csv/part-00000-770f2dd1-6863-4b86-9818-3f856138d751-c000.csv', header = True, inferSchema = True, )
part1.count()

# แก้โค้ดด้านล่างเป็นชื่อไฟล์ที่ Spark สร้างขึ้นมาใน Google Drive ของนะครับ เพราะชื่อไฟล์จะสุ่มสร้างออกมา ถ้ารันโดยไม่แก้เลยจะ Error
# อ่าน CSV ไฟล์ที่ 2
part2 = spark.read.csv('/content/Cleaned_Data_Now_Final.csv/part-00001-770f2dd1-6863-4b86-9818-3f856138d751-c000.csv', header = True, inferSchema = True, )
part2.count()

# วิธีอ่าน CSV ทุกไฟล์ในโฟลเดอร์นี้
# Write Code Here
part1_2 = spark.read.csv('/content/Cleaned_Data_Now_Final.csv/part-*.csv', header = True, inferSchema = True, )
part1_2 .count()






