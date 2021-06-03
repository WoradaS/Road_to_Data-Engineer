# Data Cleansing with Spark

ข้อมูลขายของออนไลน์
Data Dictionary
https://archive.ics.uci.edu/ml/datasets/Online+Retail

This is a transactional data set which contains all the transactions occurring between 01/12/2018 and 09/12/2019 for a UK-based and registered non-store online retail.

The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.

InvoiceNo: Invoice number. Nominal, a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'c', it indicates a cancellation.
StockCode: Product (item) code. Nominal, a 5-digit integral number uniquely assigned to each distinct product.
Description: Product (item) name. Nominal.
Quantity: The quantities of each product (item) per transaction. Numeric.
InvoiceDate: Invice Date and time. Numeric, the day and time when each transaction was generated.
UnitPrice: Unit price. Numeric, Product price per unit in sterling.
CustomerID: Customer number. Nominal, a 5-digit integral number uniquely assigned to each customer.
Country: Country name. Nominal, the name of the country where each customer resides.

##  ลง Pyspark และเชื่อมต่อ Google Colab กับ Google Drive
* เชื่อมต่อ Google Drive
## Load data
* Data Profiling
* Median of Quantity
* Summary
## EDA - Exploratory Data Analysis
* Non-Graphical EDA
* Graphical EDA
  * Boxplot
  * Histogram
  * Scatterplot
  * Plotly - interactive chart
  * Type Conversion
## Data Cleansing with Spark
* Anomalies Check
  * Syntactical Anomalies ( error sytex )
    * Lexical errors เช่น พิมพ์ผิด
  * Semantic Anomalies
    * Integrity constraints: ค่าอยู่นอกเหนือขอบเขตของค่าที่รับได้ เช่น Stockcode: ค่าจะต้องเป็นตัวเลข 5 ตัว
    * Regular exprassion
    * Missing values การเช็คและแก้ไข Missing Values (หากจำเป็น)
## Clean Data with Spark SQL
## Save cleaned data เป็น CSV
