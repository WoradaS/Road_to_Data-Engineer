# Road_to_Data-Engineer
## - Data Collection with Python (DB & REST API)

- Get data from MySQL database
   * Config DB 
   * Connect to DB
   * Query Table
   * Convert to Pandas
- Get data from REST API
   * Requests library
   * JSON loading
   * Convert to Pandas
- Join the data
   * Save to CSV

## - Data Cleansing with Spark
###  ลง Pyspark และเชื่อมต่อ Google Colab กับ Google Drive
* เชื่อมต่อ Google Drive
### Load data
* Data Profiling
* Median of Quantity
* Summary
### EDA - Exploratory Data Analysis
* Non-Graphical EDA
* Graphical EDA
  * Boxplot
  * Histogram
  * Scatterplot
  * Plotly - interactive chart
  * Type Conversion
### Data Cleansing with Spark
* Anomalies Check
  * Syntactical Anomalies ( error sytex )
    * Lexical errors เช่น พิมพ์ผิด
  * Semantic Anomalies
    * Integrity constraints: ค่าอยู่นอกเหนือขอบเขตของค่าที่รับได้ เช่น Stockcode: ค่าจะต้องเป็นตัวเลข 5 ตัว
    * Regular exprassion
    * Missing values การเช็คและแก้ไข Missing Values (หากจำเป็น)
### Clean Data with Spark SQL
### Save cleaned data เป็น CSV


## - Data Pipeline with Airflow

* simple

![Screenshot (23)](https://user-images.githubusercontent.com/83392682/119871823-0c056800-bf4d-11eb-9fa1-ce9eb759f50f.png)

* full

![Screenshot (22)](https://user-images.githubusercontent.com/83392682/119871871-14f63980-bf4d-11eb-8597-758be42f5ff3.png)

* load data to DW

![Screenshot (24)](https://user-images.githubusercontent.com/83392682/119973254-6fd77180-bfdd-11eb-8026-aad8969c3ca9.png)
