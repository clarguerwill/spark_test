import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

# install pandas and lxml



spark = SparkSession.builder \
      .master("local[*]") \
      .appName("Test") \
      .getOrCreate()
    


file_path = "uk_treasury.csv"
data = spark.sparkContext.textFile(file_path)
headers = data.collect()[1].split(",")
data = data.zipWithIndex().filter(lambda row_index: row_index[1] > 1).keys()
data = data.map(lambda row : row.split(","))

cols = []
for col in headers:
    cols.append(StructField(col, StringType(),True))

schema = StructType(cols)

df = spark.createDataFrame(data, schema)
df.printSchema()
df.show(2)



"""

file_name = "uk_treasury"
df_pd = pd.read_csv(f"{file_name}.csv", skiprows=1, header=0).to_csv(f"{file_name}2.csv")
df = spark.read.csv(f"{file_name}2.csv", header=True)
df.show(2)

"""