from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
#com.databricks:spark-xml_2.11:0.12.0
# spark-submit --master local[*] --py-files="Users/clarisseguerin-williams/Documents/ofac_data/optimize-spark.py" test.py

    
def get_uk_treasury(file_path="uk_treasury.csv"):
    data = spark.sparkContext.textFile(file_path)
    headers = data.collect()[1].split(",")
    data = data.zipWithIndex().filter(lambda row_index: row_index[1] > 1).keys()
    data = data.map(lambda row : row.split(","))

    cols = []
    for col in headers:
        cols.append(StructField(col, StringType(),True))

    schema = StructType(cols)

    df = spark.createDataFrame(data, schema)
    return df 



def get_ofac(file_path="uk_treasury.csv"):
    pass

"""
sdnEntry
    uid
    lastName
    sdnType
    programList
        program
    akaList
        aka
            uid
            type
            category
            lastName
    addressList
        address
            uid
            city
            country


"""


def main():
    global spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Test") \
        .getOrCreate() 

    # uk = get_uk_treasury()




if __name__ == "__main__":
    main()
