from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType

import json
import xmltodict
import re
# com.databricks:spark-xml_2.11:0.12.0
# spark-submit --master local[*] --py-files="Users/clarisseguerin-williams/Documents/ofac_data/optimize-spark.py" test.py

    
def keep_alphanumeric(s): return re.sub(r'[^0-9a-zA-Z]', "", s)


def get_uk_treasury(file_path="uk_treasury.csv"):
    
    with open(file_path, 'r') as file_in: data = file_in.read().splitlines(True)
    file_path = f"{file_path[:-4]}2.csv"
    with open(file_path, 'w') as file_out: file_out.writelines(data[1:])

    df = spark.read.option("header", True).csv(file_path)
    new_colnames = list(map(keep_alphanumeric, df.columns))
    df = df.toDF(*new_colnames)
    return df


def get_ofac(file_path="ofac.xml"):
    
    with open(file_path) as xml_file:
        data_dict = xmltodict.parse(xml_file.read())
    xml_file.close()

    json_data = json.dumps(data_dict["sdnList"]["sdnEntry"])
    other_data = data_dict["sdnList"]["publshInformation"]

    file_path = file_path.replace(".xml", ".json")
    with open(file_path, "w") as json_file:
        json_file.write(json_data)
    json_file.close()

    df = spark.read.json(file_path, primitivesAsString='true')
    return df
    

def print_counts(df, col):
    cntd = str(df.select(col).distinct().count())
    cnt = str(df.select(col).count())
    print(f"-------------\nDistinct Cnt = {cntd}\nCount={cnt}")


def show_sample(df, col, n=10, f=.01):
    df.select(col) \
        .filter(f"{col} IS NOT NULL") \
        .sample(withReplacement=True, fraction=f) \
        .show(n, False)


def show_distinct_vals(df, col):
    df.select(col) \
        .distinct() \
        .collect()


def main():

    global spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Test") \
        .getOrCreate() 
    
    uk = get_uk_treasury()
    ofac = get_ofac()
   
    uk.printSchema()
    ofac.printSchema()
    






if __name__ == "__main__":
    main()


