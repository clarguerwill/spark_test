from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import json
import xmltodict
import re
# com.databricks:spark-xml_2.11:0.12.0
# spark-submit --master local[*] --py-files="Users/clarisseguerin-williams/Documents/ofac_data/optimize-spark.py" test.py

    
def clean_colnames(df): 
    re_re = lambda s: re.sub(r'[^0-9a-zA-Z_]', "", s).lower()
    new_colnames = list(map(re_re, df.columns))
    return df.toDF(*new_colnames)


def get_uk_treasury(file_path="uk_treasury.csv"):
    
    with open(file_path, 'r') as file_in: data = file_in.read().splitlines(True)
    file_path = f"{file_path[:-4]}2.csv"
    with open(file_path, 'w') as file_out: file_out.writelines(data[1:])

    df = spark.read.option("header", True).csv(file_path)
    
    return clean_colnames(df)


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
    return clean_colnames(df)
    

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


def get_empty_df():
    schema = t.StructType([ \
        t.StructField("source", t.StringType(), True), \
        t.StructField("id", t.StringType(), True), \
        t.StructField("id_type", t.StringType(), True), \
        t.StructField("firstname", t.StringType(), True), \
        t.StructField("lastname", t.StringType(), True), \
        t.StructField("middlenamelist", t.ArrayType(t.StringType()), True) \
        ])

    emptyRDD = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(emptyRDD, schema)

    return df


def make_simple_cols(df):

    if "uid" in df.columns:
        df = df \
            .withColumn("source", f.lit("US OFAC").cast(t.StringType())) \
            .withColumn("id", f.col("uid").cast(t.StringType())) \
            .withColumn("id_type", f.col("sdntype").cast(t.StringType())) \
            .withColumn("firstname", f.col("firstname").cast(t.StringType())) \
            .withColumn("lastname", f.col("lastname").cast(t.StringType())) \
            .withColumn("middlenamelist", f.lit(None).cast(t.ArrayType(t.StringType()))) 

    elif "groupid" in df.columns:
        df = df \
            .withColumn("source", f.lit("UK TREASURY").cast(t.StringType())) \
            .withColumn("id", f.col("groupid").cast(t.StringType())) \
            .withColumn("id_type", f.col("grouptype").cast(t.StringType())) \
            .withColumn("firstname", f.col("name1").cast(t.StringType())) \
            .withColumn("lastname", f.col("name6").cast(t.StringType())) \
            .withColumn("middlenamelist", f.lit(None).cast(t.ArrayType(t.StringType())))
    
    else: df = None

    return df
    
 
def main():

    global spark
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Test") \
        .getOrCreate() 
    
    uk = get_uk_treasury()
    ofac = get_ofac()

    uk = make_simple_cols(uk)
    ofac = make_simple_cols(ofac)
        
    




if __name__ == "__main__":
    main()


