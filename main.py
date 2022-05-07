from array import ArrayType
from tokenize import String
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import json
import xmltodict
import re
import ast
# com.databricks:spark-xml_2.11:0.12.0
# spark-submit --master local[*] --py-files="Users/clarisseguerin-williams/Documents/ofac_data/optimize-spark.py" tesT.py




def clean_colnames(df): 
    re_re = lambda s: re.sub(r'[^0-9a-zA-Z_]', "", s).lower()
    new_colnames = list(map(re_re, df.columns))
    return df.toDF(*new_colnames)


def get_uk_treasury(file_path="uk_treasury.csv"):
    
    with open(file_path, 'r') as file_in: data = file_in.read().splitlines(True)
    file_path = f"{file_path[:-4]}2.csv"
    with open(file_path, 'w') as file_out: file_out.writelines(data[1:])

    df = spark.read.option("header", True).csv(file_path)
    df = df.withColumn("source", F.lit(SRC_UK).cast(T.StringType())) 
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
    df = df.withColumn("source", F.lit(SRC_OFAC).cast(T.StringType()))

    return clean_colnames(df)
    

def print_counts(df, col):
    cntd = str(df.select(col).distinct().count())
    cnt = str(df.select(col).count())
    print(f"-------------\nDistinct Cnt = {cntd}\nCount={cnt}")


def show_sample_col(df, col, n=10, f=.01):
    df.select(col) \
        .filter(f"{col} IS NOT NULL") \
        .sample(withReplacement=True, fraction=f) \
        .show(n, False)


def show_distinct_vals(df, col):
    df.select(col) \
        .distinct() \
        .collect()


def make_simple_cols(df):
    source = df.first()["source"]

    if source == SRC_OFAC: 

        df = df \
        .withColumn("id", F.col("uid").cast(T.StringType())) \
        .withColumn("id_type", F.col("sdntype").cast(T.StringType())) \
        .withColumn("firstname", F.col("firstname").cast(T.StringType())) \
        .withColumn("lastname", F.col("lastname").cast(T.StringType())) \
        .withColumn("middlename_list", F.array()) \
        .withColumn("title", F.col("title").cast(T.StringType()))\
        .withColumn("position", F.lit(None).cast(T.StringType()))


    elif source == SRC_UK:
        middlenames = ["name2", "name3", "name4", "name5"]

        df = df \
        .withColumn("id", F.col("groupid").cast(T.StringType())) \
        .withColumn("id_type", F.col("grouptype").cast(T.StringType())) \
        .withColumn("firstname", F.col("name1").cast(T.StringType())) \
        .withColumn("lastname", F.col("name6").cast(T.StringType())) \
        .withColumn("middlename_stage", F.array(*middlenames).cast(T.ArrayType(T.StringType()))) \
            .withColumn("middlename_list", F.expr("FILTER(middlename_stage, x -> x is not null)")) \
        .withColumn("title", F.col("title").cast(T.StringType())) \
        .withColumn("position", F.col("position").cast(T.StringType()))

    else: df = None

    return df
    

def make_dob(df):
    # ofac "dateOfBirth" "dd mmm yyyy" 

    source = df.first()["source"]

    if source == SRC_OFAC:
        months = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06', 
                    'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}

        def ofac_dob(s):
            s = ast.literal_eval(s) if s is not None else None
            if isinstance(s, list):
                for d in s:
                    if d["mainEntry"] == "true":
                        return d["dateOfBirth"].strip()
            elif isinstance(s, dict): 
                return s["dateOfBirth"].strip()
            else: return None

        udf_dob = F.udf(lambda s: ofac_dob(s), T.StringType())
        udf_dob_month = F.udf(lambda s: months.get(s), T.StringType())

        
        df = df \
        .withColumn("fulldobstr", F.col("dateofbirthlist")["dateOfBirthItem"]) \
        .withColumn("dobstr", udf_dob(F.col("fulldobstr"))) \
        .withColumn("day", F.when(F.length(F.col("dobstr")) > 4, 
            F.regexp_extract(F.col("dobstr"), r"(\d{2})", 1))) \
        .withColumn("month_long", F.upper(F.regexp_extract(F.col("dobstr"), r'([a-zA-Z]{3})', 1))) \
        .withColumn("month", udf_dob_month(F.col("month_long"))) \
        .withColumn("year", F.regexp_extract(F.col("dobstr"), r'(\d{4})', 1)) \
        .withColumn("dob_map", F.create_map(
            F.lit("day"), F.col("day"),
            F.lit("month"), F.col("month"), 
            F.lit("year"), F.col("year"),
            ))


    elif source == SRC_UK:
        dlm = "/"
        df = df \
        .withColumn("doblist", F.split(F.col("dob"),dlm).cast(T.ArrayType(T.StringType()))) \
        .withColumn("day", F.col("doblist").getItem(0)) \
        .withColumn("month", F.col("doblist").getItem(1)) \
        .withColumn("year", F.col("doblist").getItem(2))
        
        df.createOrReplaceTempView("uk_dob")
        df = spark.sql("""
            SELECT *
                , CASE CAST(day AS INT) WHEN 0 THEN NULL ELSE day END AS day_clean
                , CASE CAST(month AS INT) WHEN 0 THEN NULL ELSE month END AS month_clean
                , CASE CAST(year AS INT) WHEN 0 THEN NULL ELSE year END AS year_clean
            FROM uk_dob ;
            """)
        
        df = df.withColumn("dob_map", F.create_map(
            F.lit("day"), F.col("day_clean"),
            F.lit("month"), F.col("month_clean"), 
            F.lit("year"), F.col("year_clean"),
            ))

    else: df = None

    return df


def show_sample_rows(df, cols, n=10, f=.01):
    df.select(*cols) \
        .sample(withReplacement=True, fraction=f) \
        .show(n, False)
 

def main():

    global spark, SRC_OFAC, SRC_UK
    SRC_OFAC, SRC_UK = "US OFAC", "UK TREASURY"
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Spark Assessment") \
        .getOrCreate()
    
    ofac = get_ofac()
    uk = get_uk_treasury()

    # ofac.printSchema()
    # uk.printSchema()
    # quit()

    ofac = make_simple_cols(ofac)
    uk = make_simple_cols(uk)

    # show_sample_col(ofac, "dateOfBirthList")
    # show_sample_col(uk, "dob")
    # quit()
    
    cols = ["source", "id", "id_type", "firstname", "lastname", "middlename_list", "title", "position"
            , "dob_map"
            ]
    
    ofac = make_dob(ofac)
    uk = make_dob(uk)

    show_sample_rows(ofac, cols)
    show_sample_rows(uk, cols)
    

if __name__ == "__main__":
    main()


