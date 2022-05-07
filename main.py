from array import ArrayType
from tokenize import String
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

import pandas as pd
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
    pd_df = pd.read_csv(file_path, skiprows=1, header=0)
    pd_df = pd_df.where((pd.notnull(pd_df)), None)
    
    cols = []
    for col in pd_df.columns:
        cols.append(T.StructField(col, T.StringType(),True))
    schema = T.StructType(cols)

    df = spark.createDataFrame(pd_df, schema)
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
  

def make_simple_cols(df):
    source = df.first()["source"]

    if source == SRC_OFAC:

        df = df \
        .withColumn("sourceId", F.col("uid").cast(T.StringType())) \
        .withColumn("sidType", F.col("sdntype").cast(T.StringType())) \
        .withColumn("firstName", F.col("firstname").cast(T.StringType())) \
        .withColumn("lastName", F.col("lastname").cast(T.StringType())) \
        .withColumn("middleNameList", F.array()) \
        .withColumn("titleStage", F.array("title").cast(T.ArrayType(T.StringType()))) \
            .withColumn("titleList", F.expr("FILTER(titleStage, x -> x is not null)")) \
        .withColumn("positionList",  F.array())


    elif source == SRC_UK:
        middlenames = ["name2", "name3", "name4", "name5"]

        df = df \
        .withColumn("sourceId", F.col("groupid").cast(T.StringType())) \
        .withColumn("sidType", F.col("grouptype").cast(T.StringType())) \
        .withColumn("firstName", F.col("name1").cast(T.StringType())) \
        .withColumn("lastName", F.col("name6").cast(T.StringType())) \
        .withColumn("middleNameStage", F.array(*middlenames).cast(T.ArrayType(T.StringType()))) \
            .withColumn("middleNameList", F.expr("FILTER(middleNameStage, x -> x is not null)")) \
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

        def ofac_dob(x):
            x = ast.literal_eval(x) if x is not None and isinstance(x, str) else None
            if isinstance(x, list):
                l = []
                for d in x: 
                    d = ofac_dob(d)
                    l.extend(d) if d is not None else None
                return l

            elif isinstance(x, dict):
                d = {}
                d["mainEntry"] = x["mainEntry"] 
                d["id"] = x["uid"] 
                dob = x["dateOfBirth"].upper()
                
                dy = re.search(r'(\d{2})', dob) if len(dob) > 4 else None
                dy = dy.group(0) if dy is not None else None
                d["day"] = dy if dy is not None and int(dy) != 0 else None
                y = re.search(r'(\d{4})', dob)
                y = y.group(0) if y is not None else None
                d["year"] = y if y is not None and int(y) != 0 else None
                m = re.search(r'([a-zA-Z]{3})', dob)
                d["month"] = months.get(m.group(0)) if m is not None else None

                return [d] if d is not None else None

            else: return None

        udf_dob = F.udf(lambda x: ofac_dob(x), T.ArrayType(T.MapType(T.StringType(), T.StringType())))

        df = df \
        .withColumn("dobStr", F.col("dateofbirthlist")["dateOfBirthItem"]) \
        .withColumn("dobMap", udf_dob(F.col("dobStr")))


    elif source == SRC_UK:
        dlm = "/"
        df = df \
        .withColumn("dobList", F.split(F.col("dob"),dlm).cast(T.ArrayType(T.StringType()))) \
        .withColumn("day", F.col("dobList").getItem(0)) \
        .withColumn("month", F.col("dobList").getItem(1)) \
        .withColumn("year", F.col("dobList").getItem(2)) \
        .withColumn("day", F.when(F.col("day").cast("int") != 0, F.col("day"))) \
        .withColumn("month", F.when(F.col("month").cast("int") != 0, F.col("month"))) \
        .withColumn("year", F.when(F.col("year").cast("int") != 0, F.col("year"))) \
        .withColumn("dobMap", F.create_map(
            F.lit("mainEntry"), F.lit(None),
            F.lit("id"), F.lit(None),
            F.lit("day"), F.col("day"),
            F.lit("month"), F.col("month"), 
            F.lit("year"), F.col("year")            
            ))

    else: df = None

    return df


def make_aliases(df):
    source = df.first()["source"]
    # array struct (id, aliasType, aliasQuality, lastName, firstName, middlesNameList)
        # nonLatinName, nonLatinType, nonLatinLanguage

    if source == SRC_OFAC:
        # uid, type, category, lastName, firstName
        # end as full data schema
        df = df \
        .withColumn("aliasstr", F.col("akalist")["aka"])
        

    elif source == SRC_UK:
        # AliasType, AliasQuality, lastName, firstName, middleNameList
        # Name Non-Latin Script and Non-Latin Script Type and Non-Latin Script Language

        # end as structure type
        df = df \
        .withColumn("aliasStruct", F.struct(
            F.lit(None).cast(T.StringType()).alias("id"),
            F.col("AliasType").cast(T.StringType()).alias("aliasType"),
            F.col("AliasQuality").cast(T.StringType()).alias("aliasQuality"),
            F.col("lastName").cast(T.StringType()).alias("lastName"),
            F.col("firstName").cast(T.StringType()).alias("firstName"),
            F.col("middleNameList").cast(T.ArrayType(T.StringType())).alias("middleNameList"),
            F.col("NameNonLatinScript").cast(T.StringType()).alias("nonLatinName"),
            F.col("NonLatinScriptType").cast(T.StringType()).alias("nonLatinType"),
            F.col("NonLatinScriptLanguage").cast(T.StringType()).alias("nonLatinLanguage")
        ))

    else: df = None

    return df
  

def print_counts(df, col):
    cntd = str(df.select(col).distinct().count())
    cnt = str(df.select(col).count())
    print(f"-------------\nDistinct Cnt = {cntd}\nCount={cnt}")


def show_sample_col(df, col, n=20, f=.01):
    df.select(col) \
        .filter(f"{col} IS NOT NULL") \
        .sample(withReplacement=True, fraction=f) \
        .show(n, False)


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

    # show_sample_col(ofac, "akalist")
    # show_sample_col(uk, "dob")
    # quit()
    
    
    uk_cols = ["source", "sourceId", "sidType", "firstName", "lastName", "middleNameList", "title"
            , "position" , "dobMap"
            ]
    cols = ["source", "sourceId", "aliasStruct"]
    
    uk = get_uk_treasury()
    uk = make_simple_cols(uk)

    # uk = make_dob(uk)
    uk = make_aliases(uk)
    show_sample_rows(uk, cols)
    quit()

    ofac_cols = ["source", "sourceId", "sidType", "firstName", "lastName", "middleNameList", "titleList"
            , "positionList" , "dobMap"
            ]
    cols = ["source", "sourceId"]

    ofac = get_ofac()
    ofac = make_simple_cols(ofac)
    # ofac = make_dob(ofac)
    ofac = make_aliases(ofac)
    show_sample_rows(ofac, cols)
    quit()
    

if __name__ == "__main__":
    main()

