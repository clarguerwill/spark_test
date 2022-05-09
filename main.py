from array import ArrayType
from tokenize import String
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as T
import pyspark.sql.functions as F

# pip install pandas and xmltodict
import pandas as pd
import xmltodict
import json
import re
import ast

# com.databricks:spark-xml_2.11:0.12.0
# spark-submit --master local[*] --py-files="Users/clarisseguerin-williams/Documents/ofac_data/optimize-spark.py" tesT.py


"""
This renames the column in the given dataframe
The function keeps only alphanumeric and underscore characters
"""
def clean_colnames(df): 
    re_re = lambda s: re.sub(r'[^0-9a-zA-Z_]', "", s).lower()
    new_colnames = list(map(re_re, df.columns))
    return df.toDF(*new_colnames)


def get_uk_treasury(file_path="uk_treasury.csv"):
    # Read csv with pandas because of the skiprows functionality
    pd_df = pd.read_csv(file_path, skiprows=1, header=0)
    # Converts pandas NaNs to None so that Spark recognizes a None value
    pd_df = pd_df.where((pd.notnull(pd_df)), None)

    # Set the schema to all string columns
    # Spark was having trouble infering the schema
    cols = []
    for col in pd_df.columns:
        cols.append(T.StructField(col, T.StringType(),True))
    schema = T.StructType(cols)

    df = spark.createDataFrame(pd_df, schema)
    # Create the source column here
    df = df.withColumn("source", F.lit(SRC_UK).cast(T.StringType()))
    return clean_colnames(df)


def get_ofac(file_path="ofac.xml"):
    
    # Having trouble parsing the xml
    # This package seemed to work
    # Could not get com.databricks:spark-xml_2.11:0.12.0 to work
    with open(file_path) as xml_file:
        data_dict = xmltodict.parse(xml_file.read())
    xml_file.close()

    # Convert xml to json
    json_data = json.dumps(data_dict["sdnList"]["sdnEntry"])
    # Keeps the xml data from the begining, just in case needed in future
    other_data = data_dict["sdnList"]["publshInformation"]

    # Read the json that was just made
    file_path = file_path.replace(".xml", ".json")
    with open(file_path, "w") as json_file:
        json_file.write(json_data)
    json_file.close()

    df = spark.read.json(file_path, primitivesAsString='true')
    # Creates the source columns
    df = df.withColumn("source", F.lit(SRC_OFAC).cast(T.StringType()))

    return clean_colnames(df)
  

def make_simple_cols(df):
    source = df.first()["source"]

    if source == SRC_OFAC:

        # Rename some columns and make sure datatypes are correct
        # TitleList: had to remove null in the array; nulls become no data rather than null
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

        """
        Rename some columns and make sure datatypes are correct
        MiddleNameStage: had to remove null in the array; nulls become no data rather than null
        Title and position will become an array when uk data gets aggregated
        """
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

    source = df.first()["source"]

    if source == SRC_OFAC:
        
        months = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06', 
                    'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}

        def ofac_dob(x):
            """
            The func literal_eval reads a string as if it were code
            Using to convert the dictionaries/lists represented as strings into actual dictionaries/lists
            Only liter_eval if the input is a string
            """
            x = ast.literal_eval(x) if x and isinstance(x, str) else None
            
            # If list then iterate through every string in the list
            # The strings in the list must then be parsed as dictionaries
            if isinstance(x, list):
                l = []
                for d in x: 
                    # recursive in order to parse the dictionary values
                    d = ofac_dob(d)
                    l.extend(d) if d else None
                return l

            # If the input is a dictionary
            # Select the desired keys and edit the DOB string
            elif isinstance(x, dict):
                d = {}
                d["mainEntry"] = x["mainEntry"] 
                d["id"] = x["uid"] 
                dob = x["dateOfBirth"].upper()
                
                # parse DOB string using the magic of regex
                dy = re.search(r'(\d{2})', dob) if len(dob) > 4 else None
                dy = dy.group(0) if dy else None
                d["day"] = dy if dy and int(dy) != 0 else None
                m = re.search(r'([a-zA-Z]{3})', dob)
                d["month"] = months.get(m.group(0)) if m else None
                y = re.search(r'(\d{4})', dob)
                y = y.group(0) if y else None
                d["year"] = y if y and int(y) != 0 else None

                return [d] if d else None

            else: return None

        udf_dob = F.udf(ofac_dob, T.ArrayType(T.MapType(T.StringType(), T.StringType())))

        df = df \
        .withColumn("dobStr", F.col("dateofbirthlist")["dateOfBirthItem"]) \
        .withColumn("dobMap", udf_dob(F.col("dobStr")))


    elif source == SRC_UK:
        dlm = "/"
        # dobMap is not yet an Array because it will become an array when the data gets aggregated
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

    if source == SRC_OFAC:

        # Again, this is handled much in the same way as DOB
        def ofac_alias(x):
            x = ast.literal_eval(x) if x and isinstance(x, str) else None
            if isinstance(x, list):
                l = []
                for d in x: 
                    d = ofac_alias(d)
                    l.extend(d) if d else None
                return l

            elif isinstance(x, dict):
                d = {}
                d["id"] = x["uid"]
                d["aliasType"] = x["type"].replace(".", "").upper()
                d["aliasQuality"] = x["category"].title()
                d["firstName"] = x["firstName"] if "firstName" in x.keys() else None
                d["lastName"] = x["lastName"] if "lastName" in x.keys() else None
                d["middleNameList"] = None
                return [d] if d else None

            else: return None

        alias_schema = T.ArrayType(T.StructType([
            T.StructField("id", T.StringType(),True), \
            T.StructField("aliasType", T.StringType(),True), \
            T.StructField("aliasQuality", T.StringType(),True), \
            T.StructField("firstName", T.StringType(), True), \
            T.StructField("lastName", T.StringType(), True), \
            T.StructField("middleNameList", T.ArrayType(T.StringType()), True)
        ]))

        udf_alias = F.udf(ofac_alias, alias_schema)

        df = df \
        .withColumn("aliasStr", F.col("akalist")["aka"]) \
        .withColumn("aliasStruct", udf_alias(F.col("aliasStr")))
        

    elif source == SRC_UK:
        # Rename the columns that will be used in the aliasStruct column
        df = df \
        .withColumn("aliasStruct", F.struct(
            F.lit(None).cast(T.StringType()).alias("id"),
            F.col("aliastype").cast(T.StringType()).alias("aliasType"),
            F.col("aliasquality").cast(T.StringType()).alias("aliasQuality"),
            F.col("firstName").cast(T.StringType()).alias("firstName"),
            F.col("lastName").cast(T.StringType()).alias("lastName"),
            F.col("middleNameList").cast(T.ArrayType(T.StringType())).alias("middleNameList")
        ))

    else: df = None

    return df


def aggregate_uk(uk):

    """ 
    Aggregate UK because every row does not equal one entity
    I wanted the data to be one row is one entity
    in general, this func group by's on the sourceId and aggragates select columns
    orderId column is made because we need to isolate the rows where "aliasType" == "Primary name"
    """
    uk = uk \
        .withColumn("aliasType", F.col("aliasStruct")["aliasType"]) \
        .withColumn("orderId", 
            F.when(F.col("aliasType") == "Primary name", 1) \
            .when(F.col("aliasType") == "Primary name variation", 2) \
            .otherwise(F.lit(0))
        ).drop("aliasType") \
        .withColumn("row_num", F.row_number().over(Window.partitionBy("sourceId").orderBy("orderId")))
    
    # df has all the "Primary name" info
    df = uk.filter("row_num == 1")
    # df2 has all the alias information, also need to remove the duplicate "Primary name" values
    df2 = uk.filter("row_num <> 1") \
        .withColumn("aliasStruct", 
            F.when(F.col("aliasStruct")["aliasType"] == "Primary name", None)
            .otherwise(F.col("aliasStruct"))
        )

    # Since collect_set only takes in strings or ints as input
    # Had to convert the Map and Struct types back to strings
    df2 = df2.groupBy("sourceId").agg(
        F.collect_set("title").alias("titleList"),
        F.collect_set("position").alias("positionList"),
        F.collect_set(F.to_json(F.col("dobMap"))).alias("dobMap"),
        F.collect_set(F.to_json(F.col("aliasStruct"))).alias("aliasStruct")
    )

    # the rest of this code converts the list of strings representing dictionaries back to their original types
    alias_schema = T.ArrayType(T.StructType([
            T.StructField("id", T.StringType(),True), \
            T.StructField("aliasType", T.StringType(),True), \
            T.StructField("aliasQuality", T.StringType(),True), \
            T.StructField("firstName", T.StringType(), True), \
            T.StructField("lastName", T.StringType(), True), \
            T.StructField("middleNameList", T.ArrayType(T.StringType()), True)
        ]))

    def convert_str_to_dict(arr):
        arr2 = []
        if arr:
            for d in arr:
                if d: arr2.append(ast.literal_eval(d.replace(":null", ":None")))
            return arr2
        else: return None

    udf_dob = F.udf(convert_str_to_dict, T.ArrayType(T.MapType(T.StringType(), T.StringType())))
    udf_alias = F.udf(convert_str_to_dict, alias_schema)

    df2 = df2 \
        .withColumn("dobMap", udf_dob(F.col("dobMap"))) \
        .withColumn("aliasStruct", udf_alias(F.col("aliasStruct")))

    df = df.select("source", "sourceId", "sidType", "firstName", "lastName", "middleNameList")
       
    df = df.join(df2, "sourceId", "left")

    return df


# used in the testing process, not present in final code
def print_counts(df, col):
    cntd = str(df.select(col).distinct().count())
    cnt = str(df.select(col).count())
    print(f"-------------\nDistinct Cnt = {cntd}\nCount={cnt}")


# used in the testing process, not present in final code
def show_sample_col(df, col, n=20, f=.01):
    df.select(col) \
        .filter(f"{col} IS NOT NULL") \
        .sample(fraction=f) \
        .show(n, False)


# used in the testing process, not present in final code
def show_sample_rows(df, cols, n=10, f=.01):
    df.select(*cols) \
        .sample(fraction=f) \
        .show(n, False)
 

def main():

    global spark, SRC_OFAC, SRC_UK
    # use these to identify which dataset is which in each of the functions
    SRC_OFAC, SRC_UK = "US OFAC", "UK TREASURY"

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Spark Assessment") \
        .getOrCreate()


    # OFAC process
    ofac_cols = ["source", "sourceId", "sidType", "firstName", "lastName", "middleNameList", "titleList"
            , "positionList" , "dobMap", "aliasStruct" ]

    ofac = get_ofac() # get data
    ofac = make_simple_cols(ofac) # get the simplier columns to make
    ofac = make_dob(ofac) # parse DOB string
    ofac = make_aliases(ofac) # parse alias string
    ofac = ofac.select(*ofac_cols) # get necessary subset of data
    
    # UK process
    uk_cols = ["source", "sourceId", "sidType", "firstName", "lastName", "middleNameList", "title"
            , "position" , "dobMap", "aliasStruct"]
    
    uk = get_uk_treasury() # get the data
    uk = make_simple_cols(uk) # get the easier columns to make
    uk = make_dob(uk) # parse DOB string
    uk = make_aliases(uk) # parse aliases
    uk = uk.select(*uk_cols) # get subset of data
    uk = aggregate_uk(uk) # aggregate the data so that every row is a unque entity
    
    df = ofac.union(uk)
    # df.printSchema()
    # df.sample(fraction=.01).show()
    df.coalesce(1).write.parquet("combined_output.parquet", mode = "overwrite")


if __name__ == "__main__":
    main()