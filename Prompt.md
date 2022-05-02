# Sayari Spark ETL Problem

### Context

The [Office of Foreign Assets Control](https://en.wikipedia.org/wiki/Office_of_Foreign_Assets_Control) (OFAC) publishes a list of sanctioned companies, individuals, and vessels that are prohibited from conducting any business with counterparties in the United States. If an American company violates this sanction regime, they’re likely to be heavily fined by the US government.

There are a number of other sanctions lists produced by foreign governments. The UK Treasury publishes a consolidated list of "asset freeze targets" that's somewhat similar to OFAC's list.

Processing semi-structured data like entries in these sanctions lists is an important part of what Sayari does.

### Task

The OFAC list can be found in XML [here](https://www.treasury.gov/ofac/downloads/sdn.xml).
The UK Treasury list can be found in a number of formats [here](https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets). For this task you can use whatever format you like (we use the XML, but the CSV may be simpler to work with).

Your task is to create a simple ETL workflow that reads both sanctions lists as input, and produces a single dataset as output. You should take the following steps:

1. Download both lists. Read them into Spark and take a look at the available fields.
    - Make a note of fields that contain similar information in both datasets (id, name, etc)
    - Not every type of information exists in both datasets, but most do.
2. Write a Spark job (using Scala, Java, or Python) that reads both sanctions lists, combines them together into a single dataset, and writes them out into a single dataset in parquet format.
    - This Spark job should run in local mode
    - When the same field (for example an entity name) is available in both datasets, it should be written to the same column/field in the output file.
    - Each record in the output dataset should consist of an entity from either of the two input datasets, with a column called `source` which indicates which file it was originally found in.

#### Additional Details

1. The point of this exercise is to normalize two datasets with different schemas into a single dataset with a unified schema. This means that an entity name from the OFAC list should appear in the same column/format as an entity name from the UK list. We want to see how you think to combine fields from the two datasets that may have different field names, but contain the same type of information.
2. You don't have to normalize every single field (there are a lot). The more fields you normalize, the more credit you'll get, however. 
3. The Spark application should be buildable into a `jar` if it's Java/Scala. You can use whatever build tool you like.
4. We'll be testing your application in local mode with something like the following command:

        spark-submit --master local[*] app.(py|jar)
        
5. Please submit your application as a github repo. The repo should include:
    - Both input datasets (in native format)
    - The Spark application
    - Any instructions required to run it in a README
6. Share the repo with https://github.com/jvani.

Please feel free to reach out if you have any questions, or anything is unclear.
