from pyspark.sql.types import *

schemas = {\
	'affs'  StructType([\
            StructField("id", LONGType(), True),\
            StructField("rank", IntegerType(), True),\
            StructField("name", StringType(), True),\
            StructField("dname", StringType(), True),\
            StructField("grid", StringType(), True),\
            StructField("page", StringType(), True),\
            StructField("wiki", StringType(), True),\
            StructField("PaperReferences", LONGType(), True),\
            StructField("citations", LONGType(), True),\
            StructField("lat", IntegerType(), True),\
            StructField("LONG", IntegerType(), True),\
            StructField("createdAt", DateType(), True)\
            ])\
}



id LONG, rank INT, name STRING, dname STRING, grid STRING,
    page STRING, wiki  STRING, PaperReferences  LONG, citations  LONG,
	lat  FLOAT, LONG LONG, createdAt DATE,
	countryCode  STRING, country  STRING

