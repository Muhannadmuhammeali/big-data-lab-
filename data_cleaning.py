from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dataclean").getOrCreate()

data = [
    ("alice",34,"NY"),
    ("bob",None,"CA"),
    ("alice",34,"NY"),
    (None,23,None),
    (None,None,"TX")
]

columns = ["name","age","state"]
df=spark.createDataFrame(data,columns)
#df.show()

df_no_dupe = df.dropDuplicates()
#df_no_dupe.show()

df_no_nulls = df.dropna()
#df_no_nulls.show()

df_fill = df.fillna({"name":"unknown", "age":0,"state":"unknown"})
df_fill.show()
