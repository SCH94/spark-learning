from pyspark.sql import SparkSession, Row

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///Azuredatabricks/spark/").appName("SPARK-SQL").getOrCreate()

def parsedata(dl):
    fields = dl.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

dl = spark.sparkContext.textFile("fakefriends.csv")
rdd = dl.map(parsedata)
print(type(rdd))

# Infer the schema, and register the DataFrame as a table.
#cache them so that we can run bunch of different queries
df = spark.createDataFrame(rdd).cache()

# to query the dataframe as database table, we have to create temporary view in this case we are calling it people.
df.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE AGE BETWEEN 13 AND 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(f"{teen[1]} -> {teen[2]} -> {teen[3]}")

# We can also use functions instead of SQL queries:
df.groupBy("age").count().orderBy("age").show()

spark.stop()