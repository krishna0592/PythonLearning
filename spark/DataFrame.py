from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName('PySparkLearning').getOrCreate()

#================================================================================================================================================================================
# Creating DataFrames in different ways:

def createEmptyDataFrame():
    schema = StructType()\
        .add("firstname", StringType(), True)\
        .add("middlename", StringType(), True)\
        .add("lastname", StringType(), True)
    # converting empty RDD to Dataframe
    emptyRDD = spark.sparkContext.emptyRDD()
    df = emptyRDD.toDF(schema)
    df.printSchema()
    df.show(truncate=False)
    # Creating Empty Dataframe directly with schema
    df2 = spark.createDataFrame([], schema)
    df2.printSchema()
    df2.show(truncate=False)
    # Creating Empty Dataframe directly without schema
    df3 = spark.createDataFrame([], StructType([]))
    df3.printSchema()
    df3.show(truncate=False)


#create a dataframe using List
def createDataFrameUsingList():
    simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
                  ("Michael", "Sales", "NY", 86000, 56, 20000),
                  ("Robert", "Sales", "CA", 81000, 30, 23000),
                  ("Maria", "Finance", "CA", 90000, 24, 23000)]
    column_list = ["employee_name", "department", "state", "salary", "age", "bonus"]
    df = spark.createDataFrame(data=simpleData, schema=column_list)
    df.printSchema()
    df.show(truncate=False)


#Create a dataframe using the struct type, mostly used while creating a complex dataframe
def createDataFrameUsingStruct():
    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100, ["Spark","Python"], {"hair":"black","eye":"brown"}),
        (("Michael", "Rose", ""), "40288", "M", 4300, ["Spark","Python"], {"hair":"black","eye":"brown"}),
        (("Robert", "", "Williams"), "42114", "M", 1400, ["Spark","Scala"], {"hair":"black","eye":"brown"}),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500, ["Spark","Java"], {"hair":"black","eye":"brown"}),
        (("Jen", "Mary", "Brown"), "", "F", -1, ["Spark"], {"hair":"black","eye":"brown"})
    ]
    structureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('id', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True),
        StructField('languagesatwork', ArrayType(StringType()), True),
        StructField('property', MapType(StringType(), StringType()), True)
    ])
    df = spark.createDataFrame(data=structureData,schema=structureSchema)
    df.printSchema()
    df.show(truncate=False)


#================================================================================================================================================================================
# HDFS operations:

def renamingFileInHDFS():
    # Variable decleration
    hdfs_dir = "/user/mcietl/krishna/dev/dmt/"  # your hdfs directory
    new_filename = "dmt.csv"  # new filename
    # Intialization
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path
    # list files in the directory
    list_status = fs.listStatus(path(hdfs_dir))
    # filter name of the file starts with part-
    file_name = [file.getPath().getName() for file in list_status if file.getPath().getName().startswith('part-')][0]
    # rename the file
    fs.rename(path(hdfs_dir + '' + file_name), path(hdfs_dir + '' + new_filename))
    # To delete a file in HDFS
    # fs.delete(path(hdfs_dir + '' + new_filename))


# ================================================================================================================================================================================
# Read and Write as CSV file:

def writeAsCSV():
    df = spark.sql("select * from acometl.data_monitoring_tool")
    df = df.select([f.col(c).cast("string") for c in df.columns])
    df.limit(10).repartition(1).write.options(header="true", delimiter=",", inferSchema="true").mode('overwrite').csv(
        "/user/mcietl/krishna/dev/dmt")


def readAsCSV():
    df = spark.read.options(header="true", delimiter=",", inferSchema="true").csv("/user/mcietl/krishna/dev/dmt/part-*")
    # (*.csv) - will read all the csv files in a folder
    # (part-*.csv) - will read all the csv files in a folder which starts with the name part
    # (["file1", "file2"]) - read multiple files at a same time
    # ("file1") - read only one file at a time


#================================================================================================================================================================================
# Read and Write as Hive tables:

def readHiveTable():
    df = spark.sql("select * from acometl.data_monitoring_tool")
    df.printSchema()
    df.show(truncate=False)


def writeAsHiveTable():
    df = spark.sql("select * from acometl.data_monitoring_tool")
    df.createOrReplaceTempView("temp_table")
    # creating new table in Hive
    spark.sql("drop table if exists mohakris.dmt_test")
    spark.sql("create table mohakris.dmt_test as select * from temp_table")
    # insert overwrite into an existing table
    spark.sql("insert overwrite table mohakris.dmt_test select * from temp_table")


#================================================================================================================================================================================
# Read and Write as Json files:

def readJsonFile():
    # read one json file
    df = spark.read.option("multiline","true").json("/user/mcietl/krishna/learning/output_processed_all_snowflake.json")
    df.printSchema()
    df.show(truncate=False)
    # read multiple json files
    # df = spark.read.option("multiline", "true").json(["/user/mcietl/krishna/learning/output_processed_1.json","/user/mcietl/krishna/learning/output_processed_2.json"])
    # read all the json files under a particular directory
    # df = spark.read.json("/user/mcietl/krishna/learning/*.json")
    df1 = df.selectExpr("load_method", "output_active_flag", "params.table_name as one_column_extract", "params.*", "run_order", "workflow_name")
    df1.printSchema()
    df1.show(truncate=False)


def writeAsSimpleJson():
    df = spark.read.option("multiline", "true").json("/user/mcietl/krishna/learning/output_processed_all_snowflake.json")
    # denormalising the nested json and creating simple columns
    df1 = df.selectExpr("load_method", "output_active_flag", "params.table_name as one_column_extract", "params.*","run_order", "workflow_name")
    df1.printSchema()
    df1.show(truncate=False)
    df1.write.mode("overwrite").json("/user/mcietl/krishna/dev/json")


def writeAsNestedJsonWithoutUDF():
    df = spark.read.option("multiline", "true").json("/user/mcietl/krishna/learning/output_processed_all_snowflake.json")
    # denormalising the nested json and creating simple columns
    df1 = df.selectExpr("load_method", "output_active_flag", "params.*","run_order", "workflow_name")
    df2 = df1.withColumn('target_location', f.struct(f.col('database'), f.col('schema'), f.col('table_name'))).withColumn('target_operation', f.struct(df1.load_method, df1.mode, df1.truncate_flag))
    df3 = df2.withColumn('target', f.struct(f.col('target_location'), f.col('target_operation')))
    df4 = df3.select('workflow_name', 'connection_method', 'dataset_id', 'run_order', 'target', 'output_active_flag', 'audited_flag')
    df4.printSchema()
    df4.show(truncate=False)
    df4.write.mode("overwrite").json("/user/mcietl/krishna/dev/json")


target_location_udf = udf(lambda database, schema, table_name: {
    'database': database,
    'schema': schema,
    'table_name': table_name
}, MapType(StringType(), StringType()))

target_operation_udf = udf(lambda load_method, mode, truncate_flag: {
    'load_method': load_method,
    'mode': mode,
    'truncate_flag': truncate_flag
}, MapType(StringType(), StringType()))


def writeAsNestedJsonWithUDF():
    df = spark.read.option("multiline", "true").json("/user/mcietl/krishna/learning/output_processed_all_snowflake.json")
    # denormalising the nested json and creating simple columns
    df1 = df.selectExpr("load_method", "output_active_flag", "params.*", "run_order", "workflow_name")
    df2 = df1\
        .withColumn('target_location', target_location_udf(df1['database'], df1['schema'], df1['table_name']))\
        .withColumn('target_operation', target_operation_udf(df1.load_method, df1.mode, df1.truncate_flag))
    df3 = df2.drop('database').select('workflow_name', 'connection_method', 'dataset_id', 'run_order', 'target_location', 'target_operation', 'output_active_flag', 'audited_flag')
    df3.printSchema()
    df3.show(truncate=False)
    df3.write.mode("overwrite").json("/user/mcietl/krishna/dev/json")


#================================================================================================================================================================================