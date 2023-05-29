# IMPORTING LIBRARIES
import os

from pathlib import Path

from pyspark.sql import SparkSession

# SETTING ENVIRONMENT VARIABLES - SPARK
os.environ["PYSPARK_DRIVER_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'
os.environ["PYSPARK_PYTHON"] = r'C:\Users\Emerson\SPARK\Scripts\python.exe'

# CREATING A SPARK SESSION
spark = SparkSession.builder \
    .appName("Read CSV File") \
    .getOrCreate()

def read_csv(path_csv,
             sep=";",
             header=False,
             inferschema=True,
             encoding='latin1',
             schema=None):

    # VERIFYING INFERSCHEMA (SPARK INFER DATA SCHEMA)
    if inferschema:
        df_csv = spark.read.csv(path_csv,
                                header=False,
                                inferSchema=inferschema,
                                sep=";",
                                encoding='latin1')

    else:
        df_csv = spark.read.csv(path_csv,
                                header=False,
                                inferSchema=inferschema,
                                schema=schema,
                                sep=";",
                                encoding='latin1')

    return df_csv


# GLOBAL VARIABLES
path_csv = "../DATA/orders.csv"
data_infer_schema = False

# READ THE CSV FILE
df = read_csv("../DATA/orders.csv",
              inferschema=data_infer_schema)

# Perform operations on the DataFrame
df.show()