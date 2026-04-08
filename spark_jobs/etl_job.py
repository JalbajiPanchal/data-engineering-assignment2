from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, sum as spark_sum,
    row_number
)
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_Job")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("CustomerTransactionETL") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.0.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.cassandra.connection.host", "scylladb") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_csv_to_delta(spark):
    logger.info(" Reading CSV file...")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/opt/spark/scripts/transactions.csv")

    logger.info(f" Loaded {df.count()} raw records")
    df.printSchema()

    logger.info(" Writing to Delta Lake...")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save("/opt/spark/delta-lake/customer_transactions")

    logger.info(" Delta Lake table created!")
    return df


def read_from_delta(spark):
    logger.info(" Reading from Delta Lake...")

    df = spark.read \
        .format("delta") \
        .load("/opt/spark/delta-lake/customer_transactions")

    logger.info(f" Read {df.count()} records from Delta Lake")
    return df


def transform(spark, df):

    logger.info(" Removing duplicates...")
    window = Window.partitionBy("transaction_id").orderBy("timestamp")
    df_deduped = df.withColumn("rn", row_number().over(window)) \
                   .filter(col("rn") == 1) \
                   .drop("rn")
    logger.info(f" After dedup: {df_deduped.count()} records")

    logger.info(" Filtering invalid amounts...")
    df_valid = df_deduped.filter(col("amount") > 0)
    logger.info(f" After filter: {df_valid.count()} records")

    logger.info(" Adding transaction_date column...")
    df_dated = df_valid.withColumn(
        "transaction_date",
        to_date(col("timestamp"))
    )

    logger.info(" Aggregating daily totals...")
    df_aggregated = df_dated.groupBy(
        "customer_id", "transaction_date"
    ).agg(
        spark_sum("amount").alias("daily_total")
    ).orderBy("customer_id", "transaction_date")

    logger.info(f" Aggregated: {df_aggregated.count()} rows")
    df_aggregated.show(10)

    return df_aggregated


def write_to_scylladb(df):
    logger.info(" Writing to ScyllaDB...")

    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="daily_customer_totals",
                 keyspace="transactions_ks") \
        .save()

    logger.info(" Data written to ScyllaDB!")


if __name__ == "__main__":
    spark = create_spark_session()

    load_csv_to_delta(spark)

    df = read_from_delta(spark)

    df_result = transform(spark, df)

    write_to_scylladb(df_result)

    spark.stop()
    logger.info(" ETL Job Complete!")