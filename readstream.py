from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType, StructField

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

schema = StructType([
    StructField("id_transaction", StringType()),
    StructField("type_transaction", StringType()),
    StructField("montant", FloatType()),
    StructField("devise", StringType()),
    StructField("date", TimestampType()),
    StructField("lieu", StringType()),
    StructField("moyen_paiement", StringType()),
    StructField("details", StructType([
        StructField("produit", StringType()),
        StructField("quantite", IntegerType()),
        StructField("prix_unitaire", FloatType())
    ])),
    StructField("utilisateur", StructType([
        StructField("id_utilisateur", StringType()),
        StructField("nom", StringType()),
        StructField("adresse", StringType()),
        StructField("email", StringType())
    ]))
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "transaction") \
    .load()

df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
             .select(from_json("json_string", schema).alias("data")) \
             .select("data.*")

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .option("path", "s3a://warehouse-transaction/elem") \
    .start()

query.awaitTermination()
