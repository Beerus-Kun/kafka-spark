from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession as Session
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.dataframe import DataFrame

if __name__ == "__main__":
    spark: Session = SparkSession.builder\
                        .appName('streaming')\
                        .master('local[3]')\
                        .config('spark.streaming.stopGracefullyOnShutdown', 'true')\
                        .config('spark.sql.streaming.schemaInference', 'true') \
                        .config("spark.mongodb.input.uri","mongodb://127.0.0.1/streaming.log") \
                        .config("spark.mongodb.output.uri","mongodb://127.0.0.1/streaming.log") \
                        .config('spark.sql.streaming.schemaInference', 'true') \
                        .getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    schema = StructType([
        StructField('id', IntegerType()),
        StructField('type', StringType()),
        StructField('catagory_id', IntegerType()),
        StructField('customer_id', IntegerType()),
        StructField('datetime', StringType()),
    ])

    kafka_df = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe', 'shop') \
                .option('startingOffsets', 'earliest') \
                .load()

    value_df = kafka_df.select(from_json(col('value').cast('string'), schema).alias('value'))

    mongo_df = value_df.selectExpr('value.catagory_id as catagory_id',
                                    'value.customer_id as customer_id', 
                                    'value.type as type',
                                    'value.datetime as datetime')

    def foreach_batch_function(df:DataFrame, epoch_id):
        df.write \
            .format('com.mongodb.spark.sql.DefaultSource') \
            .mode('append') \
            .save()
  
    mongo_df.writeStream.foreachBatch(foreach_batch_function).start()

    trade_df = value_df.select('value.*') \
        .withColumn('created_time', to_timestamp(col('datetime'), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('buy', expr("case when type == 'Buy' then 1 else 0 end")) \
        .withColumn('click', expr("case when type == 'Click' then 1 else 0 end")) \
        .withColumn('search', expr("case when type == 'Search' then 1 else 0 end"))

    window_agg_df = trade_df \
        .groupBy(
            window(col('created_time'), '15 minute')) \
        .agg(
            sum('buy').alias('total_buy'),
            sum('click').alias('total_click'),
            sum('search').alias('total_search')
        )
    
    # out_put_df = window_agg_df.select('window.start', 'window.end', 'total-buy', 'total-click')
    out_put_df = window_agg_df.select(to_json(named_struct(
        lit('start_time'), col('window.start'),
        lit('end_time'), col('window.end'),
        lit('total_buy'), col('total_buy'),
        lit('total_click'), col('total_click'),
        lit('total_search'), col('total_search')
    )).alias('value'))
    
    agg_query = out_put_df.writeStream \
        .format('kafka') \
        .queryName('purchase aggregation') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('topic', 'streaming') \
        .outputMode('update') \
        .option('checkpointLocation', f"chk-point-dir/purchase-agg/{datetime.now().strftime('%Y-%m-%d')}") \
        .trigger(processingTime='1 minute') \
        .start()
    


    # # write_query.awaitTermination()
    spark.streams.awaitAnyTermination()