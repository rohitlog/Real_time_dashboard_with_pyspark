from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import pyspark.sql.functions as fn

import random
import dash
import dash_html_components as dhtml
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table
import pandas as pd
from pymongo import MongoClient
import pymongo

import time
import plotly

# Kafka Broker/Cluster Details
KAFKA_TOPIC_NAME_CONS = "transmessage"
KAFKA_BOOTSTRAP_SERVERS_CONS = '34.121.228.127:9092'

# Cassandra Cluster Details
cassandra_connection_host = "34.121.228.127"
cassandra_connection_port = "9042"
cassandra_keyspace_name = "trans_ks"
cassandra_table_name = "trans_message_detail_tbl"

# MongoDB Cluster Details
mongodb_host_name = "34.121.228.127"
mongodb_port_no = "27017"
mongodb_user_name = "demouser"
mongodb_password = "demouser"
mongodb_database_name = "trans_db"

def save_to_cassandra_table(current_df, epoc_id):
    print("Inside save_to_cassandra_table function")
    print("Printing epoc_id: ")
    print(epoc_id)

    current_df \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("spark.cassandra.connection.host", cassandra_connection_host) \
    .option("spark.cassandra.connection.port", cassandra_connection_port) \
    .option("keyspace", cassandra_keyspace_name) \
    .option("table", cassandra_table_name) \
    .save()
    print("Exit out of save_to_cassandra_table function")

def save_to_mongodb_collection(current_df, epoc_id, mongodb_collection_name):
    print("Inside save_to_mongodb_collection function")
    print("Printing epoc_id: ")
    print(epoc_id)
    print("Printing mongodb_collection_name: " + mongodb_collection_name)

    spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name

    current_df.write.format("mongo") \
        .mode("append") \
        .option("uri", spark_mongodb_output_uri) \
        .option("database", mongodb_database_name) \
        .option("collection", mongodb_collection_name) \
        .save()

    print("Exit out of save_to_mongodb_collection function")

if __name__ == "__main__":
    print("Real-Time Data Pipeline Started ...")

    spark = SparkSession \
        .builder \
        .appName("Real-Time Data Pipeline Demo") \
        .master("local[*]") \
        .config("spark.jars",
                "file:///D://spark_dependency_jars//mongo-java-driver-3.11.0.jar,file:///D://spark_dependency_jars//mongo-spark-connector_2.11-2.4.1.jar,file:///D://spark_dependency_jars//commons-configuration-1.10.jar,file:///D://spark_dependency_jars//jsr166e-1.1.0.jar,file:///D://spark_dependency_jars//spark-cassandra-connector-2.4.0-s_2.11.jar,file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.5.jar,file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar,file:///D://work//development//spark_structured_streaming_kafka//commons-pool2-2.8.0.jar,file:///D://work//development//spark_structured_streaming_kafka//spark-token-provider-kafka-0-10_2.12-3.0.1.jar") \
        .config("spark.executor.extraClassPath",
                "file:///D://spark_dependency_jars//mongo-java-driver-3.11.0.jar:file:///D://spark_dependency_jars//mongo-spark-connector_2.11-2.4.1.jar:file:///D://spark_dependency_jars//commons-configuration-1.10.jar:file:///D://spark_dependency_jars//jsr166e-1.1.0.jar:file:///D://spark_dependency_jars//spark-cassandra-connector-2.4.0-s_2.11.jar:file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.5.jar:file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar:file:///D://work//development//spark_structured_streaming_kafka//commons-pool2-2.8.0.jar:file:///D://work//development//spark_structured_streaming_kafka//spark-token-provider-kafka-0-10_2.12-3.0.1.jar") \
        .config("spark.executor.extraLibrary",
                "file:///D://spark_dependency_jars//mongo-java-driver-3.11.0.jar:file:///D://spark_dependency_jars//mongo-spark-connector_2.11-2.4.1.jar:file:///D://spark_dependency_jars//commons-configuration-1.10.jar:file:///D://spark_dependency_jars//jsr166e-1.1.0.jar:file:///D://spark_dependency_jars//spark-cassandra-connector-2.4.0-s_2.11.jar:file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.5.jar:file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar:file:///D://work//development//spark_structured_streaming_kafka//commons-pool2-2.8.0.jar:file:///D://work//development//spark_structured_streaming_kafka//spark-token-provider-kafka-0-10_2.12-3.0.1.jar") \
        .config("spark.driver.extraClassPath",
                "file:///D://spark_dependency_jars//mongo-java-driver-3.11.0.jar:file:///D://spark_dependency_jars//mongo-spark-connector_2.11-2.4.1.jar:file:///D://spark_dependency_jars//commons-configuration-1.10.jar:file:///D://spark_dependency_jars//jsr166e-1.1.0.jar:file:///D://spark_dependency_jars//spark-cassandra-connector-2.4.0-s_2.11.jar:file:///D://work//development//spark_structured_streaming_kafka//spark-sql-kafka-0-10_2.11-2.4.5.jar:file:///D://work//development//spark_structured_streaming_kafka//kafka-clients-1.1.0.jar:file:///D://work//development//spark_structured_streaming_kafka//commons-pool2-2.8.0.jar:file:///D://work//development//spark_structured_streaming_kafka//spark-token-provider-kafka-0-10_2.12-3.0.1.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from transmessage
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    # Code Block 4 Starts Here
    transaction_detail_schema = StructType([
      StructField("results", ArrayType(StructType([
        StructField("user", StructType([
          StructField("gender", StringType()),
          StructField("name", StructType([
            StructField("title", StringType()),
            StructField("first", StringType()),
            StructField("last", StringType())
          ])),
          StructField("location", StructType([
            StructField("street", StringType()),
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("zip", IntegerType())
          ])),
          StructField("email", StringType()),
          StructField("username", StringType()),
          StructField("password", StringType()),
          StructField("salt", StringType()),
          StructField("md5", StringType()),
          StructField("sha1", StringType()),
          StructField("sha256", StringType()),
          StructField("registered", IntegerType()),
          StructField("dob", IntegerType()),
          StructField("phone", StringType()),
          StructField("cell", StringType()),
          StructField("PPS", StringType()),
          StructField("picture", StructType([
            StructField("large", StringType()),
            StructField("medium", StringType()),
            StructField("thumbnail", StringType())
          ]))
        ]))
      ]), True)),
      StructField("nationality", StringType()),
      StructField("seed", StringType()),
      StructField("version", StringType()),
      StructField("tran_detail", StructType([
        StructField("tran_card_type", ArrayType(StringType())),
        StructField("product_id", StringType()),
        StructField("tran_amount", DoubleType())
      ]))
    ])
    # Code Block 4 Ends Here

    transaction_detail_df_1 = transaction_detail_df.selectExpr("CAST(value AS STRING)")

    transaction_detail_df_2 = transaction_detail_df_1.select(from_json(col("value"), transaction_detail_schema).alias("message_detail"))

    transaction_detail_df_3 = transaction_detail_df_2.select("message_detail.*")

    print("Printing Schema of transaction_detail_df_3: ")
    transaction_detail_df_3.printSchema()

    transaction_detail_df_4 = transaction_detail_df_3.select(explode(col("results.user")).alias("user"),
                                                            col("nationality"),
                                                            col("seed"),
                                                            col("version"),
                                                            col("tran_detail.tran_card_type").alias("tran_card_type"),
                                                            col("tran_detail.product_id").alias("product_id"),
                                                            col("tran_detail.tran_amount").alias("tran_amount")
                                                            )

    transaction_detail_df_5 = transaction_detail_df_4.select(
      col("user.gender"),
      col("user.name.title"),
      col("user.name.first"),
      col("user.name.last"),
      col("user.location.street"),
      col("user.location.city"),
      col("user.location.state"),
      col("user.location.zip"),
      col("user.email"),
      col("user.username"),
      col("user.password"),
      col("user.salt"),
      col("user.md5"),
      col("user.sha1"),
      col("user.sha256"),
      col("user.registered"),
      col("user.dob"),
      col("user.phone"),
      col("user.cell"),
      col("user.PPS"),
      col("user.picture.large"),
      col("user.picture.medium"),
      col("user.picture.thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      col("tran_card_type"),
      col("product_id"),
      col("tran_amount")
    )

    def randomCardType(transaction_card_type_list):
        return random.choice(transaction_card_type_list)

    getRandomCardType = udf(lambda transaction_card_type_list: randomCardType(transaction_card_type_list), StringType())

    transaction_detail_df_6 = transaction_detail_df_5.select(
      col("gender"),
      col("title"),
      col("first").alias("first_name"),
      col("last").alias("last_name"),
      col("street"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      concat(col("username"), round(rand() * 1000, 0).cast(IntegerType())).alias("user_id"),
      col("password"),
      col("salt"),
      col("md5"),
      col("sha1"),
      col("sha256"),
      col("registered"),
      col("dob"),
      col("phone"),
      col("cell"),
      col("PPS"),
      col("large"),
      col("medium"),
      col("thumbnail"),
      col("nationality"),
      col("seed"),
      col("version"),
      getRandomCardType(col("tran_card_type")).alias("tran_card_type"),
      concat(col("product_id"), round(rand() * 100, 0).cast(IntegerType())).alias("product_id"),
      round(rand() * col("tran_amount"), 2).alias("tran_amount")
    )

    transaction_detail_df_7 = transaction_detail_df_6.withColumn("tran_date",
      from_unixtime(col("registered"), "yyyy-MM-dd HH:mm:ss"))

    # Write raw data into HDFS
    transaction_detail_df_7.writeStream \
      .trigger(processingTime='5 seconds') \
      .format("json") \
      .option("path", "/data/json/trans_detail_raw_data") \
      .option("checkpointLocation", "/data/checkpoint/trans_detail_raw_data") \
      .start()

    transaction_detail_df_8 = transaction_detail_df_7.select(
      col("user_id"),
      col("first_name"),
      col("last_name"),
      col("gender"),
      col("city"),
      col("state"),
      col("zip"),
      col("email"),
      col("nationality"),
      col("tran_card_type"),
      col("tran_date"),
      col("product_id"),
      col("tran_amount"))

    transaction_detail_df_8 \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .foreachBatch(save_to_cassandra_table) \
    .start()

    # Data Processing/Data Transformation
    transaction_detail_df_9 = transaction_detail_df_8.withColumn("tran_year", \
      year(to_timestamp(col("tran_date"), "yyyy")))

    year_wise_total_sales_count_df = transaction_detail_df_9.groupby('tran_year').agg(
        fn.count('tran_amount').alias('tran_year_count'))

    mongodb_collection_name = "year_wise_total_sales_count"

    year_wise_total_sales_count_df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .foreachBatch(lambda current_df, epoc_id: save_to_mongodb_collection(current_df, epoc_id, mongodb_collection_name)) \
    .start()

    country_wise_total_sales_count_df = transaction_detail_df_9.groupby('nationality').agg(
        fn.count('nationality').alias('tran_country_count'))

    mongodb_collection_name_1 = "country_wise_total_sales_count"

    country_wise_total_sales_count_df \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .foreachBatch(lambda current_df, epoc_id: save_to_mongodb_collection(current_df, epoc_id, mongodb_collection_name_1)) \
    .start()

    card_type_wise_total_sales_count_df = transaction_detail_df_9.groupby('tran_card_type').agg(
        fn.count('tran_card_type').alias('tran_card_type_count'))

    card_type_wise_total_sales_df = transaction_detail_df_9.groupby('tran_card_type').agg(
        fn.sum('tran_card_type').alias('tran_card_type_total_sales'))

    year_country_wise_total_sales_df = transaction_detail_df_9.groupby("tran_year","nationality").agg(
        fn.sum('tran_amount').alias('tran_year_country_total_sales'))


    # Creating Dash Application
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    app.scripts.config.serve_locally = True
    # Code Block 1 Ends

    host_name = "34.121.228.127"
    port_no = "27017"
    user_name = "demouser"
    password = "demouser"
    database_name = "trans_db"

    connection_object = MongoClient(host=host_name, port=int(port_no))
    db_object = connection_object[database_name]
    db_object.authenticate(name=user_name, password=password)


    # Code Block 2 Starts
    def build_pd_df_from_sql():
        print("Starting build_pd_df_from_sql: " + time.strftime("%Y-%m-%d %H:%M:%S"))
        current_refresh_time_temp = None

        year_total_sales = db_object.year_wise_total_sales_count
        year_total_sales_df = pd.DataFrame(list(year_total_sales.find().sort([("_id", pymongo.DESCENDING)]).limit(100)))
        df1 = year_total_sales_df.groupby(['tran_year'], as_index=False).agg({'tran_year_count': "sum"})

        country_total_sales = db_object.country_wise_total_sales_count
        country_total_sales_df = pd.DataFrame(
            list(country_total_sales.find().sort([("_id", pymongo.DESCENDING)]).limit(100)))
        df2 = country_total_sales_df.groupby(['nationality'], as_index=False).agg({'tran_country_count': "sum"})
        # print(df2.head(100))

        print("Completing build_pd_df_from_sql: " + time.strftime("%Y-%m-%d %H:%M:%S"))
        return {"df1": df1, "df2": df2}


    # Code Block 2 Ends

    # Code Block 3 Starts
    df1_df1_dictionary_object = build_pd_df_from_sql()
    df1 = df1_df1_dictionary_object["df1"]
    df2 = df1_df1_dictionary_object["df2"]
    # Code Block 3 Ends

    # Code Block 4 Starts
    # Assign HTML Content to Dash Application Layout
    app.layout = dhtml.Div(
        [
            dhtml.H2(
                children="Real-Time Dashboard for Retail Sales Analysis",
                style={
                    "textAlign": "center",
                    "color": "#34A853",
                    'font-weight': 'bold',
                    'font-family': 'Verdana'
                }),
            dhtml.Div(
                id="current_refresh_time",
                children="Current Refresh Time: ",
                style={
                    "textAlign": "center",
                    "color": "#EA4335",
                    'font-weight': 'bold',
                    'fontSize': 12,
                    'font-family': 'Verdana'
                }
            ),
            dhtml.Div([
                dhtml.Div([
                    dcc.Graph(id='live-update-graph-bar')
                ]),

                dhtml.Div([
                    dhtml.Br(),
                    dash_table.DataTable(
                        id='datatable-country-wise',
                        columns=[
                            {"name": i, "id": i} for i in sorted(df2.columns)
                        ],
                        data=df2.to_dict(orient='records')
                    )
                ], className="six columns"),
            ], className="row"),

            dcc.Interval(
                id="interval-component",
                interval=10000,
                n_intervals=0
            )
        ]
    )


    # Code Block 4 Ends

    # Code Block 5 Starts
    @app.callback(
        Output("current_refresh_time", "children"),
        [Input("interval-component", "n_intervals")]
    )
    def update_layout(n):
        # current_refresh_time
        global current_refresh_time_temp
        current_refresh_time_temp = time.strftime("%Y-%m-%d %H:%M:%S")
        return "Current Refresh Time: {}".format(current_refresh_time_temp)


    # Code Block 5 Ends

    # Code Block 6 Starts
    @app.callback(
        Output("live-update-graph-bar", "figure"),
        [Input("interval-component", "n_intervals")]
    )
    def update_graph_bar(n):
        traces = list()
        bar_1 = plotly.graph_objs.Bar(
            x=df1["tran_year"],
            y=df1["tran_year_count"],
            name='Year')
        traces.append(bar_1)
        layout = plotly.graph_objs.Layout(
            barmode='group', xaxis_tickangle=-25, title_text="Yearly Retail Sales Count",
            title_font=dict(
                family="Verdana",
                size=14,
                color="black"
            ),
        )
        return {'data': traces, 'layout': layout}


    # Code Block 6 Ends

    # Code Block 7 Starts
    @app.callback(
        Output('datatable-country-wise', 'data'),
        [Input("interval-component", "n_intervals")])
    def update_table(n):
        global df1
        global df2

        print("In update_table")

        df1_df1_dictionary_object = build_pd_df_from_sql()
        df1 = df1_df1_dictionary_object["df1"]
        df2 = df1_df1_dictionary_object["df2"]

        return df2.to_dict(orient='records')


    # Code Block 7 Ends

    # Code Block 8 Starts
    if __name__ == "__main__":
        print("Starting Real-Time Dashboard For  ... ")
        app.run_server(port=8090, debug=True)
        # app.run_server(host="10.128.0.5", port=8090, debug=True)
    # Code Block 8 Ends

    # Write result dataframe into console for debugging purpose
    trans_detail_write_stream = year_country_wise_total_sales_df \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    trans_detail_write_stream.awaitTermination()


    print("Real-Time Data Pipeline Completed.")