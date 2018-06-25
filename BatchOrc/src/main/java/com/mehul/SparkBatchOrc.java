package com.mehul;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SparkBatchOrc {

    public static void main(String[] args) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        Runnable task = new Runnable() {
            @Override
            public void run() {
                executeTask();
            }
        };
        // Execute every 15 mins
        executor.scheduleAtFixedRate(task, 0,15, TimeUnit.MINUTES);
        try {
            // Execute for 1 hour
            executor.awaitTermination(30,TimeUnit.MINUTES);
            executor.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void executeTask(){

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
        Date d = new Date();
        String dateString = dateFormat.format(d);
        String dateWithHour = dateString.substring(0,dateString.lastIndexOf("/"));
        String min =  dateString.substring(dateString.lastIndexOf("/"));

        String input = "hdfs:///tmp/2018/*/*/*/*";
        String orcPath = "hdfs:///tmp/orctest/";
        String output =  orcPath + dateWithHour+min;

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Batch Orc Example")
                .master("local[*]")
                .getOrCreate();

        // Create an RDD
        JavaRDD<String> tweetRDD = spark.sparkContext()
                .textFile(input, 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "createddatetime|text|country|location";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split("\\|")) {
            if(fieldName.trim().equals("createddatetime")){
                StructField field = DataTypes.createStructField(fieldName, DataTypes.DateType, true);
            }
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = tweetRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split("\\|");
                return RowFactory.create(attributes);
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> tweetDataFrame = spark.createDataFrame(rowRDD, schema).toDF("Timedate","text","country","location");

        // save in orc file
        tweetDataFrame.write().format("orc").save(output);

        // Read orc file and load
        HiveContext hiveContext = new HiveContext(spark.sparkContext());
        Dataset<Row> orcDataFrame = hiveContext.read().format("orc").load(output+"/*.orc");
        orcDataFrame.createOrReplaceTempView("tweet");

        // sql query to form required result
        // Dataset<Row> results = hiveContext.sql("SELECT * from tweet");
        Dataset<Row> results = hiveContext.sql("select Timedate, collect_set(text) textinarray, country,  location from tweet group by location,Timedate,country");

        // Accessing first 200 records
        results.show(200,false);

        // Save it (it's saving in parquet format)
        // results.write().save(orcPath+"result/");
    }

}
