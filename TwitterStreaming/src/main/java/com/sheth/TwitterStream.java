package com.sheth;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.Authorization;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStream {
    public static void main(String[] args) {

        // System.setProperty("hadoop.home.dir", "C:\\winutil\\");
        // Working Keys
        final String consumerKey = "l9HSqyYavbk5hP9fq25GTr8K0";
        final String consumerSecret = "eqFkm7uAKnACzXhgFFeaYVpEscZbImDJfuAOIO5XJnCYuV2sgV";
        final String accessToken = "844159584835051520-LheIdi9NuThw6nENP9zB5HKfNr4iYkb";
        final String accessTokenSecret = "pu1tsRCVYQAWd9z7Sm6xQHaVAfLOjqCRwofxFQeFFLgdm";

//        final String consumerKey = "DMqI1IdwArbstMiun2Jvg9kyz";
//        final String consumerSecret = "aM5QNfhujCOeG44pltiShwR64Z0EsWrD4iHrT8WUqzaM3icQgb";
//        final String accessToken = " 844159584835051520-88tDY5U1M6DB2DgTaDpHypj2EAEt0CZ";
//        final String accessTokenSecret = "Agd2XbUoLhUUBwmwqqpbb9RyYMYXlZYP8o5YyR1eFlu7U";

        // Commenting for now to use ConfigBuilder
//        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
//        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
//        System.setProperty("twitter4j.oauth.accessToken", accessToken);
//        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        Authorization auth = twitter.getAuthorization();

        // This is the entry point for all streaming functionality
        JavaStreamingContext ssc = new JavaStreamingContext(
                "local[4]", "Tutorial", Durations.minutes(2));
        String filters[] = new String[]{"#ipl","#india","#usa"};

        // Using context, we can create DStream(Descritized Stream) which represents stream of data
        // Each record in this stream can be thought of as a tweet
        // Internally DStream is represented by continuous series of RDDs which is an immutable, distributed dataset
        JavaDStream<Status> tweets = TwitterUtils.createStream(ssc, auth, filters);
        JavaDStream<Status> intervalTweets= tweets.window(Durations.minutes(2));
        JavaDStream<String> statuses = intervalTweets.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        String createdAt, text, country, location;
                        SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd-HH:mm");

                        createdAt = s.format(status.getCreatedAt());
                        text = status.getText().replace("\n"," ").replace("\r"," ");
                        // If user has disabled country location then we cannot retrieve it so not including it for now
                        country = status.getPlace()!=null ? status.getPlace().getCountry():"N/A";
                        location = status.getUser().getLocation();
                        return createdAt+"|"+text+"|"+country+"|"+location; }
                }
        );

//        statuses.dstream().repartition(1).saveAsTextFiles("src/test/A","Z");
        statuses.foreachRDD(new VoidFunction<JavaRDD<String>>(){
            public void call(JavaRDD<String> rdd) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
                Date d = new Date();
                String dateString = dateFormat.format(d);

                String dateWithHour = dateString.substring(0,dateString.lastIndexOf("/"));
                String min =  dateString.substring(dateString.lastIndexOf("/"));
                // path for windows
                // String path = "src/test/"+ dateWithHour+min;
                String path = "hdfs:///tmp/"+ dateWithHour+min;

                // Add header to file
                String headerSTR = "createddatetime|text|country|location";
                JavaRDD<String> header = ssc.sparkContext().parallelize(Arrays.asList(headerSTR));
                // No need to add header for every text file
                // header.union(rdd).repartition(1).saveAsTextFile(path);
                rdd.coalesce(1).saveAsTextFile(path);

                // HDFS changes

                Path srcFilePath = new Path(path+"/part-00000");
                Path destFilePath = new Path(path.substring(0,path.lastIndexOf("/"))+"/"+min.substring(1)+".txt");
                Configuration conf = new Configuration();
                FileSystem fs = null;
                try {
                    fs = FileSystem.get(conf);
                    fs.rename(srcFilePath, destFilePath);
                    fs.delete(new Path(path),true);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // saveAsTextFile generates part-00000 files along with other CRC and _SUCCESS files
                // Renaming part file as per desired structure and removing additional files and directories created by  saveAsTextFile
                /*
                File file = new File(path+"/part-00000");
                if(file.renameTo(new File(path.substring(0,path.lastIndexOf("/"))+"/"+min.substring(1)+".txt"))){
                    File delDir = new File(path);
                    String[]entries = delDir.list();
                    for(String s: entries){
                        File currentFile = new File(delDir.getPath(),s);
                        currentFile.delete();
                    }
                    delDir.delete();
                    System.out.println("Worked");
                }else{
                    System.out.println("Failed to move:"+path);
                }
                */
            }
        });

        // printing the stream
        statuses.print();
        ssc.start();
        try {
            // Run this job for 10 minutes
            ssc.awaitTerminationOrTimeout(60000*10);
            ssc.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
