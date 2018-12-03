An attempt to create basic Spark App to understand Streaming API and how big data works

-----------
Description
-----------

Spark streaming application which should read tweets from twitter for every 2 minutes on a twitter hashtags(list of hashtags need be read as input to the stream application)

Following are things the application should do after reading.

1.Three attributes need to read from the tweet (createddate,text,country)
2.Store contents of every 2 minutes into hdfs as pipe delimeted files in a folder structure year/month/day/hour(in 24hrs format)/minute.txt
  example if tweets are read on April 24th 2018,13:l2 then it should store it as 2018/04/24/13/2.txt
3.Files should be in following format.
 
                    createddatetime|text|country
                    2018-04-18-13:11|hi all|India
                    2018-04-18-13:12|hello spark|China
                    2018-04-18-13:11|hello scala|India
                    2018-04-18-13:12|hello hive|Mexico
                    2018-04-18-13:12|hello hbase|China
                    2018-04-18-13:12|hello python|India
 
 4.Create another spark application(batch application) which will be triggred for every 1 hour and read the above files acculmalated for an hour and convert them into following schema and store it as one orc(Optimized Row Columnar) file.
 
 Timedate|Country|Alltweetsinaarray
 2018-04-18-13:11|India|('hi all','hello scala')
 2018-04-18-13:12|China|('hello spark','hello hbase')
 2018-04-18-13:12|Mexico|('hello hive')
 2018-04-18-13:11|India|('hello python')
