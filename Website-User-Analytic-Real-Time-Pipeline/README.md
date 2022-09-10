# WEBSITE USER ANALYTICS REAL TIME PIPELINE

### GOAL
1-To collect data from from kafka regarding an ecommerce user actions w.r.t time
2-Filter this data to generate Abandon Cart dataset and pushed to a new kafka topic for client to consume and analyze
3-Aggregate ecommerce user actions data by 5 second window and save it to maria db for leader board consumer.

### Pre-Requisites:
* Docker 
* Spark
* Kafka

### SETUP
* Fireup docker compose file "spark-cluster" using below command
  docker-compose -f spark-cluster.yml up -d  
  
* Download Hadooop from below URL:
  https://hadoop.apache.org/releases.html
* Extract and copy to C:/hadoop

For windows user`s only:
* Hadoop need winutills to successfully run on window machine
* Please download it from below url and paste all content to C:/hadoop/bin
* You also need to paste winutil files to C:/windows/system32

#### note: D:/tmp/spark-events/    :plz make sure you have this directory as spark uses this to store event logs 
#### or if you don`t want the event logs then just remove this property from session config :  ".config("spark.eventLog.enabled", true)"

![image](https://user-images.githubusercontent.com/28490692/188035250-360fc1b0-4674-46c7-a719-b59ce1f157b6.png)


### HOW TO RUN

* Execute DataGenerator  
* Finally, Execute KStream Listener


### Components:

* ##### DataGenerator:
  This class is mainly responsible to generate raw data for this example and make it available to kafka topic.

      // setting up environment variables
      appConstants.setUpConfig()

      dbConn = mariaDbManager.openConnection("mysql") // giving dbName that we sure is exist , also later on used to fetch schema names

      initiateGenerator()

      mariaDbManager.closeDbConnection()
     

* ##### KStream Listener: 
  This component is mainly responsible to lissen to new messages from kafka and perform operations i.e extract,transform,load

   
      System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

      // setting up environment variables
      appConstants.setUpConfig()

      // creating spark session
      sparkManager.sparkCreateSession()

      // Getting Data from topic
      val rawVisitStatsDF =  sparkManager.sparkReadStreamFromTopic();

      // As data is in binary , so we need to convert into String
      val visitStatsDF = kafkaManager.convertToJson(rawVisitStatsDF)


      // Listening to new messages
     sparkManager.sparkStartStreamingDataFrame(visitStatsDF)

      // Filtering Shopping Cart actions for generate abandon cart items
      val shoppingCartStats = visitStatsDF.filter("lastAction == 'SHOPPING_CART'")
      sparkManager.sparkWriteStreamToTopic(shoppingCartStats,"spark.streaming.carts.abandoned")


      // Create window of 5 seconds , collect , aggregate , generate summary and write to db
      val dataSummary = sparkManager.sparkCreateDataWindow(visitStatsDF)
      dataSummary
        .writeStream
        .foreach(new MariaDbWriter(mariaDbManager, dbName = appConstants.MARIA_DB_NAME)).start()  // writing summary to db for leader board consumer




      // Below is to keep program going. you can also use .start().awaitTermination()
      val latch: CountDownLatch = new CountDownLatch(1)
      latch.await


      // closing spark session
       sparkManager.closeSparkSession()

        
![image](https://user-images.githubusercontent.com/28490692/188923697-35f1142f-3f09-4c11-88fc-4fcaf1316466.png)

![image](https://user-images.githubusercontent.com/28490692/188923855-680e1686-8fe3-40cb-8148-a2cdc79731c5.png)

![image](https://user-images.githubusercontent.com/28490692/188923938-84b42a67-42e0-4d92-be64-2a8ad257d601.png)

![image](https://user-images.githubusercontent.com/28490692/188924069-ec25e2ec-31c0-4f42-a469-07a3df088f89.png)



  
  


