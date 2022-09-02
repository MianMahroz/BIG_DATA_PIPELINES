# STORE INVENTORY BATCH PROCESSING PIPELINE

### GOAL
To create a centralized database , that contains stock details of all the warehouses of company in every country or cities.
Each warehouse has its own database.Centralized database should update on daily basis at day end.

### Pre-Requisites:
* Docker 
* Spark

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
* And then Execute DataUploader
* Finally Execute Data Processor


### Components:

* ##### DataGenerator: 
  This class is kind of utill , as it only support to generate some raw data and save into db for this example
  Like we created 3 warehouses and each warehouse has its own stock_table.
  
      private static void setUpWareHouses() throws SQLException {

        // local warehouses
        createWarehouseDbIfNotExist("GERMANY_WAREHOUSE");
        createWarehouseDbIfNotExist("LONDON_WAREHOUSE");
        createWarehouseDbIfNotExist("ENGLAND_WAREHOUSE");

        // centralized global warehouse
        createWarehouseDbIfNotExist(GLOBAL_DB_NAME);

        /**
         * add stock to warehouses
         * Un-comment below to add raw stock data to each store
         */
        addStockToWareHouse("GERMANY_WAREHOUSE");
        addStockToWareHouse("LONDON_WAREHOUSE");
        addStockToWareHouse("ENGLAND_WAREHOUSE");
      }

* ##### DataUploader:
  This class is mainly responsible to upload data from warehouses local db`s to common file systems for further processing.
  Common file system can be hadoop,S3 or your local systems as we used for this example (/raw_data) is our local file system location.
  
        var dbName = "GERMANY_WAREHOUSE";
        var startDate = "2022-10-09";
        var endDate = "2022-10-10";

        // GERMANY WARE HOUSE
        var dataFrame =  sparkReadFromDb(dbName,
                 props , GET_STOCK_DATA_SQL.replace("dbName",dbName)+"'" +  startDate + "' AND '" + endDate + "' ",
                DataReader.getDataBoundaries(dbName,startDate, endDate),
                "2","ID");


* ##### DataProcessor: 
  The main function of procesor is to read data from file system , analyze, aggregate and save to CENTRALIZED_GLOBAL_WAREHOUSE
  
            /**
         * For now, we have 3 warehouses data on our local file system,
         * We`re going to read them one by one
         * Combine all warehouse data in one temp table
         */

        Dataset<Row> combinedDataset= null;
        for (String wareHouse: AVAILABLE_WAREHOUSES){
            //read data of each warehouse and store it in separate temp views
           var dataset= sparkReadFromFile(wareHouse,"raw_data/"+wareHouse);
           combinedDataset = null==combinedDataset?dataset:combinedDataset.union(dataset);
        }

        // Creating temp table that was only accessible within one session
        combinedDataset.createOrReplaceTempView("GLOBAL_TABLE");

        // Verifying Global View creation and content
        System.out.println("Total Records available : " );
        spark.sql("SELECT count(*) FROM "+"GLOBAL_TABLE").show();

        /**
         * Analysing data from all local warehouses
         * For aggregation we are using GLOBAL_TABLE that we created using combinedDataSet of all warehouses
         */
        Dataset<Row> stockSummary =  sparkAggregateData("GLOBAL_TABLE");


        /**
         *  Save aggregated data to centralized global db
         */
        var dbName = "CENTRALIZED_GLOBAL_WAREHOUSE";
        sparkWriteToDb(dbName,stockSummary,props);  // props came by calling setUpConfig above.
        
        
        
 ### DB SHOTS:
 ![image](https://user-images.githubusercontent.com/28490692/188035328-5ae4900c-c3a6-4e44-8b2d-dfd86822923e.png)



  
  


