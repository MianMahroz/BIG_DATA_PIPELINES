package util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * This class manage all big data entities i.e spark,hadoop
 */
public class BigDataUtils {

    public static SparkSession spark = null;

    /**
     * Creates new Spark session
     * @return
     */
    public static SparkSession sparkCreateSession(){
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[2]")
                .config("spark.driver.host","127.0.0.1")
                .config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.sql.shuffle.partitions", 2)
                .config("spark.default.parallelism", 2)
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                .config("spark.eventLog.enabled", true)
                .config("spark.rdd.compress", "true")
                .config("spark.streaming.backpressure.enabled", "true")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
                .appName("StockDailyUploaderJob")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");
        spark = sparkSession;
        System.out.println("SPARK SESSION CREATED!");
        return spark;
    }



    /**
     * Reading data from local warehouse and saving it to temporary file location using spark default RDD (Resilient Distributed Dataset)
     */
    public static Dataset<Row> sparkReadFromDb(String dbName,Properties props, String sqlQuery, DataBoundaryDto boundary, String partitionCount, String partitionColumn){

        System.out.println("SPARK READING FROM DB "+sqlQuery);
        Dataset<Row> dataFrame
                = spark.read()
                .format("jdbc")
                .option("url", props.getProperty("db.mysqlUrl")+dbName) // using db.mysqlUrl buz maria driver has issues
                .option("dbtable", "( " + sqlQuery + " ) as tmpStock")
                .option("user", props.getProperty("db.user"))
                .option("password", props.getProperty("db.pass"))

                /**
                 * i.e partitionColumn is ID
                 * Means fetch ID from lowerBound to upperBount
                 * These 3 properties work combine
                 */
                .option("partitionColumn",partitionColumn)
                .option("lowerBound", boundary.getMinBound())
                .option("upperBound",boundary.getMaxBound() + 1)

                .option("numPartitions",partitionCount)
                .load();

        System.out.println("Total size of Stock DF : " + dataFrame.count());
        dataFrame.show();

        return dataFrame;
    }

    /**
     * Saving data to local file systems using spark default RDD (Resilient Distributed Dataset)
     * @param dbName
     * @param dataFrame
     */
    public static void  sparkWriteToFileSystem(String dbName,Dataset<Row> dataFrame){
        dataFrame.write()
                .mode(SaveMode.Append)
                .partitionBy("STOCK_DATE")
                .parquet("raw_data/"+dbName);

    }

    public static void closeSparkSession(){
        spark.close();
    }


    public static  Dataset<Row> sparkReadFromFile(String dbName,String sourceDir){
        Dataset<Row> dataset = spark.read().parquet(sourceDir);
        return dataset;
    }

    public static Dataset<Row> sparkAggregateData(String tableName){
        Dataset<Row> stockSummary
                = spark.sql(
                "SELECT STOCK_DATE, ITEM_NAME, " +
                        "COUNT(*) as TOTAL_REC," +
                        "SUM(OPENING_STOCK) as OPENING_STOCK, " +
                        "SUM(RECEIPTS) as RECEIPTS, " +
                        "SUM(ISSUES) as ISSUES, " +
                        "SUM( OPENING_STOCK + RECEIPTS - ISSUES) as CLOSING_STOCK, "+
                        "SUM( (OPENING_STOCK + RECEIPTS - ISSUES) * UNIT_VALUE ) as CLOSING_VALUE " +
                        "FROM  "+tableName +
                        " GROUP BY STOCK_DATE, ITEM_NAME"
        );

        System.out.println("Global Stock Summary: ");
        stockSummary.show();

        return stockSummary;

    }

    public static void sparkWriteToDb(String dbName,Dataset<Row> stockSummary,Properties props){
        //Append to the MariaDB table. Will add duplicate rows if run again
        stockSummary
                .write()
                .mode(SaveMode.Append)
                .format("jdbc")
                //Using mysql since there is a bug in mariadb connector
                //https://issues.apache.org/jira/browse/SPARK-25013
                .option("url", props.getProperty("db.mysqlUrl")+dbName) // using db.mysqlUrl buz maria driver has issues
                .option("dbtable", dbName+".item_stock")
                .option("user", props.getProperty("db.user"))
                .option("password", props.getProperty("db.pass"))
                .save();

    }

}
