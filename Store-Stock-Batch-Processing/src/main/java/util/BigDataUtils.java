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

    static SparkSession spark = null;

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

        Dataset<Row> dataFrame
                = spark.read()
                .format("jdbc")
                .option("url", props.getProperty("db.url")+dbName)
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

    private static void closeSparkSession(){
        spark.close();
    }
}
