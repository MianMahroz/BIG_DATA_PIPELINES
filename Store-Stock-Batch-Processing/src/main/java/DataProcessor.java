import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static util.AppConstants.*;
import static util.BigDataUtils.*;



import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is mainly responsible to Aggregate data that we collect from local warehouses
 * and saved to local file system as parquet file.
 *
 * So we're going to read data from raw_data , aggregate it and them save it to global centralized db
 */
public class DataProcessor {


    public static void main(String[] args) throws AnalysisException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");

        setUpConfig();

        sparkCreateSession();


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

        closeSparkSession();

    }





}
