import static util.AppConstants.*;
import static util.BigDataUtils.*;



import java.sql.SQLException;

/**
 * This class is going to read data from the local warehouse
 * and save it to some file systems (hadoop,s3 or local file systems) in parquet format
 *
 * For this project we're going to read local warehouse data from maria db and save to local file system for further processing
 * Instead of local file system(raw_data) , you can also save data to hadoop, s3 etc.
 */
public class DataUploader {



    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");


        sparkCreateSession();


        setUpConfig();


        /**
         *  From local warehouse db , transferring data to file system (/raw_data)
         *  This file system can be could hadoop or s3 but for this example we use local file systems
         *
         *  You need to repeat below code for other warehouses as well
         */
        var dbName = "GERMANY_WAREHOUSE";
        var startDate = "2022-10-09";
        var endDate = "2022-10-10";

        // GERMANY WARE HOUSE
        var dataFrame =  sparkReadFromDb(dbName,
                 props , GET_STOCK_DATA_SQL.replace("dbName",dbName)+"'" +  startDate + "' AND '" + endDate + "' ",
                DataReader.getDataBoundaries(dbName,startDate, endDate),
                "2","ID");


        sparkWriteToFileSystem(dbName,dataFrame);

        // ENGLAND warehouse
         dbName = "ENGLAND_WAREHOUSE";
         var englandDataFrame =  sparkReadFromDb(dbName,
                props , GET_STOCK_DATA_SQL.replace("dbName",dbName)+"'" +  startDate + "' AND '" + endDate + "' ",
                DataReader.getDataBoundaries(dbName,startDate, endDate),
                "2","ID");


        sparkWriteToFileSystem(dbName,englandDataFrame);

        // LONDON warehouse
        dbName = "LONDON_WAREHOUSE";
        var londonDataFrame =  sparkReadFromDb(dbName,
                props , GET_STOCK_DATA_SQL.replace("dbName",dbName)+"'" +  startDate + "' AND '" + endDate + "' ",
                DataReader.getDataBoundaries(dbName,startDate, endDate),
                "2","ID");


        sparkWriteToFileSystem(dbName,londonDataFrame);




        closeSparkSession();



    }


}
