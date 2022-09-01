import static util.AppConstants.*;
import static util.BigDataUtils.*;



import java.sql.SQLException;

/**
 * This class is going to read data from the local warehouse
 * and save it to some file systems (hadoop,s3 or local file systems) in parquet format
 *
 * For this project we're going to read local warehouse data from maria db and save to local file system for further processing
 * Instead of local file system , you can also save data to hadoop, s3 etc.
 */
public class DataUploader {



    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");


        sparkCreateSession();


        setUpConfig();


        var dbName = "GERMANY_WAREHOUSE";
        var startDate = "2022-10-09";
        var endDate = "2022-10-10";

        var dataFrame =  sparkReadFromDb(dbName,
                 props , GET_STOCK_DATA_SQL.replace("dbName",dbName)+"'" +  startDate + "' AND '" + endDate + "' ",
                DataReader.getDataBoundaries(dbName,startDate, endDate),
                "2","ID");


        sparkWriteToFileSystem(dbName,dataFrame);



    }


}
