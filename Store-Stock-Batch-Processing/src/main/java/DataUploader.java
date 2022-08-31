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
        var dataFrame =  sparkReadFromDb(dbName,
                 props , GET_STOCK_DATA_SQL,
                DataReader.getDataBoundaries(dbName,"2021-06-01", "2021-06-03"),
                "2","ID");

        sparkWriteToFileSystem(dbName,dataFrame);



    }


}
