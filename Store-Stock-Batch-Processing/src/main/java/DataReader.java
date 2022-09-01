import util.DataBoundaryDto;

import java.sql.ResultSet;
import java.sql.SQLException;
import static util.AppConstants.*;
/**
 * This class reads data from local warehouses and provide assistance in data pipeline
 */
public class DataReader {

    /**
     * This function gets min and max value if id`s available in db
     * Using min and max , we would be able to analyze that how much data we are going to upload
     * This same info is required my spark to prepare itself for this much data using (min & max ID`s of database)
     *
     * We use dbName in table query to explicitly inform about the database from where we are expecting data
     * @return
     */
    public static DataBoundaryDto getDataBoundaries(String dbName, String startDate, String endDate) throws SQLException, ClassNotFoundException {
        // Opening db connection for particular warehouse to upload its data to centralized server
        var conn= DataGenerator.openMariaDbConnection(dbName);

        String sql =GET_DATA_BOUNDS_SQL.replace("dbName",dbName)+"'" +  startDate + "' AND '" + endDate + "' ";
        System.out.println("SQL: "+sql);
        var stmt= conn.prepareStatement(sql);
        ResultSet resultSet =  stmt.executeQuery();

        // fetching first and last id value to calculate , how many record we need to process
        DataBoundaryDto dataBoundaryObj = null;
        while (resultSet.next()){
            dataBoundaryObj = new DataBoundaryDto(resultSet.getInt(1),resultSet.getInt(2));
        }

        //closing connection
        DataGenerator.closeDbConnection();

        return dataBoundaryObj;
    }

    public static void main(String[] args) {

    }

}
