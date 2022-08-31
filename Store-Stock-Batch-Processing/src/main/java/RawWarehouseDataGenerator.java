import util.PipelineUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import static util.PipelineUtils.*;

/**
 * This class is responsible to generate raw data for local warehouses
 * So we can use it later in Data pipelines
 */
public class RawWarehouseDataGenerator {

   static Connection warehouseConn = null;

    private static void openMariaDbConnection(String dbName) throws SQLException, ClassNotFoundException {
       String db_url= props.get("db.url").toString()+dbName;
       String db_user = props.get("db.user").toString();
       String db_pass= props.get("db.pass").toString();
       Class.forName("org.mariadb.jdbc.Driver");
       warehouseConn = DriverManager.getConnection(db_url,db_user,db_pass);
    }

    private static void closeDbConnection() throws SQLException {
        warehouseConn.close();
    }

    private static void setUpWareHouses() throws SQLException {

        // local warehouses
        createWarehouseDbIfNotExist("NEW_YORK_WAREHOUSE");
        createWarehouseDbIfNotExist("LONDON_WAREHOUSE");
        createWarehouseDbIfNotExist("ENGLAND_WAREHOUSE");


        // centralized global warehouse
        createWarehouseDbIfNotExist(GLOBAL_DB_NAME);

    }

    private static void createWarehouseDbIfNotExist(String dbName) throws SQLException {

        // fetching all db schemas to check if the db is already exist or not
        var resultSet= warehouseConn.getMetaData().getCatalogs();
        var schemaList = new ArrayList<String>();
        while (resultSet.next()){
            schemaList.add(resultSet.getString(1)); // column index 1 is schema name, index 0 probably be ID
        }

        // checking if db already exist
        if(!schemaList.contains(dbName)){
            setUpDatabase(dbName);
        }else{
            System.out.println("DB "+dbName+" already EXIST!");
        }

    }

    private static void setUpDatabase(String dbName) throws SQLException {
        // creating database per warehouse
        var stmt = warehouseConn.createStatement();
        stmt.executeUpdate(CREATE_DB_SQL+dbName);

        /**
         *  creating stock table & dynamically inserting dbName  into query using replace method
         *  If db is GLOBAL CENTRALIZED then change the table query as it requires to store aggregated info.
         */
        stmt.executeUpdate((dbName.equals(GLOBAL_DB_NAME)?CENTRALIZED_GLOBAL_WAREHOUSE_TABLE_SQL:CREATE_TABLE_SQL)
                .replace("dbName",dbName));

        System.out.println("DB "+dbName+ " SETUP COMPLETED.");
        /**
         * In case of you want to use some different user other that root, execute below commands as well
         *    schemaStmt.executeUpdate("GRANT ALL PRIVILEGES ON warehouse_stock.* to 'spark'@'%'");
         *    schemaStmt.executeUpdate("FLUSH PRIVILEGES");
         */

    }


    private static void readFromWareHouse() {

    }

    private void writeToWareHouse(){

    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        // setting up application properties
        PipelineUtils.setUpConfig();

        // opening db connection for db operations
        openMariaDbConnection("mysql");

        // setting up raw databases for warehouse for data pipeline
        setUpWareHouses();

        closeDbConnection();
    }
}
