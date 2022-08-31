package util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PipelineUtils {


    public static final String GLOBAL_DB_NAME = "CENTRALIZED_GLOBAL_WAREHOUSE";
    public static final String CREATE_DB_SQL = "CREATE DATABASE ";
    public static final String CREATE_TABLE_SQL = "" +
            "CREATE TABLE `dbName`.`item_stock` (" +
            "  `ID` INT NULL AUTO_INCREMENT," +
            "  `STOCK_DATE` DATETIME NOT NULL," +
            "  `WAREHOUSE_ID` VARCHAR(45) NOT NULL," +
            "  `ITEM_NAME` VARCHAR(45) NOT NULL," +
            "  `OPENING_STOCK` INT NOT NULL DEFAULT 0," +
            "  `RECEIPTS` INT  NOT NULL DEFAULT 0," +
            "  `ISSUES` INT  NOT NULL DEFAULT 0," +
            "  `UNIT_VALUE` DECIMAL(10,2)  NOT NULL DEFAULT 0," +
            "  PRIMARY KEY (`ID`)," +
            "  INDEX `STOCK_DATE` (`STOCK_DATE` ASC));";

    public static final String CENTRALIZED_GLOBAL_WAREHOUSE_TABLE_SQL = "" +
            "CREATE TABLE `dbName`.`item_stock` (" +
            "  `ID` INT NULL AUTO_INCREMENT," +
            "  `STOCK_DATE` DATETIME NOT NULL," +
            "  `ITEM_NAME` VARCHAR(45) NOT NULL," +
            "  `TOTAL_REC` INT NOT NULL DEFAULT 0," +
            "  `OPENING_STOCK` INT NOT NULL DEFAULT 0," +
            "  `RECEIPTS` INT  NOT NULL DEFAULT 0," +
            "  `ISSUES` INT  NOT NULL DEFAULT 0," +
            "  `CLOSING_STOCK` INT NOT NULL DEFAULT 0," +
            "  `CLOSING_VALUE` DECIMAL(10,2)  NOT NULL DEFAULT 0," +
            "  PRIMARY KEY (`ID`)," +
            "  INDEX `STOCK_DATE` (`STOCK_DATE` ASC));";

    public static final String USE_DB_SQL = "USE DATABASE ";

    public static final String  INSERT_TO_TABLE_SQL =  "" +
            "INSERT INTO `dbName`.`item_stock`\n" +
            "(`STOCK_DATE`,`WAREHOUSE_ID`,`ITEM_NAME`,\n" +
            "`OPENING_STOCK`,`RECEIPTS`,`ISSUES`,`UNIT_VALUE`)\n" +
            "VALUES\n" +
            "(?,?,?,?,?,?,?)";

    public static Properties props = new Properties();
    /**
     * Take props path and load config from the given path
     * @return
     */
    public static void setUpConfig(){

        props.setProperty("db.url","jdbc:mariadb://localhost:3306/");
        props.setProperty("db.user","root");
        props.setProperty("db.pass","spark");

    }


    public static Map<String, Double> getRawStockItems() {
        Map<String,Double> itemValues = new HashMap<String,Double>();
        itemValues.put("Tape Dispenser",5.99);
        itemValues.put("Pencil Sharpener",10.00);
        itemValues.put("Labeling Machine",25.00);
        itemValues.put("Calculator",14.99);
        itemValues.put("Scissors",7.99);
        itemValues.put("Sticky Notes",2.00);
        itemValues.put("Notebook",2.50);
        itemValues.put("Clipboard",12.00);
        itemValues.put("Folders",1.00);
        itemValues.put("Pencil Box",2.99);

        return itemValues;
    }
}
