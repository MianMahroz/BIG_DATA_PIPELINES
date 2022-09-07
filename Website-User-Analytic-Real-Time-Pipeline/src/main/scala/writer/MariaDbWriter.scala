package writer

import manager.MariaDbManager
import org.apache.spark.sql.{ForeachWriter, Row}
import util.AppConstants
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema


class MariaDbWriter(val mariaDbManager: MariaDbManager,val dbName: String) extends ForeachWriter[Row]{


  override def open(partitionId: Long, epochId: Long): Boolean = {
    mariaDbManager.openConnection(dbName)
    true
  }

  override def process(record: Row): Unit = {
    println("MARIA WRITER"+record)

    mariaDbManager
      .insertSummary(
          record.get(0)
          .asInstanceOf[GenericRowWithSchema].get(0).toString,
          record.getString(1), record.getDouble(2).toString
      )
  }

  override def close(errorOrNull: Throwable): Unit = {
    mariaDbManager.closeDbConnection()
  }
}
