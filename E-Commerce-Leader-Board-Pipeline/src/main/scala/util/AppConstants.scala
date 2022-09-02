package util

import java.util.Properties

class AppConstants {


  var props = new Properties


  val JOBS_READ_DATA_SQL = "select " +
    "job.id," +
    "job.title," +
    "job.description," +
    "job.mode_of_work as modeOfWork," +
    "job.status," +
    "job.budget_figure ," +
    "job.created," +
    "job.created_by " +
    "from post_caam job where job.created BETWEEN "

  val JOBS_READ_BOUNDARY_SQL = "select " +
    "MIN(job.id) as minBound, " +
    "MAX(job.id) as maxBound " +
    "from post_caam as job " +
    "where job.created BETWEEN"

  /**
   * Take props path and load config from the given path
   *
   * @return
   */
  def setUpConfig(): Unit = {
    props.setProperty("db.url", "jdbc:postgresql://yourUrlHere:5432/")
    props.setProperty("db.user", "user")
    props.setProperty("db.pass", "pass")
    props.setProperty("db.jobs", "db")

  }

}
