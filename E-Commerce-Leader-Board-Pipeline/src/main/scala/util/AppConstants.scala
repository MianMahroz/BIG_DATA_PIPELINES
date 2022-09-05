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
    props.setProperty("db.url", "jdbc:postgresql://caamdbinstance.ccmzrhaccii0.us-east-2.rds.amazonaws.com:5432/")
    props.setProperty("db.user", "CaamAdmin")
    props.setProperty("db.pass", "Encapsulateme1")
    props.setProperty("db.jobs", "caam_job_service_db")

    // producer props
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    /**
     * in case when custom serializer is defined
     */
  //  props.put("value.serializer","dto.JobSummarySerializer")

    props.put("acks","all")

  }

}
