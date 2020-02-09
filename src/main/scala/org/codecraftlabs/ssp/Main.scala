package org.codecraftlabs.ssp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.utils.ArgsUtils.parseArgs
import org.codecraftlabs.spark.utils.Timer.timed
import org.codecraftlabs.ssp.data.SSPDataHandler.{getStandardPoliceReportSchema, readContents}

object Main {
  private val GeneralInputFolder: String = "--general-input-folder"
  private val RegularReportFolder: String = "--regular-report-folder"
  private val DigitalReportFolder: String = "--digital-report-folder"

  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting application")

    val argsMap = parseArgs(args)
    val generalInputFolder = argsMap(GeneralInputFolder)
    val regularReportFolder = argsMap(RegularReportFolder)
    val digitalReportFolder = argsMap(DigitalReportFolder)

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-crime-data-brazil-spark").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    logger.info("Loading BO CSV files")
    val policeReports = timed("Reading all police reports", readContents(s"$regularReportFolder/*.csv", "csv", sparkSession, getStandardPoliceReportSchema))
    policeReports.show(10)
  }
}
