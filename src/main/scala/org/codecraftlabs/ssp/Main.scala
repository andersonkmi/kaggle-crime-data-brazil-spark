package org.codecraftlabs.ssp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.utils.ArgsUtils.parseArgs
import org.codecraftlabs.spark.utils.Timer.timed
import org.codecraftlabs.ssp.data.SSPDataHandler.readContents

object Main {
  private val InputFolder: String = "--input-folder"
  private val GeneralInputFolder: String = "--general-input-folder"

  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting application")

    val argsMap = parseArgs(args)
    val inputFolder = argsMap(InputFolder)

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-crime-data-brazil-spark").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    logger.info("Loading all CSV files")
    val policeReports = timed("Reading all police reports", readContents(s"$inputFolder/*.csv", "csv", sparkSession))
    policeReports.show(10)
  }
}
