package org.codecraftlabs.ssp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.utils.ArgsUtils.parseArgs
import org.codecraftlabs.spark.utils.Timer.timed
import org.codecraftlabs.ssp.data.SSPDataHandler.readContents

object Main {
  private val CsvFolder: String = "--csv-folder"

  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting application")

    val argsMap = parseArgs(args)
    val csvFolder = argsMap(CsvFolder)

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-crime-data-brazil-spark").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    logger.info("Loading all CSV files")
    val policeReports = timed("Reading all police reports", readContents("csv", s"$csvFolder/*.csv", sparkSession))
  }
}
