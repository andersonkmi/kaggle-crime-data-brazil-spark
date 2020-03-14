package org.codecraftlabs.ssp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.codecraftlabs.spark.utils.ArgsUtils.parseArgs
import org.codecraftlabs.spark.utils.DataFormat.CSV
import org.codecraftlabs.spark.utils.Timer.timed
import org.codecraftlabs.ssp.data.SSPDataHandler.{getDigitalPoliceReportSchema, getDigitalReportDescriptionSchema, getStandardPoliceReportSchema, readContents}

object Main {
  private val GeneralInputFolder: String = "--general-input-folder"
  private val RegularReportFolder: String = "--regular-report-folder"
  private val DigitalReportFolder: String = "--digital-report-folder"
  private val FileExtension: String = "--file-extension"
  private val RowNumber: Int = 10

  def main(args: Array[String]): Unit = {

    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting application")

    val argsMap = parseArgs(args)
    val generalInputFolder = argsMap(GeneralInputFolder)
    val regularReportFolder = argsMap(RegularReportFolder)
    val digitalReportFolder = argsMap(DigitalReportFolder)
    val fileExtension = argsMap(FileExtension)

    val sparkSession: SparkSession = SparkSession.builder.appName("kaggle-crime-data-brazil-spark").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    logger.info("Loading field description CSV")
    val digitalReportFields = readContents(s"$generalInputFolder/$fileExtension", CSV, sparkSession, getDigitalReportDescriptionSchema)
    digitalReportFields.show(RowNumber)

    logger.info("Loading BO (standard reports) CSV files")
    val policeReports = timed("Reading all police reports", readContents(s"$regularReportFolder/$fileExtension", CSV, sparkSession, getStandardPoliceReportSchema))
    policeReports.show(RowNumber)

    logger.info("Loading RDO (digital reports) CSV files")
    val digitalPoliceReports = timed("Reading all digital police reports", readContents(s"$digitalReportFolder/$fileExtension", CSV, sparkSession, getDigitalPoliceReportSchema))
    digitalPoliceReports.show(RowNumber)
  }
}
