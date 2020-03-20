package org.codecraftlabs.ssp

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.codecraftlabs.spark.utils.ArgsUtils.parseArgs
import org.codecraftlabs.spark.utils.DataFormat.CSV
import org.codecraftlabs.spark.utils.DataSetUtil
import org.codecraftlabs.spark.utils.DataSetUtil.saveDataSetToCsv
import org.codecraftlabs.spark.utils.Timer.timed
import org.codecraftlabs.ssp.data.PoliceReportDataHandler._
import org.codecraftlabs.ssp.data.PoliceStation
import org.codecraftlabs.utils.PoliceStationDataUtil.unifyPoliceStationDataFrames

object Main {
  private val GeneralInputFolder: String = "--general-input-folder"
  private val RegularReportFolder: String = "--regular-report-folder"
  private val DigitalReportFolder: String = "--digital-report-folder"
  private val OutputFolder: String = "--output-folder"
  private val FileExtension: String = "--file-extension"
  private val ExecutionMode: String = "--execution-mode"
  private val RowNumber: Int = 10

  def main(args: Array[String]): Unit = {
    @transient lazy val logger = Logger.getLogger(getClass.getName)
    logger.info("Starting application")

    val argsMap = parseArgs(args)
    val generalInputFolder = argsMap(GeneralInputFolder)
    val regularReportFolder = argsMap(RegularReportFolder)
    val digitalReportFolder = argsMap(DigitalReportFolder)
    val fileExtension = argsMap.getOrElse(FileExtension, "*.csv")
    val executionMode = argsMap.getOrElse(ExecutionMode, "dev")
    val outputFolder = argsMap.getOrElse(OutputFolder, ".")

    val sparkSession: SparkSession = if (executionMode.equals("dev")) SparkSession.builder.appName("kaggle-crime-data-brazil-spark").master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate() else SparkSession.builder.appName("kaggle-crime-data-brazil-spark").master("local[*]").getOrCreate()
    import sparkSession.implicits._

    logger.info("Loading field description CSV")
    val digitalReportFields = timed("Reading CSV file description", readContents(s"$generalInputFolder/$fileExtension", CSV, sparkSession, getDigitalReportDescriptionSchema))
    digitalReportFields.show(RowNumber)

    logger.info("Loading BO (standard reports) CSV files")
    val policeReports = timed("Reading all police reports", readContents(s"$regularReportFolder/$fileExtension", CSV, sparkSession, getStandardPoliceReportSchema))
    val policeReportsDataFrame = timed("Changing data frame column names", policeReports.toDF(StandardPoliceReportColumnNames: _*))
    policeReportsDataFrame.show(RowNumber)

    logger.info("Loading RDO (digital reports) CSV files")
    val digitalPoliceReports = timed("Reading all digital police reports", readContents(s"$digitalReportFolder/$fileExtension", CSV, sparkSession, getDigitalPoliceReportSchema))
    val digitalPoliceReportsDataFrame = timed("Changing data frame column names", digitalPoliceReports.toDF(DigitalPoliceReportColumns: _*))
    digitalPoliceReportsDataFrame.show(RowNumber)

    // Extract police station names and ids
    val policeStationDF = unifyPoliceStationDataFrames(policeReportsDataFrame, digitalPoliceReportsDataFrame).distinct().sort("policeStationId")
    val policeStationDataSet : Dataset[PoliceStation] = policeStationDF.as[PoliceStation]
    policeStationDataSet.show(RowNumber)
    saveDataSetToCsv(policeStationDataSet, 1, s"${outputFolder}/stations.csv")

    // Extract police report occurrences
    val policeOccurrences = None
  }
}
