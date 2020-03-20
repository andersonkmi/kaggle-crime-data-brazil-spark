package org.codecraftlabs.utils

import org.apache.spark.sql.DataFrame
import org.codecraftlabs.spark.utils.Timer.timed
import org.codecraftlabs.utils.DatasetExtractorUtil.extractColumns

object ReportOccurrenceDataUtil {
  def unifyPoliceStationDataFrames(policeStationsFromReports: DataFrame, policeStationsFromDigitalReports: DataFrame): DataFrame = {
    // Extract police station names and ids
    val df1 = timed("Extracting occurrences",
      extractColumns(policeStationsFromReports, List("occurrenceName")).sort("occurrenceName"))
    val df2 = timed("Extraction occurrences",
      extractColumns(policeStationsFromDigitalReports, List("occurrenceName")).sort("occurrenceName"))

    // merge both data frames
    df1.join(df2, Seq("occurrenceName"))
  }
}
