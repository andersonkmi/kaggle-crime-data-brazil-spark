package org.codecraftlabs.utils

import org.apache.spark.sql.DataFrame
import org.codecraftlabs.spark.utils.Timer.timed
import org.codecraftlabs.utils.DatasetExtractorUtil.extractColumns

object PoliceStationDataUtil {
  def unifyPoliceStationDataFrames(policeStationsFromReports: DataFrame, policeStationsFromDigitalReports: DataFrame): DataFrame = {
    // Extract police station names and ids
    val df1 = timed("Extracting police stations",
                                          extractColumns(policeStationsFromReports, List("policeStationId" , "policeStationName")).sort("policeStationName"))
    val df2 = timed("Extraction police stations",
                                                extractColumns(policeStationsFromDigitalReports, List("policeStationId" , "policeStationName")).sort("policeStationName"))

    // merge both data frames
    df1.join(df2, Seq("policeStationId", "policeStationName"))
  }
}
