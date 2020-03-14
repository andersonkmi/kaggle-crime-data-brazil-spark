package org.codecraftlabs.ssp

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object DatasetExtractorUtil {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def extractColumns(contents: DataFrame, columns: List[String]): DataFrame = {
    val firstResult = contents.select(columns.map(col): _*)
    firstResult.distinct()
  }
}
