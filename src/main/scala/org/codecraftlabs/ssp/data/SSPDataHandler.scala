package org.codecraftlabs.ssp.data

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SSPDataHandler {
  val CsvColumns: Seq[String] = Seq(
    "NUM_BO",
    "ANO_BO",
    "ID_DELEGACIA",
    "NOME_DEPARTAMENTO",
    "NOME_SECCIONAL",
    "DELEGACIA",
    "NOME_DEPARTAMENTO_CIRC",
    "NOME_SECCIONAL_CIRC",
    "NOME_DELEGACIA_CIRC",
    "ANO",
    "MES",
    "DATA_OCORRENCIA_BO",
    "HORA_OCORRENCIA_BO",
    "FLAG_STATUS",
    "RUBRICA",
    "DESDOBRAMENTO",
    "CONDUTA",
    "LATITUDE",
    "LONGITUDE",
    "CIDADE",
    "LOGRADOURO",
    "NUMERO_LOGRADOURO",
    "FLAG_STATUS",
    "DESCR_TIPO_PESSOA",
    "CONT_PESSOA",
    "SEXO_PESSOA",
    "IDADE_PESSOA",
    "COR",
    "DESCR_PROFISSAO",
    "DESCR_GRAU_INSTRUCAO"
  )

  val ColumnNames: Seq[String] = Seq(
    "reportNumber",
    "reportYear",
    "policeStationId",
    "departmentName",
    "sectionName",
    "policeStationName",
    "circDepartmentName",
    "circSectionName",
    "circPoliceStation",
    "year",
    "month",
    "reportDate",
    "reportTime",
    "flagStatus",
    "signature",
    "aftermath",
    "proceedings",
    "latitude",
    "longitude",
    "city",
    "address",
    "addressNumber",
    "flagStatus2",
    "personTypeDescription",
    "personCont",
    "gender",
    "age",
    "color",
    "occupation",
    "educationLevel"
  )

  def readContents(file: String, session: SparkSession, hasHeader: Boolean = true, delimiter: String = ","): DataFrame = {
    session.read.format("csv").schema(getSchema(CsvColumns.toList)).option("header", hasHeader.toString).option("delimiter", delimiter).load(file)
  }

  private def getSchema(colNames: List[String]): StructType = {
    val policeReportNumberField = StructField(colNames.head, LongType, nullable = true)
    val policeReportYearField = StructField(colNames(1), IntegerType, nullable = true)
    val policeStationIdField = StructField(colNames(2), IntegerType, nullable = true)
    val departmentNameField = StructField(colNames(3), StringType, nullable = true)
    val sectionNameField = StructField(colNames(4), StringType, nullable = true)
    val policeStationNameField = StructField(colNames(5), StringType, nullable = true)
    val circDepartmentNameField = StructField(colNames(6), StringType, nullable = true)
    val circSectionNameField = StructField(colNames(7), StringType, nullable = true)
    val circPoliceStationField = StructField(colNames(8), StringType, nullable = true)
    val yearField = StructField(colNames(9), IntegerType, nullable = true)
    val monthField = StructField(colNames(10), IntegerType, nullable = true)
    val reportDateField = StructField(colNames(11), StringType, nullable = true)
    val reportTimeField = StructField(colNames(12), StringType, nullable = true)
    val flagStatusField = StructField(colNames(13), StringType, nullable = true)

    val fields = List(
      policeReportNumberField,
      policeReportYearField,
      policeStationIdField,
      departmentNameField,
      sectionNameField,
      policeStationNameField,
      circDepartmentNameField,
      circSectionNameField,
      circPoliceStationField,
      yearField,
      monthField,
      reportDateField,
      reportTimeField,
      flagStatusField
    )

    StructType(fields)
  }
}
