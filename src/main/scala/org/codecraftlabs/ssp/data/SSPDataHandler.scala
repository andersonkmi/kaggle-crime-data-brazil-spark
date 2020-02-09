package org.codecraftlabs.ssp.data

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SSPDataHandler {
  private val standardPoliceReportCsvColumns: Seq[String] = Seq(
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
    "FLAG_STATUS_2",
    "DESCR_TIPO_PESSOA",
    "CONT_PESSOA",
    "SEXO_PESSOA",
    "IDADE_PESSOA",
    "COR",
    "DESCR_PROFISSAO",
    "DESCR_GRAU_INSTRUCAO"
  )

  private val digitalReportFieldCsvColumns: Seq[String] = Seq(
    "Field",
    "Description"
  )

  val StandardPoliceReportColumnNames: Seq[String] = Seq(
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

  def readContents(file: String, formatName: String, session: SparkSession, structType: StructType, hasHeader: Boolean = true, delimiter: String = ","): DataFrame = {
    session.read.format(formatName)
      .schema(structType)
      .option("header", hasHeader.toString)
      .option("delimiter", delimiter)
      .load(file)
  }


  def getDigitalReportDescriptionSchema: StructType = {
    val fieldNameField = StructField(digitalReportFieldCsvColumns(0), StringType, nullable = true)
    val descriptionField = StructField(digitalReportFieldCsvColumns(1), StringType, nullable = true)

    val fields = List(fieldNameField, descriptionField)
    StructType(fields)
  }

  def getStandardPoliceReportSchema: StructType = {
    val policeReportNumberField = StructField(standardPoliceReportCsvColumns.head, LongType, nullable = true)
    val policeReportYearField = StructField(standardPoliceReportCsvColumns(1), IntegerType, nullable = true)
    val policeStationIdField = StructField(standardPoliceReportCsvColumns(2), IntegerType, nullable = true)
    val departmentNameField = StructField(standardPoliceReportCsvColumns(3), StringType, nullable = true)
    val sectionNameField = StructField(standardPoliceReportCsvColumns(4), StringType, nullable = true)
    val policeStationNameField = StructField(standardPoliceReportCsvColumns(5), StringType, nullable = true)
    val circDepartmentNameField = StructField(standardPoliceReportCsvColumns(6), StringType, nullable = true)
    val circSectionNameField = StructField(standardPoliceReportCsvColumns(7), StringType, nullable = true)
    val circPoliceStationField = StructField(standardPoliceReportCsvColumns(8), StringType, nullable = true)
    val yearField = StructField(standardPoliceReportCsvColumns(9), IntegerType, nullable = true)
    val monthField = StructField(standardPoliceReportCsvColumns(10), IntegerType, nullable = true)
    val reportDateField = StructField(standardPoliceReportCsvColumns(11), StringType, nullable = true)
    val reportTimeField = StructField(standardPoliceReportCsvColumns(12), StringType, nullable = true)
    val flagStatusField = StructField(standardPoliceReportCsvColumns(13), StringType, nullable = true)
    val signatureField = StructField(standardPoliceReportCsvColumns(14), StringType, nullable = true)
    val aftermathField = StructField(standardPoliceReportCsvColumns(15), StringType, nullable = true)
    val proceedingsField = StructField(standardPoliceReportCsvColumns(16), StringType, nullable = true)
    val latitudeField = StructField(standardPoliceReportCsvColumns(17), StringType, nullable = true)
    val longitudeField = StructField(standardPoliceReportCsvColumns(18), StringType, nullable = true)
    val cityField = StructField(standardPoliceReportCsvColumns(19), StringType, nullable = true)
    val addressField = StructField(standardPoliceReportCsvColumns(20), StringType, nullable = true)
    val addressNumber = StructField(standardPoliceReportCsvColumns(21), StringType, nullable = true)
    val flagStatus2Field = StructField(standardPoliceReportCsvColumns(22), StringType, nullable = true)
    val personTypeDescriptionField = StructField(standardPoliceReportCsvColumns(23), StringType, nullable = true)
    val personContField = StructField(standardPoliceReportCsvColumns(24), StringType, nullable = true)
    val genderField = StructField(standardPoliceReportCsvColumns(25), StringType, nullable = true)
    val ageField = StructField(standardPoliceReportCsvColumns(26), StringType, nullable = true)
    val colorField = StructField(standardPoliceReportCsvColumns(27), StringType, nullable = true)
    val occupationField = StructField(standardPoliceReportCsvColumns(28), StringType, nullable = true)
    val educationLevelField = StructField(standardPoliceReportCsvColumns(29), StringType, nullable = true)

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
      flagStatusField,
      signatureField,
      aftermathField,
      proceedingsField,
      latitudeField,
      longitudeField,
      cityField,
      addressField,
      addressNumber,
      flagStatus2Field,
      personTypeDescriptionField,
      personContField,
      genderField,
      ageField,
      colorField,
      occupationField,
      educationLevelField
    )

    StructType(fields)
  }
}
