package org.codecraftlabs.ssp.data

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codecraftlabs.spark.utils.DataFormat.DataFormat

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

  private val digitalPoliceReportCsvColumns: Seq[String] = Seq (
    "ID_DELEGACIA",
    "NOME_DEPARTAMENTO",
    "NOME_SECCIONAL",
    "NOME_DELEGACIA",
    "CIDADE",
    "ANO_BO",
    "NUM_BO",
    "NOME_DEPARTAMENTO_CIRC",
    "NOME_SECCIONAL_CIRC",
    "NOME_DELEGACIA_CIRC",
    "NOME_MUNICIPIO_CIRC",
    "DESCR_TIPO_BO",
    "DATA_OCORRENCIA_BO",
    "HORA_OCORRENCIA_BO",
    "DATAHORA_COMUNICACAO_BO",
    "FLAG_STATUS",
    "RUBRICA",
    "DESCR_CONDUTA",
    "DESDOBRAMENTO",
    "DESCR_TIPOLOCAL",
    "DESCR_SUBTIPOLOCAL",
    "LOGRADOURO",
    "NUMERO_LOGRADOURO",
    "LATITUDE",
    "LONGITUDE",
    "DESCR_TIPO_PESSOA",
    "FLAG_VITIMA_FATAL",
    "SEXO_PESSOA",
    "IDADE_PESSOA",
    "COR_CUTIS"
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
    "skinColor",
    "occupation",
    "educationLevel"
  )

  val DigitalPoliceReportColumns: Seq[String] = Seq (
    "policeStationId",
    "departmentName",
    "sectionName",
    "policeStationName",
    "city",
    "reportYear",
    "reportNumber",
    "circDepartmentName",
    "circSectionName",
    "circPoliceStationName",
    "circCityName",
    "reportTypeDescription",
    "reportDate",
    "reportTime",
    "communicationDateTime",
    "flagStatus",
    "signature",
    "proceedings",
    "aftermath",
    "locationTypeDescription",
    "subTypeDescription",
    "address",
    "addressNumber",
    "latitude",
    "longitude",
    "personTypeDescription",
    "fatalVictimFlag",
    "gender",
    "age",
    "skinColor"
  )

  def readContents(file: String, formatName: DataFormat, session: SparkSession, structType: StructType, hasHeader: Boolean = true, delimiter: String = ","): DataFrame = {
    session.read.format(formatName)
      .schema(structType)
      .option("header", hasHeader.toString)
      .option("delimiter", delimiter)
      .load(file)
  }


  def getDigitalReportDescriptionSchema: StructType = {
    val fieldNameField = StructField(digitalReportFieldCsvColumns.head, StringType, nullable = true)
    val descriptionField = StructField(digitalReportFieldCsvColumns(1), StringType, nullable = true)

    val fields = List(fieldNameField, descriptionField)
    StructType(fields)
  }

  def getDigitalPoliceReportSchema: StructType = {
    val policeStateIdField = StructField(digitalPoliceReportCsvColumns.head, LongType, nullable = false)
    val departmentNameField = StructField(digitalPoliceReportCsvColumns(1), StringType, nullable = false)
    val sectionNameField = StructField(digitalPoliceReportCsvColumns(2), StringType, nullable = false)
    val policeStationNameField = StructField(digitalPoliceReportCsvColumns(3), StringType, nullable = false)
    val cityField = StructField(digitalPoliceReportCsvColumns(4), StringType, nullable = false)
    val yearField = StructField(digitalPoliceReportCsvColumns(5), IntegerType, nullable = false)
    val reportNumberField = StructField(digitalPoliceReportCsvColumns(6), LongType, nullable = false)
    val circDepartmentNameField = StructField(digitalPoliceReportCsvColumns(7), StringType, nullable = false)
    val circSectionNameField = StructField(digitalPoliceReportCsvColumns(8), StringType, nullable = false)
    val circPoliceStationNameField = StructField(digitalPoliceReportCsvColumns(9), StringType, nullable = false)
    val circCityField = StructField(digitalPoliceReportCsvColumns(10), StringType, nullable = false)
    val reportTypeDescriptionField = StructField(digitalPoliceReportCsvColumns(11), StringType, nullable = false)
    val reportDateField = StructField(digitalPoliceReportCsvColumns(12), StringType, nullable = false)
    val reportTimeField = StructField(digitalPoliceReportCsvColumns(13), StringType, nullable = false)
    val reportCommunicationDateTimeField = StructField(digitalPoliceReportCsvColumns(14), StringType, nullable = false)
    val flagStatusField = StructField(digitalPoliceReportCsvColumns(15), StringType, nullable = false)
    val signatureField = StructField(digitalPoliceReportCsvColumns(16), StringType, nullable = false)
    val proceedingsField = StructField(digitalPoliceReportCsvColumns(17), StringType, nullable = false)
    val rolloutField = StructField(digitalPoliceReportCsvColumns(18), StringType, nullable = false)
    val locationTypeDescriptionField = StructField(digitalPoliceReportCsvColumns(19), StringType, nullable = false)
    val locationSubtypeDescriptionField = StructField(digitalPoliceReportCsvColumns(20), StringType, nullable = false)
    val addressField = StructField(digitalPoliceReportCsvColumns(21), StringType, nullable = false)
    val addressNumberField = StructField(digitalPoliceReportCsvColumns(22), StringType, nullable = false)
    val latitudeField = StructField(digitalPoliceReportCsvColumns(23), StringType, nullable = false)
    val longitudeField = StructField(digitalPoliceReportCsvColumns(24), StringType, nullable = false)
    val personTypeDescriptionField = StructField(digitalPoliceReportCsvColumns(25), StringType, nullable = false)
    val fatalVictimFlagField = StructField(digitalPoliceReportCsvColumns(26), StringType, nullable = false)
    val genderField = StructField(digitalPoliceReportCsvColumns(27), StringType, nullable = false)
    val ageField = StructField(digitalPoliceReportCsvColumns(28), StringType, nullable = false)
    val skinColorField = StructField(digitalPoliceReportCsvColumns(29), StringType, nullable = false)

    val fields = List(
      policeStateIdField,
      departmentNameField,
      sectionNameField,
      policeStationNameField,
      cityField,
      yearField,
      reportNumberField,
      circDepartmentNameField,
      circSectionNameField,
      circPoliceStationNameField,
      circCityField,
      reportTypeDescriptionField,
      reportDateField,
      reportTimeField,
      reportCommunicationDateTimeField,
      flagStatusField,
      signatureField,
      proceedingsField,
      rolloutField,
      locationTypeDescriptionField,
      locationSubtypeDescriptionField,
      addressField,
      addressNumberField,
      latitudeField,
      longitudeField,
      personTypeDescriptionField,
      fatalVictimFlagField,
      genderField,
      ageField,
      skinColorField
    )

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
