package org.codecraftlabs.ssp.data

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SSPDataHandler {
  val CsvColumns: Seq[String] = Seq("NUM_BO",
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

  val ColumnNames: Seq[String] = Seq("reportNumber",
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

  private def getSchema(colNames: List[String]): StructType = ???
}
