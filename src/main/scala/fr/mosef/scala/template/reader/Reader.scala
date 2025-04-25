package fr.mosef.scala.template.reader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait Reader {

  // Lecture CSV avec séparateur customisable, avec ou sans header
  def readCSV(path: String, separator: String = ",", header: Boolean = true, inferSchema: Boolean = true): DataFrame

  // Lecture CSV avec schéma explicite
  def readCSVWithSchema(path: String, separator: String, schema: StructType): DataFrame

  // Lecture fichier Parquet
  def readParquet(path: String): DataFrame
}
