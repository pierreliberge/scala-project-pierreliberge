package fr.mosef.scala.template.reader.impl

import fr.mosef.scala.template.reader.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  // Lecture d'un fichier CSV
  override def readCSV(path: String, separator: String = ",", header: Boolean = true, inferSchema: Boolean = true): DataFrame = {
    sparkSession.read
      .option("sep", separator)
      .option("header", header.toString)
      .option("inferSchema", inferSchema.toString)
      .csv(path)
  }

  // Lecture d'un CSV avec un schéma explicite
  override def readCSVWithSchema(path: String, separator: String, schema: StructType): DataFrame = {
    sparkSession.read
      .option("sep", separator)
      .schema(schema)
      .csv(path)
  }

  // Lecture d'un fichier Parquet
  override def readParquet(path: String): DataFrame = {
    sparkSession.read.parquet(path)
  }
}
