package fr.mosef.scala.template.writer

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import java.io.InputStream

class Writer(sparkSession: SparkSession) {

  private val props: Properties = new Properties()
  private val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("application.properties")
  if (inputStream != null) {
    props.load(inputStream)
  }

  // Cette méthode affiche simplement les données au lieu de les écrire
  def write(df: DataFrame, mode: String = "overwrite", path: String, format: String = "csv"): Unit = {
    // Simplement afficher le DataFrame dans le format par défaut de Spark
    println(s"Nombre de lignes: ${df.count()}")
    df.show(20)  // Affiche les 20 premières lignes comme dans votre code original
  }
}