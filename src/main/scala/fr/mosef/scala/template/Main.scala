package fr.mosef.scala.template

import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("❌ Veuillez fournir le chemin du fichier en argument.")
      System.exit(1)
    }

    val srcPath = args(0)
    val sparkSession = SparkSession.builder()
      .appName("Projet Spark - Traitement de données")
      .master("local[*]")
      .getOrCreate()

    val reader = new ReaderImpl(sparkSession)
    val processor = new ProcessorImpl()

    // Lecture intelligente du fichier (CSV ou Parquet)
    val df: DataFrame = if (srcPath.toLowerCase.endsWith(".parquet")) {
      println("📦 Lecture du fichier Parquet...")
      reader.readParquet(srcPath)
    } else {
      println("📄 Lecture du fichier CSV...")
      reader.readCSV(srcPath)
    }

    // Affichage du schéma pour debug
    println("📌 Schéma du DataFrame chargé :")
    df.printSchema()
    println("🧱 Colonnes détectées : " + df.columns.mkString(", "))

    // === Traitements ===
    val rapportClasse = processor.rapportParClasseDePassager(df)
    val rapportSexeClasse = processor.rapportParSexeEtClasse(df)
    val passagersSansEmbarquement = processor.rapportPassagersSansEmbarquement(df)

    // === Affichage des résultats ===
    println("\n📊 Rapport par classe de passager :")
    rapportClasse.show()

    println("\n📊 Rapport par sexe et classe :")
    rapportSexeClasse.show()

    println("\n🕵️ Passagers sans information d'embarquement :")
    passagersSansEmbarquement.show()

    sparkSession.stop()
  }
}
