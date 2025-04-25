package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ProcessorImpl extends Processor {

  // Rapport par classe de passager (1ère, 2ème, 3ème)
  override def rapportParClasseDePassager(df: DataFrame): DataFrame = {
    df.groupBy("Pclass")
      .agg(
        count("PassengerId").alias("nombreDePassagers"),
        avg("Age").alias("ageMoyen")
      )
      .orderBy("Pclass")
  }

  // Rapport par sexe et classe
  override def rapportParSexeEtClasse(df: DataFrame): DataFrame = {
    df.groupBy("Sex", "Pclass")
      .agg(
        count("PassengerId").alias("nombreDePassagers"),
        avg("Age").alias("ageMoyen")
      )
      .orderBy("Sex", "Pclass")
  }

  // Rapport des passagers sans information d'embarquement
  override def rapportPassagersSansEmbarquement(df: DataFrame): DataFrame = {
    df.filter(col("Embarked").isNull || col("Embarked") === "")
      .select("PassengerId", "Name")
  }
}
