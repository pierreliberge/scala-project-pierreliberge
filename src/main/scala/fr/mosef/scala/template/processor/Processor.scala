package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {
  def rapportParClasseDePassager(df: DataFrame): DataFrame
  def rapportParSexeEtClasse(df: DataFrame): DataFrame
  def rapportPassagersSansEmbarquement(df: DataFrame): DataFrame
}
