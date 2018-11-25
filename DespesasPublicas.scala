package com.sparkTutorial.sparkSql

import java.text.{Normalizer, SimpleDateFormat}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object DespesasPublicas {

  val FORMAT_TIMESTAMP = "yyyy-MM-dd HH:mm:ss"
  val FORMAT_DATE = "yyyy-MM-dd"
  val FORMAT_AMERICAN_MONTHLY = "yyyyMM"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("DespesasPublicas").master("local[*]").getOrCreate()

    val despesas = session.read
      .option("header", "true")
      .option("encoding", "ISO-8859-1")
      .option("delimiter", ";")
      .csv("D:\\Users\\matteus-paula\\Downloads\\Transferencias\\")

    despesas.printSchema()
    import session.sqlContext.implicits._

    val total = despesas.select("*").count()
    println(s" TOTAL DE REGISTROS >>> $total ")

    val removeNaStringDespesas = despesas.na.fill("--")

    val columns = removeNaStringDespesas.columns
    val despesasClean = columns.foldLeft(removeNaStringDespesas){(df, name) =>
      df.withColumn(name, toTraitValues(name))
    }

    // Use spark 2.x or more
    //primitiveDS.withColumn("data", date_format(unix_timestamp($"data", "yyyyMM").cast("timestamp"), "dd-MM-yyyy")).show()
    //Use spark 1.6 or more
    //primitiveDS.withColumn("data", from_unixtime(unix_timestamp($"data", "yyyyMM"), "dd-MM-yyyy"))
    val dp2 = despesasClean.withColumn("ANO/MES", from_unixtime(unix_timestamp($"ANO / MÊS", "yyyyMM"), "yyyy-MM-dd"))

    val d = dp2.withColumn("ANO", year($"ANO/MES"))
                            .withColumn("MES", month($"ANO/MES"))
                              .withColumn("VALOR TRANSFERIDO", regexp_replace(col("VALOR TRANSFERIDO"), ",", ".").cast("double"))

    val f = d.select("UF", "ANO", "MES", "VALOR TRANSFERIDO")
                .groupBy("UF", "ANO", "MES")
                .agg(sum("VALOR TRANSFERIDO").alias("VALOR RECEBIDO"), count("UF").alias("TOTAL REGISTROS"))
                .orderBy("ANO", "MES")

    val df = f.withColumn("VALOR REAIS RECEBIDO", toTraitCurrency("VALOR RECEBIDO"))

    val rj = df.filter(col("UF") === "RJ")
    rj.show()

    val todos = df.select("UF", "ANO", "VALOR RECEBIDO")
                  .groupBy("UF", "ANO")
                  .agg(sum("VALOR RECEBIDO").alias("VALOR RECEBIDO"), avg("VALOR RECEBIDO").alias("MEDIA"), count("UF").alias("TOTAL REGISTROS"))
                  .orderBy($"VALOR RECEBIDO".desc, $"UF", $"ANO")


    val todosDf = todos.withColumn("VALOR REAIS RECEBIDO", toTraitCurrency("VALOR RECEBIDO"))
                        .withColumn("MEDIA", toTraitCurrency("MEDIA"))
    todosDf.show(10)

  }

  /**
    * Normalize each line, remove wrong characters and set to upperCase
    * It´s necessary remove double quotes to avoid column break or shift
    * @param data String data
    * @return String value treated
    */
  def normalizeRemoveWrongCharactersAndUpperCase(data: String): String ={
    val n = Normalizer.normalize(data.toString, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
    val g = n.replaceAll("[^\\p{Print}]", "").replaceAll("\\s\\s+", "")
    val d = g.replaceAll("\\n", " ")
    d.toUpperCase.trim
  }

  /**
    * Get actual date in american format yyyy-MM
    * @param date date
    * @return date string
    */
  def formatDateString(date: String): String = {
    val simpleDateFormat = new SimpleDateFormat(FORMAT_AMERICAN_MONTHLY)
    val newDate = simpleDateFormat.parse(date)
    new SimpleDateFormat(FORMAT_DATE).format(newDate)
  }

  val traitCurrency = udf[String, String]((currency: String) => {
    val currencyReplaced = currency.replaceAll(",", ".")
    val formatter = java.text.NumberFormat.getCurrencyInstance
    formatter.format(currencyReplaced)
  })

  def showNumber(vl: Double): String ={
    val fmt = new java.text.DecimalFormat("#,##0.###")
    fmt.format(vl)
  }

  def toTraitCurrency(nameColumn: String): Column = {
    val getColumn = col(nameColumn)
    val getUdf = udf[String, Double](showNumber)
    getUdf(getColumn)
  }

  /**
    * Get string column name and apply function to transform data
    * @param nameColumn column name
    * @return column treated
    */
  def toTraitDate(nameColumn: String): Column = {
    val getColumn = col(nameColumn)
    val getUdf = udf[String, String](formatDateString)
    getUdf(getColumn)
  }

  /**
    * Get string column name and apply function to transform data
    * @param nameColumn column name
    * @return column treated
    */
  def toTraitValues(nameColumn: String): Column = {
    val getColumn = col(nameColumn)
    val getUdf = udf[String, String](normalizeRemoveWrongCharactersAndUpperCase)
    getUdf(getColumn)
  }
}
