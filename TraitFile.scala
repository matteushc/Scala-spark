package com.sparkTutorial.tests

import java.text.SimpleDateFormat
import java.util.SimpleTimeZone

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TraitFile {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("readFile").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)

    val responseRDD = sparkContext.textFile("C:\\file.csv")

    val f = responseRDD.filter(x => !x.contains("_id"))

    val rs = f.map(line => {
      val splits = line.split(";")
        matches(splits(0)) + "; " +
        checkOtherValues(splits(1)) + "; " +
        convertToTimeStamp(splits(2)) + "; " +
        checkOtherValues(splits(3)) + "; " +
        clearSimcards(splits(4)) + "; " +
        getDDDTelephoneNumber(splits(5)) + "; " +
        getTelephoneNumber(splits(5)) + ";" +
        checkOtherValues(splits(6)) + "; " +
        checkOtherValues(splits(7)) + "; " +
        checkOtherValues(splits(8)) + "; " +
        checkOtherValues(splits(9)) + "; " +
        checkOtherValues(splits(10)) + "; " +
        checkOtherValues(splits(11)) + "; " +
        checkOtherValues(splits(12)) + "; " +
        checkOtherValues(splits(13)) + "; " +
        getLabelCpfCnpf(splits(14)) + ";" +
        checkCpfCnpj(splits(14)) + ";" +
        checkOtherValues(splits(15)) + "; " +
        checkOtherValues(splits(16)) + "; " +
        checkOtherValues(splits(17)) + "; " +
        checkOtherValues(splits(18)) + "; " +
        checkOtherValues(splits(19)) + "; " +
        checkOtherValues(splits(20)) + "; " +
        checkOtherValues(splits(21)) + "; " +
        checkOtherValues(splits(22)) + "; " +
        checkOtherValues(splits(23)) + "; " +
        convertToTimeStamp(splits(24)) + "; " +
        checkOtherValues(splits(25)) + "; " +
        checkOtherValues(splits(26)) + "; " +
        convertDateFormat(splits(27))

    })
    rs.saveAsTextFile("out/newteste.csv")
  }

  def checkOtherValues(data: String): String = {
    if(checkValueIsNullOrEmpty(data))  "--" else data
  }

  def checkValueIsNullOrEmpty(data: String): Boolean = {
    if(data == null || data.isEmpty || "".equals(data))  true else false
  }

  def convertTimeStampDate(data: String): String = {
    if(checkValueIsNullOrEmpty(data) ){
      "--"
    }else{
      val d = data.replace(".", "").replace("e+12", "")
      val ts: BigInt = d.toLong
      val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
      df.format(ts.toLong)
    }
  }

  def convertToTimeStamp(data: String): String = {
    if(checkValueIsNullOrEmpty(data) ){
      "--"
    }else{
      val d = data.replace(".", "").replace("e+12", "")
      val ts: BigInt = d.toLong
      val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      df.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
      df.format(ts.toLong)
    }
  }

  def convertDateFormat(data: String): String = {
    if(checkValueIsNullOrEmpty(data) || "info".equals(data)){
      "--"
    }else{
      val  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val date = simpleDateFormat.parse(data)
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date)
    }
  }

  def getDDDTelephoneNumber(number: String): String = {
    if (checkValueIsNullOrEmpty(number)) "--" else number.substring(0,2)
  }

  def getTelephoneNumber(number: String): String = {
    if (checkValueIsNullOrEmpty(number)) "--" else number.substring(2,number.length)
  }

  def matches(s: String): String = {
    //Uma forma de fazer regex em scala
    //val pattern = """.*\((.*)\)""".r
    //val pattern(valor) = s
    //valor
    //Outra forma de fazer regex em scala

    if (checkValueIsNullOrEmpty(s)){
      "--"
    }else{
      val pattern = """([0-9]+\w+)""".r
      pattern.findFirstIn(s).get
    }
  }

  def clearSimcards(s: String): String ={
    if (checkValueIsNullOrEmpty(s)) "--"  else s.replaceAll("""\"""", "").replaceAll("""\,""", """\|""")
  }

  def addZeroLeft(cpfcnpj: String, len: Int): String ={
    if("unknown".equals(cpfcnpj)){
      "--"
    }else{
      val f = "%0" + len + "d"
      f.format(cpfcnpj.toLong)
    }
  }

  def checkCpfCnpj(cpfcnpj: String): String = {
    if(checkValueIsNullOrEmpty(cpfcnpj)){
      "--"
    }else{
      if (isCnpj(cpfcnpj)) addZeroLeft(cpfcnpj, 14)
      else addZeroLeft(cpfcnpj, 11)
    }
  }

  def getLabelCpfCnpf(cpfcnpj: String): String = {
    if(checkValueIsNullOrEmpty(cpfcnpj)){
      "--"
    }else{
      if (isCnpj(cpfcnpj)) "PJ" else "PF"
    }
  }

  def isCnpj(cpfcnpj: String): Boolean = {
    if (cpfcnpj.length <= 14 && cpfcnpj.length > 11) true else false
  }

}
