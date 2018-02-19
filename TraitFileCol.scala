package com.sparkTutorial.tests

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.text.Normalizer

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object TraitFileCol{

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("readFile").setMaster("local[*]").set("spark.driver.maxResultSize", "6g")
    val sparkContext = new SparkContext(conf)

    val responseRDD = sparkContext.textFile("C:\\desenvolvimento\\trait\\test.csv")

    val f = responseRDD.filter(x => !x.contains("protocolo"))

   /* val fl = fixColumnsFile(f)
    val file = sparkContext.parallelize(fl)
    file.coalesce(1).saveAsTextFile("out/files-trait.txt")*/

    val rs = f.map(line => {
      val n = Normalizer.normalize(line, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "")
      val upperCase = n.toUpperCase
      val l = upperCase.replaceAll("\"", ";")
      val splits = l.split(";", -1)
      checkOtherValues(splits(0)) + ";" +
        getOnlyNumber(splits(1)) + ";" +
        checkOtherValues(splits(2)) + ";" +
        checkOtherValues(splits(3)) + ";" +
        emailLowerCase(splits(4)) + ";" +
        checkOtherValues(splits(5)) + ";" +
        checkOtherValues(splits(6)) + ";" +
        checkOtherValues(splits(7)) + ";" +
        checkOtherValues(splits(8)) + ";" +
        checkOtherValues(splits(9)) + ";" +
        checkOtherValues(splits(10)) + ";" +
        checkOtherValues(splits(11)) + ";" +
        checkOtherValues(splits(12)) + ";" +
        checkOtherValues(splits(13)) + ";" +
        checkOtherValues(splits(14)) + ";" +
        checkOtherValues(splits(15)) + ";" +
        checkOtherValues(splits(16)) + ";" +
        getOnlyNumber(splits(17)) + ";" +
        getOnlyNumber(splits(18)) + ";" +
        checkOtherValues(splits(19)) + ";" +
        checkOtherValues(splits(20)) + ";" +
        checkOtherValues(splits(21)) + ";" +
        checkOtherValues(splits(22)) + ";" +
        checkOtherValues(splits(23)) + ";" +
        checkOtherValues(splits(24)) + ";" +
        checkOtherValues(splits(25)) + ";" +
        checkOtherValues(splits(26)) + ";" +
        checkOtherValues(splits(27))
    })
    rs.saveAsTextFile("out/newcloud.csv")
  }

  def checkValueIsNullOrEmpty(data: String): Boolean = {
    if(data == null || data.isEmpty || "".equals(data))  true else false
  }
  def getOnlyNumber(s: String): String = {
    if (checkValueIsNullOrEmpty(s)){
      "--"
    }else{
      val pattern = """([0-9]+)""".r
      pattern.findAllIn(s.trim).mkString
    }
  }

  def emailLowerCase(s: String): String ={
    if (checkValueIsNullOrEmpty(s))  "--" else s.trim.toLowerCase
  }

  def checkOtherValues(data: String): String = {
    if(checkValueIsNullOrEmpty(data))  "--" else data.trim
  }

  def fixColumnsFile(responseRDD: RDD[String]): ListBuffer[String] ={
    val f = responseRDD.collect()
    val nl = ListBuffer[String]()

    val totList = f.length
    val totCol = 28

    var j = 0
    while (j < totList){

      val s = f(j).split("\"", -1)
      val totLiAtual = s.length

      if(totLiAtual < totCol){

        if((j + 1) < totList){
          nl += proc(j, f(j), totLiAtual)
        }

        def proc(d: Int, vl: String, atual: Int):  String = {
          val next = d + 1
          val s1 = f(next).split("\"", -1)
          val totLiNext = s1.length

          if ((atual + (totLiNext -1)) > totCol) {
            val sob = totCol - atual
            var nv = ""
            for (n2 <- 0 to (sob)) {
              nv += s1(n2)
            }
            var rv = ""
            for (n3 <- sob to (totLiNext)) {
              rv += s1(n3)
            }
            f(next) = rv
            j+=1
            nv
          } else {
            val fl = vl.concat(f(next))
            val sz = fl.split("\"", -1).length
            if(sz < totCol) {
              j=next
              j+=1
              proc(next, fl, sz)
            }else{
              j=next
              j+=1
              fl
            }
          }
        }
      }else{
        nl += f(j)
        j+=1
      }
    }
    nl
    /*var l = 0
    for(g <- nl){
      l+=1
      println(l + " - " + g)
    }*/
  }
}
