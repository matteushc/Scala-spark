import org.apache.spark.ml.linalg._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object VectorArray {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder().appName("DespesasPublicas").master("local[*]").getOrCreate()

    import session.sqlContext.implicits._

    val sites = session.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("D:\\Users\\matteus-paula\\Downloads\\teste_recomend\\sites.csv")

    val df0 = sites.groupBy($"site_id").pivot("user_id").count().na.fill(0)

    val df = df0.map(d => {

      val p = d.toSeq.toArray
      val remove_first = p.drop(1)
      val val_indices = remove_first.zipWithIndex.filter(_._1 == 1L)
      val indices =  val_indices.map(_._2)
      val values =  val_indices.map(_._1.toString.toDouble)
      (p(0).toString.toLong, Vectors.sparse(remove_first.length, indices, values))

    }).toDF("id", "features")


    val indices = udf((v: SparseVector) => v.indices)

    val possibleMatches = df
      //.withColumn("key", explode(indices($"features")))
      //.transform(df => df.alias("left").join(df.alias("right"), Seq("key")))
      .transform(df => df.alias("left").crossJoin(df.alias("right")))

    val closeEnough = udf((v1: SparseVector, v2: SparseVector) =>  jaccardSimilarity(v1, v2))

    possibleMatches.withColumn("new", closeEnough($"left.features", $"right.features")).select($"left.id", $"right.id", $"new")
      .filter($"left.id".notEqual($"right.id")).show()

  }

  def jaccardSimilarity(v1: SparseVector, v2: SparseVector) = {
    val indices1 = v1.indices.toSet
    val indices2 = v2.indices.toSet

    val intersection = indices1.intersect(indices2)
    val union = indices1.union(indices2)

    (intersection.size.toDouble / union.size.toDouble).toDouble
  }

}
