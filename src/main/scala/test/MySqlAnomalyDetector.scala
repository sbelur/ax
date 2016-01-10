package test

import java.io.{ObjectInputStream, FileInputStream, FileOutputStream, ObjectOutputStream}
import java.util

import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Created by sbelur on 06/01/16.
 */
object MySqlAnomalyDetector {



  def processRows(rawData: RDD[Row],sc:SparkContext): Unit = {
    val protocols = rawData.map(r => r.getAs[String]("protocol")).distinct().collect().zipWithIndex.toMap
    println("protocols"+protocols)
    val parseFunction = buildCategoricalAndLabelFunction(rawData,protocols)
    val data:RDD[Vector] = rawData.map(parseFunction)
    val normalizedData = data.cache();//.map(buildNormalizationFunction(data)).cache()
    println("****size ..."+normalizedData.collect().size)
    (Array(8)).map(k =>{
      println("kmeansk "+k)
      val v = clusteringScore(normalizedData, k,sc)
      println("***** for k "+k+ " score "+v)
      (k, v)}).toList.foreach(x=>println("****An entry"+x))

    normalizedData.unpersist()
    val loaded = KMeansModel.load(sc,"/Users/sbelur/work/anomalymodel")
    println(loaded)
    val oos= new ObjectOutputStream(new FileOutputStream("/Users/sbelur/work/clustermapping"))
    oos.writeObject(map)
    oos.flush()
    oos.close()

    val ois = new ObjectInputStream(new FileInputStream("/Users/sbelur/work/clustermapping"))
    println(ois.readObject().toString)


  }

  def clusteringScore(data: RDD[Vector], k: Int,sc:SparkContext): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(20)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    model.save(sc,"/Users/sbelur/work/anomalymodel")
    data.map(datum => distToCentroid(datum, model,map)._2).mean()
  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel,mapping:collection.mutable.Map[Int,ListBuffer[Double]]) = {
    //println("in distToCentroid "+mapping)
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    val d = distance(centroid, datum)
    val string: String = util.Arrays.toString(datum.toArray)
    //println("mapping "+mapping)
    if(mapping != null) {
      val existing: ListBuffer[Double] = mapping.getOrElse(cluster,ListBuffer[Double]())
      existing.+=(d)
      //println("adding entry "+cluster+" , "+existing)
      mapping.put(cluster,existing)
      //println("map2 in "+map)
    }
    (cluster,d)
  }

  var b = false;

  def buildCategoricalAndLabelFunction(rawData: RDD[Row],protocols:Map[String,Int]): (Row => Vector) = {
    (r: Row) => {
      var str = r.toString()
      str = str.replace("[","")
      str = str.replace("]","")
      //println(str)
      val line = str
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(0)
      buffer.remove(0)
      buffer.remove(0)
      //println("bufffer "+buffer)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0

      vector.insertAll(0, newProtocolFeatures)
      //if(!b){
        val string: String = util.Arrays.toString(vector.toArray)
        if(string.contains("11.0") && string.contains("50.0"))
          println("vector.toArray"+string)
      //Thread.sleep(1000)
        //b=true
      //}
      Vectors.dense(vector.toArray)
    }
  }


  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.aggregate(
      new Array[Double](numCols)
    )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
        (a, b) => a.zip(b).map(t => t._1 + t._2)
      )
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("AnomalyApp")
      .setJars(Array("/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/mysql-connector-java-5.1.34.jar"))
    println("**************************"+conf)

    val sc = new SparkContext(conf)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    val dataframe_mysql = sqlcontext.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ax")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable","testdata")
      .option("user", "root")
      .option("password", "mysql").load()
    val rdd:RDD[Row] = dataframe_mysql.rdd;//filter("filesize < 50 or (filesize > 1000 and filesize < 1100)").rdd
    //val splits = rdd.randomSplit(Array(0.7,0.3))
    anomalies(rdd,sqlcontext,sc)
    //processRows(rdd,sc)
  }

  class Customordering[T <: (Double,Vector)] extends Ordering[T]{
    def compare(x: T, y: T): Int = {
      return x._1.compareTo(y._1)
    }
  }
  val map = scala.collection.mutable.Map[Int,ListBuffer[Double]]()
  def buildAnomalyDetector(
                            data: RDD[Vector],
                            normalizeFunction: (Vector => Vector),sc:SparkContext): (Vector => Boolean) = {
    val normalizedData = data//.map(normalizeFunction)
    println("****** training size " +normalizedData.collect().size)
    Thread.sleep(1000)
    //println("running for 10")
/*
    val kmeans = new KMeans()
    kmeans.setK(8)
    kmeans.setRuns(20)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
*/
    val model = KMeansModel.load(sc,"/Users/sbelur/work/anomalymodel")
    val ois = new ObjectInputStream(new FileInputStream("/Users/sbelur/work/clustermapping"))
    val clmapping: collection.mutable.Map[Int,ListBuffer[Double]] = ois.readObject().asInstanceOf[collection.mutable.Map[Int,ListBuffer[Double]]]
    println(clmapping.getClass.getCanonicalName)

    //println("mappassed "+map + " , "+normalizedData.collect().size)
   /* val distances = normalizedData.map(datum => {
      val string = datum.toString
      val d = distToCentroid(datum, model,map)._2
      datum
    })*/
    //distances.collect()
    //distances.top(15)(new Customordering[(Double,Vector)]).mkString(" , ")

    //println("******* map "+map.toString() + " distances "+distances)
    //Thread.sleep(2000)

    //val threshold = distances.top(12)(new Customordering[(Double,Vector)]).last._1
    //println(s"threshold $threshold")
    (datum: Vector) => {
      val str = datum.toString
      val tuple: (Int, Double) = distToCentroid(datum, model,null)
      val cl = tuple._1;
      val centroid: Double = tuple._2
      //println("checking for cluster in map "+cl + " , "+map.get(cl))
      val distances:ListBuffer[Double] = clmapping.get(cl).get
      //if(datum.toString.contains("11") && datum.toString.contains("600"))
        //println("distances "+distances + " dataum "+datum + " cluster "+cl)
      distances.sortWith((d1:Double,d2:Double) => d1 < d2)
      val per = new Percentile()
      val doubles: Array[Double] = distances.toArray
      val calcThreshold95 = per.evaluate(doubles,95)
      val calcThreshold90 = per.evaluate(doubles,90)
      val calcThreshold85 = per.evaluate(doubles,85)
      val calcThreshold80 = per.evaluate(doubles,80)
      val calcThreshold75 = per.evaluate(doubles,75)
      val b = centroid >= calcThreshold90
      //if(b) {
        println("condition " + centroid + " , " + calcThreshold90 + " in cluster " + cl + " dataum " + datum)
        val string: String = util.Arrays.toString(datum.toArray)
        println("datum..." + datum + " , cluster  " +  cl + " , centroid  " + centroid+ " ,  string "+string + " , calcThresholds "+calcThreshold95 + " & "+calcThreshold90 + " & "+calcThreshold85+ " & "+calcThreshold80 + " & "+calcThreshold75)
      //}
      /*val string: String = util.Arrays.toString(datum.toArray)
      if(string.contains("1310") && (string.contains("45") )) {
        println("datum..." + datum + " , cluster  " +  cl + " , centroid  " + centroid+ " ,  string "+string + " , calcThresholds "+calcThreshold95 + " & "+calcThreshold90 + " & "+calcThreshold85+ " & "+calcThreshold80 + " & "+calcThreshold75)
      }
      //println(datum)
      if(datum.toString.contains("13")) {
        println("datum..." + datum + " , cluster  " +  cl + " , centroid  " + centroid+ " ,  string "+string + " , calcThresholds "+calcThreshold95 + " & "+calcThreshold90 + " & "+calcThreshold85+ " & "+calcThreshold80 + " & "+calcThreshold75)
      }*/
      b
    }
  }

  def anomalies(allRawData: RDD[Row],sqlc : SQLContext,sc:SparkContext) = {
    //val splits = allRawData.randomSplit(Array(0.7,0.3))
    //println("********** "+splits.size + " , "+splits(0).collect().size + " , "+splits(1).collect().size)
   val rawData = allRawData
    val protocols = rawData.map(r => r.getAs[String]("protocol")).distinct().collect().zipWithIndex.toMap
    val parseFunction = buildCategoricalAndLabelFunction(rawData,protocols)
    val originalAndData = rawData.map(line => (line, parseFunction(line)))
    val data = originalAndData.values
    val normalizeFunction = buildNormalizationFunction(data)
    val anomalyDetector = buildAnomalyDetector(data, normalizeFunction,sc)



  /* val dataframe_mysql = sqlc.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ax")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable","testdata")
      .option("user", "root")
      .option("password", "mysql").load()
    val testrdd:RDD[Row] = dataframe_mysql.rdd*/
    val testrdd = originalAndData
    //println("********* testdata size "+testrdd.collect().size)
    //val testdata = testrdd.map(line => (line, parseFunction(line)))
    val anomalies = testrdd.filter {
      case (orig,datum) => {
        val string = datum.toString
        val b = anomalyDetector(datum)
        //println(datum + " => "+b)
        b
      }
    }
    anomalies.collect.foreach(println)
  }
}
