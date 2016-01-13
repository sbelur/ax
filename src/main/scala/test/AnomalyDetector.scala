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
object AnomalyDetector {



  def processRows(rawData: RDD[Row],sc:SparkContext): Unit = {
    val protocols = rawData.map(r => r.getAs[String]("Transport")).distinct().collect().zipWithIndex.toMap
    println("protocols"+protocols)
    val parseFunction = buildCategoricalAndLabelFunction(rawData,protocols)
    val data:RDD[Vector] = rawData.map(parseFunction)
    val normalizedData = data.cache() //map(buildNormalizationFunction(data)).
    println("****size ..."+normalizedData.collect().size)
    Array(35).map(k =>{
      println("kmeansk "+k)
      val v = clusteringScore(normalizedData, k,sc)
      println("***** for k "+k+ " score "+v)
      (k, v)}).toList.foreach(x=>println("****An entry"+x))

    normalizedData.unpersist()
val loaded = KMeansModel.load(sc,"/Users/sbelur/work/axanomalymodel")
   //println(loaded)

    val oos= new ObjectOutputStream(new FileOutputStream("/Users/sbelur/work/axclustermapping"))
    oos.writeObject(map)
    oos.flush()
    oos.close()

    val ois = new ObjectInputStream(new FileInputStream("/Users/sbelur/work/axclustermapping"))
    //println(ois.readObject().toString)


  }

  def clusteringScore(data: RDD[Vector], k: Int,sc:SparkContext): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(0.01)
    val model = kmeans.run(data)
    model.save(sc,"/Users/sbelur/work/axanomalymodel")
    data.map(datum => distToCentroid(datum, model,map)._2).mean()
  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel,mapping:collection.mutable.Map[Int,ListBuffer[Double]]) = {
    val cluster = model.predict(datum)
    val a = datum.toArray

    val centroid = model.clusterCenters(cluster)
    val d = distance(centroid, datum)
    //if(a(2) == 1.0D && a(3) == 431.0D && (a(4) == 0.26D || a(4) == 0.25D))
      //println("****** Mapped "+datum + " to cluster "+cluster + " with dist "+d)

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
      val line = str
      val buffer = line.split(',').toBuffer

      var ctr=1
      while(ctr <= 8)  {
        buffer.remove(0)
        ctr = ctr + 1
      }
      ctr=1
      while(ctr <= 8) {
        buffer.remove(1)
        ctr = ctr +1
      }
      //println("buffer after second "+buffer)
      ctr=1
      var len= buffer.length
      while(ctr <= (len-4)) {
        buffer.remove(4)
        ctr = ctr +1
      }
      buffer.remove(3)
      var protocol = buffer.remove(0)

      //println("************ "+buffer)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0

      vector.insertAll(0, newProtocolFeatures)
/*
      //if(!b){
        val string: String = util.Arrays.toString(vector.toArray)
        if(string.contains("11.0") && string.contains("50.0"))
          println("vector.toArray"+string)
      //Thread.sleep(1000)
        //b=true
      //}
*/

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
      .option("dbtable","VLTransfers")
      .option("user", "root")
      .option("password", "mysql").load()
    val rdd:RDD[Row] = dataframe_mysql.filter("Direction = 'send'").rdd
    //val splits = rdd.randomSplit(Array(0.7,0.3))
   val tuple = setupAnomalies(rdd,sc)
    new Scheduler(sqlcontext,tuple._2,tuple._3).schedule()
    anomalies(tuple._1,sqlcontext,tuple._2)
    //processRows(rdd,sc)
  }


  def setupAnomalies(allRawData:RDD[Row],sc:SparkContext) = {
    val rawData = allRawData
    val protocols = rawData.map(r => r.getAs[String]("Transport")).distinct().collect().zipWithIndex.toMap
    println("protocols "+protocols)
    val parseFunction = buildCategoricalAndLabelFunction(rawData,protocols)
    val originalAndData = rawData.map(line => (line, parseFunction(line)))
    val data = originalAndData.values
    //val normalizeFunction = buildNormalizationFunction(data)
    val anomalyDetector = buildAnomalyDetector(data, null,sc)
    (originalAndData,anomalyDetector,protocols)
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
    val model = KMeansModel.load(sc,"/Users/sbelur/work/axanomalymodel")
    val ois = new ObjectInputStream(new FileInputStream("/Users/sbelur/work/axclustermapping"))
    val clmapping: collection.mutable.Map[Int,ListBuffer[Double]] = ois.readObject().asInstanceOf[collection.mutable.Map[Int,ListBuffer[Double]]]
    println(clmapping.getClass.getCanonicalName)

    //println("********** mappassed " + clmapping.keySet.toSeq.toString())
    clmapping.foreach({
      case (x:Int,y:ListBuffer[Double]) => {
        val top10 = y.sortWith((x:Double,y:Double) => x > y).take(10)
        val first10 = y.sortWith((x:Double,y:Double) => x < y).take(10)

        println("***** Points in cluster "+x + " are "+ top10 + " and least "+first10 + " with size "+y.size)
        Thread.sleep(3000)
      }
      case y => println(y)
    })

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
      //println("*********** "+str)
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
      val calcThreshold99 = per.evaluate(doubles,99)
      val calcThreshold95 = per.evaluate(doubles,95)
      val calcThreshold90 = per.evaluate(doubles,90)
      val calcThreshold85 = per.evaluate(doubles,85)
      val calcThreshold80 = per.evaluate(doubles,80)
      val calcThreshold75 = per.evaluate(doubles,75)
      val b = centroid > calcThreshold95 && centroid > 0
      if(b) {
        val str = datum.toString
        if(str.contains("431") && (str.contains("0.04") || str.contains("0.29") || str.contains("0.36")) || str.contains("0.3") || str.contains("0.26") || str.contains("0.06")) {
          println("condition " + centroid + " , " + calcThreshold90 + " in cluster " + cl + " dataum " + datum)
          val string: String = util.Arrays.toString(datum.toArray)
          println("datum..." + datum + " , cluster  " + cl + " , centroid  " + centroid + " ,  string " + string + " , calcThresholds " + calcThreshold99 + " & "+calcThreshold95 + " & " + calcThreshold90 + " & " + calcThreshold85 + " & " + calcThreshold80 + " & " + calcThreshold75)
        }
      }
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

  def anomalies(originalAndData: RDD[(Row,Vector)],sqlc : SQLContext, detector:Vector => Boolean) = {
    //val splits = allRawData.randomSplit(Array(0.7,0.3))
    //println("********** "+splits.size + " , "+splits(0).collect().size + " , "+splits(1).collect().size)
 /*  val rawData = allRawData
    val protocols = rawData.map(r => r.getAs[String]("protocol")).distinct().collect().zipWithIndex.toMap
    println("protocols "+protocols)
    val parseFunction = buildCategoricalAndLabelFunction(rawData,protocols)
    val originalAndData = rawData.map(line => (line, parseFunction(line)))
    val data = originalAndData.values
    val normalizeFunction = buildNormalizationFunction(data)
    val anomalyDetector = buildAnomalyDetector(data, normalizeFunction,sc)*/


    /* val dataframe_mysql = sqlc.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ax")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable","testdata")
      .option("user", "root")
      .option("password", "mysql").load()
    val testrdd:RDD[Row] = dataframe_mysql.rdd*/
    //println("********* testdata size "+testrdd.collect().size)
    //val testdata = testrdd.map(line => (line, parseFunction(line)))
    println("**** Checking for anomalies in  "+originalAndData.collect().size + " records")
    val anomalies = originalAndData.filter {
      case (orig,datum) => {
        val string = datum.toString
        val b = detector(datum)
        //println(datum + " => "+b)
        b
      }
    }
    println("******** ANOMALIES ************")
    val allkeys: Array[Row] = anomalies.keys.collect
    if(allkeys.isEmpty){
      println("******** NO ANOMALIES!!! ************")
    }
    println("total found "+allkeys.size)
    Thread.sleep(2000)

    allkeys.foreach(e => {
      println("ANOMALY => "+e)
      Thread.sleep(1000)
    })

  }
}
