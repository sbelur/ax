package ax.poc

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


  var normalized = collection.mutable.Map[String, Array[Double]]()

  def processRows(rawData: RDD[Row], sc: SparkContext): Unit = {
    val protocols = rawData.map(r => r.getAs[String]("Transport")).distinct().collect().zipWithIndex.toMap
    println("protocols" + protocols)
    val parseFunction = buildCategoricalAndLabelFunction(rawData, protocols)
    val data: RDD[Vector] = rawData.map(parseFunction)
    val normalizedData = data.map(buildNormalizationFunction(data)).cache()
    println("****size ..." + normalizedData.collect().size)
    Array(13).map(k => {
      println("kmeansk " + k)
      val v = clusteringScore(normalizedData, k, sc)
      println("***** for k " + k + " score " + v)
      (k, v)
    }).toList.foreach(x => println("****An entry" + x))
    println("COST " + cost)
    normalizedData.unpersist()




    val loaded = KMeansModel.load(sc, "/Users/sbelur/work/axanomalymodel")
    //println(loaded)

    var oos = new ObjectOutputStream(new FileOutputStream("/Users/sbelur/work/axclustermapping"))
    oos.writeObject(map)
    oos.flush()
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream("/Users/sbelur/work/axprotocolmapping"))
    oos.writeObject(protocols)
    oos.flush()
    oos.close()

    oos = new ObjectOutputStream(new FileOutputStream("/Users/sbelur/work/axnormalizedmapping"))
    oos.writeObject(normalized)
    oos.flush()
    oos.close()





    //println(ois.readObject().toString)


  }

  val cost = collection.mutable.Map[Int, Double]()

  def clusteringScore(data: RDD[Vector], k: Int, sc: SparkContext): Double = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(0.000001)
    val model = kmeans.run(data)
    //println("******clustercost "+k + " => "+model.computeCost(data))
    cost(k) = model.computeCost(data)
    model.save(sc, "/Users/sbelur/work/axanomalymodel")
    data.map(datum => distToCentroid(datum, model, map)._2).mean()
  }

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel, mapping: collection.mutable.Map[Int, ListBuffer[Double]]) = {
    val cluster = model.predict(datum)
    val a = datum.toArray

    val centroid = model.clusterCenters(cluster)
    val d = distance(centroid, datum)
    //if(a(2) == 1.0D && a(3) == 431.0D && (a(4) == 0.26D || a(4) == 0.25D))
    //println("****** Mapped "+datum + " to cluster "+cluster + " with dist "+d)

    val string: String = util.Arrays.toString(datum.toArray)
    //println("mapping "+mapping)
    if (mapping != null) {
      val existing: ListBuffer[Double] = mapping.getOrElse(cluster, ListBuffer[Double]())
      existing.+=(d)
      //println("adding entry "+cluster+" , "+existing)
      mapping.put(cluster, existing)
      //println("map2 in "+map)
    }
    (cluster, d)
  }

  var b = false;

  def buildCategoricalAndLabelFunction(rawData: RDD[Row], protocols: Map[String, Int]): (Row => Vector) = {
    (r: Row) => {
      var str = r.toString()
      str = str.replace("[", "")
      str = str.replace("]", "")
      val line = str
      val buffer = line.split(',').toBuffer

      var ctr = 1
      while (ctr <= 8) {
        buffer.remove(0)
        ctr = ctr + 1
      }
      ctr = 1
      while (ctr <= 8) {
        buffer.remove(1)
        ctr = ctr + 1
      }
      //println("buffer after second "+buffer)
      ctr = 1
      var len = buffer.length
      while (ctr <= (len - 4)) {
        buffer.remove(4)
        ctr = ctr + 1
      }
      buffer.remove(3)
      var protocol = buffer.remove(0)

      println("************ "+buffer + " row "+r.toString())
      val vector = buffer.map(_.toDouble)
      /*x => {
        try {
          x.toDouble
        }
        catch {
          case e => 0
        }
      })
*/
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
    val stdevs: Array[Double] = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means: Array[Double] = sums.map(_ / n)
    normalized("mean") = means
    normalized("stdev") = stdevs
    println("got normalized " + normalized)
    normalizer
  }

  def normalizer: (Vector) => Vector = {
    println("in normalizer " + normalized)
    (datum: Vector) => {
      val normalizedArray = (datum.toArray, normalized("mean"), normalized("stdev")).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0) (value - mean) else (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
  }

  var sc: SparkContext = _

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("AnomalyApp")
      .setJars(Array("/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/mysql-connector-java-5.1.34.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-core-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/elasticsearch-2.1.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-analyzers-common-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-queries-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-join-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-queryparser-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-sandbox-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-highlighter-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-memory-5.3.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/guava-18.0.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/jsr166e-1.1.0.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/t-digest-3.0.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/hppc-0.7.1.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/jackson-dataformat-smile-2.6.2.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/jackson-core-2.6.2.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/jackson-dataformat-yaml-2.6.2.jar",
      "/Users/sbelur/work/spark-1.5.2-bin-hadoop2.6/lib/lucene-suggest-5.3.1.jar"))

    println("**************************" + conf)

    sc = new SparkContext(conf)
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    val dataframe_mysql = sqlcontext.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ax")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "VLTransfers")
      .option("user", "root")
      .option("password", "mysql").load()
    val rdd: RDD[Row] = dataframe_mysql.filter("Direction = 'send'").rdd
    //val splits = rdd.randomSplit(Array(0.7,0.3))
    val tuple = setupAnomalies(rdd, sc)
    new Scheduler(sqlcontext, tuple._1, tuple._2).schedule()
    //anomalies(tuple._1, sqlcontext, tuple._2)
    //processRows(rdd, sc)
  }


  def setupAnomalies(allRawData: RDD[Row], sc: SparkContext) = {
    val rawData = allRawData
    var ois = new ObjectInputStream(new FileInputStream("/Users/sbelur/work/axprotocolmapping"))
    val protocols = ois.readObject().asInstanceOf[Map[String, Int]]

    ois = new ObjectInputStream(new FileInputStream("/Users/sbelur/work/axnormalizedmapping"))
    normalized = ois.readObject().asInstanceOf[collection.mutable.Map[String, Array[Double]]]

    //val protocols = rawData.map(r => r.getAs[String]("Transport")).distinct().collect().zipWithIndex.toMap
    println("******protocols " + protocols)
    println("******normalized " + normalized)
    val anomalyDetector = buildAnomalyDetector(sc)
    (anomalyDetector, protocols)
  }


  class Customordering[T <: (Double, Vector)] extends Ordering[T] {
    def compare(x: T, y: T): Int = {
      return x._1.compareTo(y._1)
    }
  }

  val map = scala.collection.mutable.Map[Int, ListBuffer[Double]]()

  def buildAnomalyDetector(sc: SparkContext): (Vector => Boolean) = {
    val model = KMeansModel.load(sc, "/Users/sbelur/work/axanomalymodel")
    var ois = new ObjectInputStream(new FileInputStream("/Users/sbelur/work/axclustermapping"))
    val clmapping: collection.mutable.Map[Int, ListBuffer[Double]] = ois.readObject().asInstanceOf[collection.mutable.Map[Int, ListBuffer[Double]]]
    println(clmapping.getClass.getCanonicalName)



    clmapping.foreach({
      case (x: Int, y: ListBuffer[Double]) => {
        val top10 = y.sortWith((x: Double, y: Double) => x > y).take(10)
        val first10 = y.sortWith((x: Double, y: Double) => x < y).take(10)

        println("***** Points in cluster " + x + " are " + top10 + " and least " + first10 + " with size " + y.size)
        Thread.sleep(1000)
      }
      case y => println(y)
    })


    (datum: Vector) => {
      //var str = datum.toString
      //println("*********** "+str)

      val tuple: (Int, Double) = distToCentroid(datum, model, null)
      val cl = tuple._1;
      val centroid: Double = tuple._2
      //println("checking for cluster in map "+cl + " , "+map.get(cl))
      val distances: ListBuffer[Double] = clmapping.get(cl).get
      //if(datum.toString.contains("11") && datum.toString.contains("600"))
      //println("distances "+distances + " dataum "+datum + " cluster "+cl)
      distances.sortWith((d1: Double, d2: Double) => d1 < d2)
      val per = new Percentile()
      val doubles: Array[Double] = distances.toArray
      val calcThreshold100 = per.evaluate(doubles, 100)
      val calcThreshold99 = per.evaluate(doubles, 99)
      val calcThreshold95 = per.evaluate(doubles, 95)
      val calcThreshold90 = per.evaluate(doubles, 90)
      val calcThreshold85 = per.evaluate(doubles, 85)
      val calcThreshold80 = per.evaluate(doubles, 80)
      val calcThreshold75 = per.evaluate(doubles, 75)
      val b = centroid > calcThreshold99 && centroid > 0
      //if (b) {
      val str = datum.toString
      //if(str.contains("0.02") && str.contains("5893") ) {
      println("condition " + centroid + " , " + calcThreshold99 + " in cluster " + cl + " dataum " + datum)
      val string: String = util.Arrays.toString(datum.toArray)
      println("datum..." + datum + " , cluster  " + cl + " , centroid  " + centroid + " ,  string " + string + " , calcThresholds " + calcThreshold100 + " & " + calcThreshold99 + " & " + calcThreshold95 + " & " + calcThreshold90 + " & " + calcThreshold85 + " & " + calcThreshold80 + " & " + calcThreshold75)
      //}
      //}
      b

    }
  }

  def anomalies(originalAndData: RDD[(Row, Vector)], sqlc: SQLContext, detector: Vector => Boolean) = {
    println("**** Checking for anomalies in  " + originalAndData.collect().size + " records")
    val anomalies = originalAndData.filter {
      case (orig, datum) => {
        var ndatum = normalizer(datum)
        val string = ndatum.toString
        val b = detector(ndatum)
        if (!b) {
          println("********* NOT AN ANOMALy " + datum + " => " + b)
        }
        b
      }
    }
    println("******** ANOMALIES ************")
    val allkeys: Array[Row] = anomalies.keys.collect
    if (allkeys.isEmpty) {
      println("******** NO ANOMALIES!!! ************NO ANOMALIES!!!")
    }
    if (allkeys.size > 0) {
      println("************* total found " + allkeys.size)
      val dataToStore = collection.mutable.Map[String, Any]()
      allkeys.foreach(e => {
        dataToStore("protocol") = e.get(8)
        dataToStore("filesize") = e.get(17)
        dataToStore("transfetime") = e.get(18)
        dataToStore("insertedtime") = e.get(36)
        println("\n\n***** ANOMALY => (" + e.get(8) + "," + e.get(17) + "," + e.get(18) + ")")
        Utils.saveToES(dataToStore,"anomalies")
        Thread.sleep(1000)
      })
    }

  }
}
