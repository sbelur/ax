package ax.poc

import java.io._
import java.text.SimpleDateFormat
import java.util.{UUID, Calendar}
import java.util.concurrent.ThreadLocalRandom

import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sbelur on 17/01/16.
 */
object Aggregator {

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
    rdd.cache()
    val mapped: RDD[((Long, String, Long), Double)] = rdd.map {
      r: Row => {
        val protocol = r.get(8).toString
        val filesize = r.get(17).toString.toLong
        val transfetime = r.get(18).toString.toDouble
        val insertedtime = r.get(36)
        val sdf = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")
        val dt = sdf.parse(insertedtime.toString)
        val cal = Calendar.getInstance()
        cal.setTime(dt)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        //println(cal.getTimeInMillis + "," + protocol + "," + filesize + "," + transfetime)
        (cal.getTimeInMillis, protocol, filesize) -> transfetime
      }
    }

    val temp: RDD[((Long, String, Long), Iterable[Double])] = mapped.groupByKey()
    val per: RDD[((Long, String, Long), Array[Double])] = temp.map {
      e => {
        val key = e._1
        val values: Iterable[Double] = e._2
        val size = values.size
        //val r:RDD[Double] = sc.parallelize(values.toSeq)
        val indexed: Seq[(Int, Double)] = values.toSeq.sortWith((x: Double, y: Double) => x < y).zipWithIndex.map(x => x.swap)
        val percentiles: Array[Double] = Array(0.5, 0.9, 0.99).map {
          percentile =>
            val id = (size * percentile).asInstanceOf[Long];
            val d: Option[(Int, Double)] = indexed.find {
              t => t._1 == id
            }
            if (d.isDefined) {
              val db: Double = d.get._2
              db
            }
            else
              -1D
        }
        (key, percentiles)
      }
    }
    val dataToStore = collection.mutable.Map[String, Any]()
    per.foreach {
      e => {
        val key = e._1
        val stats = e._2
        val insertedtime = key._1
        val protocol = key._2
        val fs = key._3
        val pt50 = stats(0)
        val pt90 = stats(1)
        val pt99 = stats(2)
        dataToStore("insertedtime")  = insertedtime
        dataToStore("protocol")  = protocol
        dataToStore("filesize")  = fs
        dataToStore("percentile50")  = pt50
        dataToStore("percentile90")  = pt90
        dataToStore("percentile99")  = pt99
        Utils.saveToES(dataToStore,"transferstats")
      }
    }
    dataToStore.clear()
    val topprotocols = rdd.map {
      r: Row => {
        val protocol = r.get(8).toString
        val filesize = r.get(17).toString.toLong
        protocol->filesize
      }
    }
    val protocolgrp = topprotocols.groupByKey()
    protocolgrp.foreach {
      e => {
        dataToStore("protocol") = e._1
        dataToStore("totaltransfer") = e._2.sum
        dataToStore("mintransfer") = e._2.min
        dataToStore("maxtransfer") = e._2.max
        Utils.saveToES(dataToStore,"protocolview")
      }
    }

    rdd.unpersist()
  }


}
