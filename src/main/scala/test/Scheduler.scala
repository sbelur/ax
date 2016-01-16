package test

import java.sql.Timestamp
import java.util.Calendar
import java.util.concurrent.{TimeUnit, Executors}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{functions, Column, Row, SQLContext}
import org.apache.spark.mllib.linalg.Vector
/**
 * Created by sbelur on 10/01/16.
 */
class Scheduler(val sqlc:SQLContext,detector:Vector => Boolean,protocols:Map[String,Int]) extends Serializable{






  def schedule(): Unit ={
    val reader = Executors.newScheduledThreadPool(1)
    var start = java.lang.Long.MAX_VALUE
    var insert:Boolean = true
    var current:Calendar = null
    val executor = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors()+1);
    reader.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = {
        try {
          var meta =Utils.getRecordMetaToScan()
          if(meta.isDefined){
            start = meta.get.getTime
            insert = false
          }
          else {
            meta = Utils.getFirstRecord
            if(meta.isDefined){
              start = meta.get.getTime
              insert = true
            }
            else {
              start = java.lang.Long.MAX_VALUE
            }
          }
          val dataframe_mysql = sqlc.read.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/ax")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("dbtable", "VLTransfers")
            .option("user", "root")
            .option("password", "mysql").load()
          sqlc.udf.register("truncMin", new TruncMinFn())
          current = Calendar.getInstance()
          val testrdd: RDD[Row] = dataframe_mysql.where("truncMin(InsertedTime)").where("Direction = 'send'").rdd
          val parseFunction = AnomalyDetector.buildCategoricalAndLabelFunction(testrdd,protocols)
          val originalAndData = testrdd.map(line => (line, parseFunction(line)))
          println("Checking anomalies in "+originalAndData.collect().size + " records")
          println("***********STARTFOUND "+start + " , "+new Timestamp(start) + " , "+Calendar.getInstance().setTimeInMillis(start))
          AnomalyDetector.anomalies(originalAndData,sqlc,detector)
          if(start != java.lang.Long.MAX_VALUE) {
            println("updating start")
            Utils.updateRecordsMetaToScan(new Timestamp(current.getTimeInMillis), insert)
          }
        }
        catch {
          case e:Throwable => {
            println(e.getMessage)
            e.printStackTrace()
            System.exit(0)
          }
        }
      }
    },0,5,TimeUnit.SECONDS)


    class TruncMinFn extends Function1[java.sql.Timestamp,Boolean] with Serializable{
      override def apply(d: java.sql.Timestamp): Boolean = {
        truncMin(d)
      }
    }


    def truncMin(date:java.sql.Timestamp) = {
      val end = current.getTime.getTime
      //println("*** timerange "+start + " <=> "+end)
      date.getTime >= start && date.getTime < end
    }

  }


}
