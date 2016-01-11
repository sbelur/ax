package test

import java.util.Calendar
import java.util.concurrent.{TimeUnit, Executors}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{functions, Column, Row, SQLContext}

/**
 * Created by sbelur on 10/01/16.
 */
class Scheduler(val sqlc:SQLContext) extends Serializable{






  def schedule(): Unit ={
    val reader = Executors.newScheduledThreadPool(1)
    val executor = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors()+1);
    reader.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = {
        try {
          val f = "insertedtime >= (CONVERT(DATE_FORMAT(now(),'%Y-%m-%d-%H:%i:00'),DATETIME) - interval 1 minute) " +
            "and insertedtime < (CONVERT(DATE_FORMAT(now(),'%Y-%m-%d-%H:%i:00'),DATETIME))"
          val dataframe_mysql = sqlc.read.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/ax")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("dbtable", "testdata")
            .option("user", "root")
            .option("password", "mysql").load()
          println("*********** " + f)
          sqlc.udf.register("truncMin", new TruncMinFn())
          //val col:Column = dataframe_mysql.col("insertedtime")
          //functions.udf
          val testrdd: RDD[Row] = dataframe_mysql.where("truncMin(insertedtime)").rdd
          println("SIZEDB" + testrdd.collect().size)
          testrdd.collect().foreach(x => println("RecordFound " + x))
          //val r = dataframe_mysql.sqlContext.sql(" select * from testdata where truncMin(insertedtime);").rdd
          //println("SIZEDB2" + r.collect().size)

          //r.collect().foreach(x => println("Record2 " + x))
        }
        catch {
          case e:Throwable => {
            println(e.getMessage)
            e.printStackTrace()
            System.exit(0)
          }
        }
      }
    },0,1,TimeUnit.SECONDS)


    class TruncMinFn extends Function1[java.sql.Timestamp,Boolean] with Serializable{
      override def apply(d: java.sql.Timestamp): Boolean= {
        truncMin(d)
      }
    }


    def truncMin(date:java.sql.Timestamp) = {
      val c = Calendar.getInstance();
      c.set(Calendar.SECOND,0)
      c.set(Calendar.MILLISECOND,0)
      val end = c.getTime.getTime
      c.set(Calendar.MINUTE,c.get(Calendar.MINUTE)-1)
      val start = c.getTime.getTime
      date.getTime >= start && date.getTime < end
    }

  }


}
