package test

import java.sql.{Timestamp, DriverManager}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import org.elasticsearch.node.NodeBuilder._
import scala.collection.JavaConversions._
/**
 * Created by sbelur on 15/01/16.
 */
object Utils {


  Class.forName("com.mysql.jdbc.Driver")
  var con = DriverManager.getConnection("jdbc:mysql://localhost:3306/ax", "root", "mysql")
  var stmt = con.createStatement()


  def updateRecordsMetaToScan(dt:Timestamp,insert:Boolean): Unit ={
    if(insert)
      stmt.executeUpdate("insert into meta values('"+dt+"')")
    else
      stmt.executeUpdate("update meta set insertedtime='"+dt+"'")
  }

  def getRecordMetaToScan():Option[Timestamp] = {
    var rs = stmt.executeQuery("select insertedtime from meta")
    if(rs != null && rs.next()){
        val dt = rs.getTimestamp("insertedtime")
        Some(dt)
    }
    else {
        None
    }
  }

  def getFirstRecord() = {
    val rs = stmt.executeQuery("select min(InsertedTime) as InsertedTime from VLTransfers")
    if(rs.next()){
      val dt = rs.getTimestamp("InsertedTime")
      Some(dt)
    }
    else {
      None
    }
  }


  def client():Option[TransportClient] = {
    if(esclient.isDefined)
      esclient
    else {
      val settingsbuilder = ImmutableSettings.settingsBuilder()
        .put("http.port", 9600)
        .put("transport.tcp.port", 9700)
        .put("cluster.name", "ax")

      val transportClient = new TransportClient(settingsbuilder).addTransportAddress(
        new InetSocketTransportAddress("localhost", 9700))
      println("transportClient.connectedNodes()" + transportClient.connectedNodes())
      esclient = Some(transportClient)
      esclient
    }
  }


  def saveToES(anomalies:collection.mutable.Map[String,Any],sc:SparkContext): Unit ={

    val esconnector = client()
    if(anomalies == null || anomalies.isEmpty || !esconnector.isDefined){
      return
    }
    val tsclient = esconnector.get


    val jmap:java.util.Map[java.lang.String, java.lang.Object] = mapAsJavaMap(anomalies).asInstanceOf[java.util.Map[java.lang.String, java.lang.Object]]
    println(jmap)
    val resp  = tsclient.prepareIndex("ax","anomalies").setSource(jmap).execute().actionGet()
    println(resp.isCreated + ","+resp.getId + ","+resp.getIndex+","+resp.getType)
  }

  var esclient:Option[TransportClient] = None
}
