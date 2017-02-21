package com.vpals.apps.Ingestion;

import org.apache.logging.log4j.LogManager
import com.jcraft.jsch.JSch
import com.jcraft.jsch.ChannelSftp
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import java.net.URI
import java.util.Properties
import java.io.FileInputStream
import com.vpals.apps.entities.SourceInfoDetails
import com.vpals.apps.entities.TargetInfoDetails
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.sql.ResultSet
import java.sql.ResultSetMetaData

/**
 * @author ${user.name}
 */
object App {
  
  val logger = LogManager.getLogger() 
  var srcDetails = new SourceInfoDetails()
  var targetDetails = new TargetInfoDetails()
  
  def main(args : Array[String]) {
    logger.info("started!")
    readProperties(args(0))
    var jsch=new JSch();
    var session=jsch.getSession(srcDetails.userName, srcDetails.hostName, srcDetails.port);
    logger.info("Getting Session")
    session.setPassword(srcDetails.password);
    session.setConfig("StrictHostKeyChecking", "no");
    logger.info("Session properties set")
    session.connect()
    logger.info("Session connected")
    var sftp = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    sftp.connect()
    logger.info("SFTP channel connected")
    var stream = sftp.get(srcDetails.fileLocation)
    logger.info("Got inputstream from source file")
    var conf = new Configuration()
    conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"))
    conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"))
    logger.info("configured filesystem = " + conf.get("fs.defaultFS"))
    logger.info("Got configuration")
    var fileSystem=FileSystem.get(new URI(conf.get("fs.defaultFS")),conf)
    logger.info("HDFS FileSystem set")
    var outputStream=fileSystem.create(new Path(conf.get("fs.defaultFS") 
        + targetDetails.targetPath + targetDetails.tableName + "." + srcDetails.fileFormat))
    logger.info("Created path in HDFS")
    logger.info("Starting Copy to HDFS")
    IOUtils.copyBytes(stream, outputStream, conf, true);
    logger.info("Writing from inputstream to outputstream")
    stream.close()
    outputStream.close()
    sftp.disconnect()
    session.disconnect()
    logger.info("closing all streams")
    createTable()
  }

  def readProperties(configFilePath : String) {
    var prop = new Properties()
    var input = new FileInputStream(configFilePath)
    prop.load(input)
    
    srcDetails.hostName_(prop.getProperty("hostName"))
    srcDetails.fileLocation_(prop.getProperty("fileLocation"))
    srcDetails.userName_(prop.getProperty("userName"))
    srcDetails.password_(prop.getProperty("password"))
    srcDetails.port_(prop.getProperty("port").toInt)
    srcDetails.fileFormat_(prop.getProperty("fileFormat"))
    targetDetails.targetPath_(prop.getProperty("targetPath"))
    targetDetails.tableName_(prop.getProperty("tableName"))
    targetDetails.tableSchema_(prop.getProperty("tableSchema"))
    targetDetails.sparkUrl_(prop.getProperty("sparkConnUrl"))
  }

  def createTable() {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    var conn: Connection = DriverManager.getConnection(targetDetails.sparkUrl,srcDetails.userName, srcDetails.password)
    var stmt: Statement = conn.createStatement()
    var sql1 = "DROP TABLE IF EXISTS " + targetDetails.tableName
    var pathToRegister = "\"" + targetDetails.targetPath + "\"" 
    var sql2 = "CREATE TABLE " + targetDetails.tableName  + " (" + targetDetails.tableSchema + ") USING com.databricks.spark.csv OPTIONS (path " + pathToRegister + ", header \"false\")"
    //var sql3 = "SELECT * FROM " + targetDetails.tableName
    stmt.executeQuery(sql1)
    stmt.executeQuery(sql2)
    /*var rs: ResultSet = stmt.executeQuery(sql3)
    var metaData: ResultSetMetaData = rs.getMetaData()
    var columnNumber = metaData.getColumnCount()
    while (rs.next()) {
      for (i <- 1 to columnNumber) {
        if (i > 1) System.out.print(",  ");
        var columnValue = rs.getString(i);
        System.out.print(columnValue + " " + metaData.getColumnName(i));
      }
      System.out.println("");
    }*/
  }
}
