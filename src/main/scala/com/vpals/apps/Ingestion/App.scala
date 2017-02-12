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
  }
}
