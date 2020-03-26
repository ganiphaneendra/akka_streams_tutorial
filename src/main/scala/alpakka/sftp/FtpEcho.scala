package alpakka.sftp

import java.io.{File, PrintWriter}
import java.net.InetAddress
import java.nio.file.Files

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.scaladsl.Ftp
import akka.stream.alpakka.ftp.{FtpCredentials, FtpFile, FtpSettings}
import akka.stream.scaladsl.{FileIO, RunnableGraph, Sink, Source}
import akka.stream.{IOResult, ThrottleMode}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.commons.net.PrintCommandListener
import org.apache.commons.net.ftp.FTPClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}


/**
  * Implement an upload/download echo flow for the happy path,
  * trying to use alpakka ftp features for everything
  *
  * Prerequisite:
  *  - Start the docker FTP server from: /docker/docker-compose.yml
  *    eg by cmd line: docker-compose up -d pure_ftpd
  *
  * Issues:
  *  - config of home dir vs in SFTP
  *  - Method [[FtpEcho.createFolders]] does not work?!
  *    - Workaround: None?!
  *    - Workaround: [[FtpEcho.createFoldersNative]]
  *  - Confusion about active/passive and binary
  *  - Compare to original linked example on doc page
  *
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/ftp.html
  * https://stackoverflow.com/questions/1699145/what-is-the-difference-between-active-and-passive-ftp
  *
  */
object FtpEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("FtpEcho")
  implicit val executionContext = system.dispatcher

  //we need a sub folder due to permissions set on on the atmoz_sftp docker image
  val rootDir = "echo"
  val processedDir = "processed"

  val hostname = "127.0.0.1"
  val port = 21000
  val username = "echouser"
  val password = "password"
  val credentials = FtpCredentials.create(username, password)

  val ftpSettings = FtpSettings
    .create(InetAddress.getByName(hostname))
    .withPort(port)
    .withCredentials(credentials)
    .withBinary(true)
    .withPassiveMode(true)
    // only useful for debugging
    .withConfigureConnection((ftpClient: FTPClient) => {
      ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true))
    })


  removeAll().onComplete {
    case Success(_) =>
      logger.info("Successfully cleaned...")
      createFolders()
      uploadClient()
      downloadClient()
    case Failure(e) =>
      logger.info(s"Failure while cleaning: ${e.getMessage}. About to terminate...")
      system.terminate()
  }


  def uploadClient() = {
    logger.info("Starting upload...")

    Source(1 to 100)
      .throttle(10, 1.second, 10, ThrottleMode.shaping)
      .wireTap(number => logger.info(s"Upload file with TRACE_ID: $number"))
      .mapAsync(parallelism = 10) {
        each =>
          val result = Source
            //Generate file content in memory to avoid the overhead of generating n test files on the filesystem
            .single(ByteString(s"this is the file contents for: $each"))
            .runWith(uploadToPath( s"/file_$each.txt"))
          result.onComplete(res => logger.info(s"Client uploaded file with TRACE_ID: $each. Result: $res"))
          result
      }
      .runWith(Sink.ignore)
  }


  def downloadClient(): Unit = {
    Thread.sleep(5000) //wait to get some files
    logger.info("Starting download run...")

    processAndMove(s"/$rootDir", (file: FtpFile) => s"/$rootDir/$processedDir/${file.name}", ftpSettings).run()
  }

  def processAndMoveVerbose(): Unit = {
    val fetchedFiles: Future[immutable.Seq[(String, IOResult)]] =
      listFiles(s"/$rootDir")
        .take(50) //Try to batch
        .filter(ftpFile => ftpFile.isFile)
        .mapAsyncUnordered(parallelism = 5)(ftpFile => fetchAndMoveVerbose(ftpFile))
        .runWith(Sink.seq)

    fetchedFiles.onComplete {
      case Success(results) =>
        logger.info(s"Successfully fetched: ${results.size} files for this run. About to start next run...")
        downloadClient()
      case Failure(exception) =>
        logger.info(s"The stream failed with: ${ExceptionUtils.getRootCause(exception)}")
        system.terminate()
    }
  }

  private def fetchAndMoveVerbose(ftpFile: FtpFile) = {

    val localFile = File.createTempFile(ftpFile.name, ".tmp.client")
    //localFile.deleteOnExit()
    val localPath = localFile.toPath
    logger.info(s"About to fetch file: $ftpFile to local path: $localPath")

    val fetchFile: Future[IOResult] = retrieveFromPath(ftpFile.path)
      .runWith(FileIO.toPath(localPath))
    fetchFile.map { ioResult =>
      // TODO Check if it works after 2.0.0-RC1
      Ftp.move(_ => s"$rootDir/$processedDir/", ftpSettings)

      (ftpFile.path, ioResult)
    }
  }



  //TODO This hangs at the 1st element
  def processAndMove(sourcePath: String,
                     destinationPath: FtpFile => String,
                     ftpSettings: FtpSettings): RunnableGraph[NotUsed] =
    Ftp
      .ls(sourcePath, ftpSettings)
      .flatMapConcat(ftpFile => Ftp.fromPath(ftpFile.path, ftpSettings).map((_, ftpFile)))
      .wireTap(each => logger.info(s"About to process file: ${each._2.name}"))
      .alsoTo(FileIO.toPath(Files.createTempFile("downloaded", "tmp")).contramap(_._1))
      .to(Ftp.move(destinationPath, ftpSettings).contramap(_._2))


  private def mkdir(basePath: String, directoryName: String): Source[Done, NotUsed] =
    Ftp.mkdir("/home/echouser/" + basePath, directoryName, ftpSettings)

  private def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Ftp.ls(basePath, ftpSettings)

  private def uploadToPath(path: String) = {
    Ftp.toPath(path, ftpSettings)
  }

  private def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Ftp.fromPath(path, ftpSettings)

  private def createFolders() = {
    mkdir("/", s"/$rootDir")
    mkdir("/", s"/$rootDir/$processedDir")
  }

  private def removeAll() = {
    val source = listFiles("/")
    val sink = Ftp.remove(ftpSettings)
    source.runWith(sink)
  }
}