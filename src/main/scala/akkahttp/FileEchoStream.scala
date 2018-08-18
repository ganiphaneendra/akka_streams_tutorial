package akkahttp

import java.io.File
import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Multipart.FormData
import akka.http.scaladsl.model.{HttpRequest, MediaTypes, RequestEntity, _}
import akka.http.scaladsl.server.Directives.{complete, logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import spray.json.DefaultJsonProtocol

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}



trait JsonProtocol2 extends DefaultJsonProtocol with SprayJsonSupport {

  final case class FileHandle(fileName: String, absolutePath: String, length: Long = 0)

  implicit def fileInfoFormat = jsonFormat3(FileHandle.apply)
}


/**
  * Differences to FileEcho:
  *  * Uses a Stream for upload instead of a Single requests as in FileEcho
  *  * Uses the host-level API with a queue for download
  *  * Retries via config param max-retries set to: 1
  *
  * Doc:
  * https://doc.akka.io/docs/akka-http/current/client-side/host-level.html?language=scala#retrying-a-request
  *
  *
  * TODOs
  * Cleanup on pool shutdown
  *
  */
object FileEchoStream extends App with JsonProtocol2 {
  implicit val system = ActorSystem("FileEchoStream")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  val resourceFileName = "testfile.jpg"
  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  roundtripClient(address, port)

  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

        storeUploadedFile("binary", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            println(s"Server stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
            complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
        }
      } ~
        path("download") {
          get {
            entity(as[FileHandle]) { fileHandle: FileHandle =>
              println(s"Server received download request for: ${fileHandle.fileName}")
              getFromFile(new File(fileHandle.absolutePath), MediaTypes.`application/octet-stream`)
            }
          }
        }
    }

    val bindingFuture = Http().bindAndHandle(routes, address, port)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  def filesToUpload(): Source[FileHandle, NotUsed] =
  // This could be a lazy/infinite stream. For this example we have a finite one:
    Source(List(
      FileHandle("1.jpg", Paths.get("./src/main/resources/testfile.jpg").toString),
      FileHandle("2.jpg", Paths.get("./src/main/resources/testfile.jpg").toString),
      FileHandle("3.jpg", Paths.get("./src/main/resources/testfile.jpg").toString),
      FileHandle("4.jpg", Paths.get("./src/main/resources/testfile.jpg").toString),
      FileHandle("5.jpg", Paths.get("./src/main/resources/testfile.jpg").toString)
    ))


  def roundtripClient(address: String, port: Int) = {

    val poolClientFlowUpload =
      Http().cachedHostConnectionPool[FileHandle](address, port)

    def createUploadRequest(fileToUpload: FileHandle): Future[(HttpRequest, FileHandle)] = {
      val bodyPart =
        FormData.BodyPart.fromPath("binary", ContentTypes.`application/octet-stream`, Paths.get(fileToUpload.absolutePath))

      val body = FormData(bodyPart) // only one file per upload
      Marshal(body).to[RequestEntity].map { entity => // use marshalling to create multipart/formdata entity
        val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/upload"))
        HttpRequest(method = HttpMethods.POST, uri = target, entity = entity) -> fileToUpload
      }
    }


    def createDownloadRequest(fileToDownload: FileHandle): Future[HttpRequest] = {
      Marshal(fileToDownload).to[RequestEntity].map { entity: MessageEntity =>
        val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/download"))
        HttpRequest(HttpMethods.GET, uri = target, entity = entity)
      }
    }

    def createDownloadRequestNoFuture(fileToDownload: FileHandle) ={
      val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/download"))
      val entityFuture = Marshal(fileToDownload).to[MessageEntity]
      val entity = Await.result(entityFuture, 1.second) //TODO Do it without blocking
      HttpRequest(HttpMethods.GET, target, entity = entity)
    }



    filesToUpload()
      // The stream will "pull out" these requests when capacity is available.
      // When that is the case we create one request concurrently
      // (the pipeline will still allow multiple requests running at the same time)
      .mapAsync(1)(createUploadRequest)
      // then dispatch the request to the connection pool
      .via(poolClientFlowUpload)
      // report each response
      // Note: responses will NOT come in in the same order as requests. The requests will be run on one of the
      // multiple pooled connections and may thus "overtake" each other!
      .runForeach {
      case (Success(response: HttpResponse), fileToUpload) =>
        println(s"Upload for file: $fileToUpload was successful: ${response.status}")

        val fileHandleFuture = Unmarshal(response).to[FileHandle]
        val fileHandle = Await.result(fileHandleFuture, 1.second)  //TODO Do it without blocking
        response.discardEntityBytes()


        //TODO sep download method?

        val QueueSize = 1
        val poolClientFlowDownload = Http().cachedHostConnectionPool[Promise[HttpResponse]](address, port)
        val queue =
          Source.queue[(HttpRequest, Promise[HttpResponse])](QueueSize, OverflowStrategy.backpressure)
            .via(poolClientFlowDownload)
            .toMat(Sink.foreach({
              case (Success(resp), p) => p.success(resp)
              case (Failure(e), p)    => p.failure(e)
            }))(Keep.left)
            .run()

        def queueRequest(request: HttpRequest): Future[HttpResponse] = {
          val responsePromise = Promise[HttpResponse]()
          queue.offer(request -> responsePromise).flatMap {
            case QueueOfferResult.Enqueued    => responsePromise.future
            case QueueOfferResult.Dropped     => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
            case QueueOfferResult.Failure(ex) => Future.failed(ex)
            case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
          }
        }

        val responseFuture: Future[HttpResponse] = queueRequest(createDownloadRequestNoFuture(fileHandle))
        responseFuture.onComplete {
          case Success(resp) => {
            val localFile = File.createTempFile("downloadLocal", ".tmp.client")
            val result = resp.entity.dataBytes.runWith(FileIO.toPath(Paths.get(localFile.getAbsolutePath)))
            result.map {
              ioresult =>
                println(s"Download client for file: $resp finished downloading: ${ioresult.count} bytes!")
            }
          }
          case Failure(exception) => println(s"Boom $exception")
        }

      case (Failure(ex), fileToUpload) =>
        println(s"Uploading file $fileToUpload failed with $ex")
    }
  }
}