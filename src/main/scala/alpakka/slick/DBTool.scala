package alpakka.slick

import java.nio.file.Paths

import akka.Done
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl._
import akka.util.ByteString
import slick.jdbc.GetResult

import scala.concurrent.Future

object DBTool {
  implicit val system = ActorSystem("DBTool")
  implicit val executionContext = system.dispatcher
  implicit val session = SlickSession.forConfig("slick-mysql")

  def main(args: Array[String]): Unit = {

    def reusableLineSink(filename: String): Sink[String, Future[IOResult]] =
      Flow[String]
        .map(s => ByteString(s + "\n"))
        //Keep.right means: we want to retain what the FileIO.toPath sink has to offer
        .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)

    system.registerOnTermination(() => session.close())

    // The example domain
    case class Profile(hp_id: String, primary_organisation_id: String, org_representative: Int)
    case class OutputChannel(id: Int, user_id: String, user_type: String)


    implicit val getProfileResult = GetResult(r => Profile(r.nextString(), r.nextString, r.nextInt()))
    implicit val getOutputChannelResult = GetResult(r => OutputChannel(r.nextInt(), r.nextString, r.nextString()))

    // This import enables the use of the Slick sql"...",
    // sqlu"...", and sqlt"..." String interpolators.
    // See also: "http://slick.lightbend.com/doc/3.2.1/sql.html#string-interpolation"
    import session.profile.api._

//    val profiles: List[Profile] = sql"SELECT hp_id, primary_organisation_id, org_representative from hp_profile".as[Profile]
//    val outputChannels: List[OutputChannel] =
//
//    val innerJoin = for {
//      (c, s) <- profiles join outputChannels on (_.supID === _.id)
//    } yield (c.name, s.name)


    // Stream the results of a query
    val done: Future[Done] =
      Slick
        .source(sql"SELECT hp_id, primary_organisation_id, org_representative from hp_profile".as[Profile])
        .wireTap(each => println(each))
        .runWith(Sink.ignore)

    done.onComplete(_ => system.terminate())
  }
}
