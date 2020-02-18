package sample.stream_shared_state

import java.time.Instant

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.util.Random

case class Record(eventTime: Instant, id: Long)

/**
  * Goal:
  * Tumbling window based on event time, with watermark
  * TODO
  *  - What is the cheapest way (using operators) to achieve this?
  *
  */
object TumblingWindow extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("TumblingWindow")
  implicit val executionContext = system.dispatcher

  val random = new Random()
  val watermark = 6 //test with values 1/6/8/10/100

  val tickSource: Source[Instant, Cancellable] = Source
    .tick(0.second, 1.second, NotUsed)
    .map { _ =>
      val now = System.currentTimeMillis()
      val delay = random.nextInt(8)
      Instant.ofEpochMilli(now - delay * 1000L)
    }

  tickSource.zipWithIndex
    .map(each => Record(each._1, each._2))
    //.groupedWithin(10, 5.seconds)
    //TODO This is a tumbling window based on processing time...
    .groupedWeightedWithin(1L, 5.seconds)({
      //add heavy weight, if record.eventTime is within watermark
      case Record(eventTime, _) if (eventTime.isBefore(Instant.ofEpochMilli(System.currentTimeMillis() - watermark * 1000L))) => 1L
      //else add low weight
      case _ => 0
    })

    .wireTap(each => println(s"each: $each"))
    .runWith(Sink.ignore)
}
