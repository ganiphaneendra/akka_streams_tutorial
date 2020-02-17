package sample.stream_shared_state

import java.time.{Instant, LocalDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

/**
  * Inspired by:
  * https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/splitAfter.html
  *
  */
object SplitAfter extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SplitAfter")
  implicit val executionContext = system.dispatcher

  Source(1 to 100)
    .throttle(1, 100.millis)
    .map(elem => (elem, Instant.now()))
    //allows to compare this element with the next element
    .sliding(2)
    .splitAfter { slidingElements =>
      if (slidingElements.size == 2) {
        val current = slidingElements.head
        val next = slidingElements.tail.head
        val currentBucket = LocalDateTime.ofInstant(current._2, ZoneOffset.UTC).withNano(0)
        val nextBucket = LocalDateTime.ofInstant(next._2, ZoneOffset.UTC).withNano(0)
        println(s"Comparing:  $currentBucket with: $nextBucket - result:${currentBucket != nextBucket}")
        currentBucket != nextBucket
      } else {
        false
      }
    }
    .map(_.head._1)
    .fold(0)((acc, _) => acc + 1) // sum
    .to(Sink.foreach(println))
    .run()

}
