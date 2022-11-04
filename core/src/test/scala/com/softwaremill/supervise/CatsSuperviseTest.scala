package com.softwaremill.supervise

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class CatsSuperviseTest
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with SuperviseTestData
    with Eventually {

  object WrapInCatsIO extends Wrap[IO] {
    override def apply[T](t: => T): IO[T] = IO { t }
  }

  it should "forward messages and recover from failures" in {
    val testData = createTestData(WrapInCatsIO)

    val receivedMessages = new ConcurrentLinkedQueue[String]()

    val t = for {
      br <- UsingCats.broadcast(testData.queueConnector)
      _ <- br.inbox.offer(UsingCats.Subscribe(msg => IO(receivedMessages.add(msg))))
    } yield br.cancel

    val cancelBroadcast = t.unsafeRunSync()

    try {
      eventually {
        receivedMessages.asScala.toList.slice(0, 5) should be(List("msg1", "msg2", "msg3", "msg", "msg"))

        testData.connectingWhileClosing.get() should be(false)
        testData.connectingWithoutClosing.get() should be(false)
      }
    } finally {
      cancelBroadcast.unsafeRunSync()

      // get a chance to see that the queue has closed
      Thread.sleep(1000)
    }
  }
}
