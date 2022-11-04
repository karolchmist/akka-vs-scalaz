package com.softwaremill.supervise

import cats.effect.std.Queue
import cats.effect.{IO, std}
import com.typesafe.scalalogging.StrictLogging
import cats.implicits._

object UsingCats extends StrictLogging {
 type MVar[A] = std.Queue[IO, A]
  sealed trait BroadcastMessage
  case class Subscribe(consumer: String => IO[Unit]) extends BroadcastMessage
  case class Received(msg: String) extends BroadcastMessage

  case class BroadcastResult(inbox: MVar[BroadcastMessage], cancel: IO[Unit])

  def broadcast(connector: QueueConnector[IO]): IO[BroadcastResult] = {
    def processMessages(inbox: MVar[BroadcastMessage], consumers: Set[String => IO[Unit]]): IO[Unit] =
      inbox.take
        .flatMap {
          case Subscribe(consumer) => processMessages(inbox, consumers + consumer)
          case Received(msg) =>
            consumers
              .map(consumer => consumer(msg).start)
              .toList
              .sequence_
              .flatMap(_ => processMessages(inbox, consumers))
        }

    def consumeForever(inbox: MVar[BroadcastMessage]): IO[Unit] =
      consume(connector, inbox).attempt
        .map {
          case Left(e) =>
            logger.info("[broadcast] exception in queue consumer, restarting", e)
          case Right(()) =>
            logger.info("[broadcast] queue consumer completed, restarting")
        }
        .foreverM

    for {
      inbox <- std.Queue.unbounded[IO, BroadcastMessage]
      f1 <- consumeForever(inbox).start
      f2 <- processMessages(inbox, Set()).start
    } yield BroadcastResult(inbox, f1.cancel *> f2.cancel)
  }

  def consume(connector: QueueConnector[IO], inbox: MVar[BroadcastMessage]): IO[Unit] = {
    val connect: IO[Queue[IO]] = IO(logger.info("[queue-start] connecting"))
      .flatMap(_ => connector.connect)
      .map { q =>
        logger.info("[queue-start] connected")
        q
      }

    def consumeQueue(queue: Queue[IO]): IO[Unit] =
      IO
        .apply(logger.info("[queue] receiving message"))
        .flatMap(_ => queue.read())
        .flatMap(msg => inbox.offer(Received(msg)))
        .foreverM

    def releaseQueue(queue: Queue[IO]): IO[Unit] =
      IO(logger.info("[queue-stop] closing"))
        .flatMap(_ => queue.close())
        .map(_ => logger.info("[queue-stop] closed"))

    connect.bracket(consumeQueue)(releaseQueue)
  }
}
