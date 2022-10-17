package com.softwaremill.ratelimiter

import cats.effect.std.Queue
import cats.effect.{FiberIO, IO}
import cats.syntax.all._
import com.softwaremill.ratelimiter.RateLimiterQueue.{RateLimiterTask, Run, RunAfter}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{DurationLong, FiniteDuration}

object UsingCats {
  class CatsRateLimiter(
                         queue: Queue[IO, RateLimiterMsg],
                         queueFiber: FiberIO[Unit]
                       ) {
    def runLimited[T](f: IO[T]): IO[T] = {
      for {
        mv <- Queue.bounded[IO, T](1)
        _ <- queue.offer(Schedule(f.flatMap(mv.offer)))
        r <- mv.take
      } yield r
    }

    def stop(): IO[Unit] = {
      queueFiber.cancel
    }
  }

  object CatsRateLimiter extends StrictLogging {
    def create(maxRuns: Int, per: FiniteDuration): IO[CatsRateLimiter] =
      for {
        queue <- Queue.bounded[IO, RateLimiterMsg](1)
        runQueueFiber <- runQueue(
          RateLimiterQueue[IO[Unit]](maxRuns, per.toMillis),
          queue
        )
          .onCancel(IO(logger.info("Stopping rate limiter")))
          .start
      } yield new CatsRateLimiter(queue, runQueueFiber)

    private def runQueue(
                          data: RateLimiterQueue[IO[Unit]],
                          queue: Queue[IO, RateLimiterMsg]
                        ): IO[Unit] = {
      queue
        // (1) take a message from the queue (or wait until one is available)
        .take
        // (2) modify the data structure accordingly
        .map {
          case ScheduledRunQueue => data.notScheduled
          case Schedule(t) => data.enqueue(t)
        }
        // (3) run the rate limiter queue: obtain the rate-limiter-tasks to be run
        .map(_.run(System.currentTimeMillis()))
        .flatMap {
          case (
            tasks: List[RateLimiterTask[IO[Unit]]],
            d: RateLimiterQueue[IO[Unit]]
            ) =>
            tasks
              // (4) convert each rate-limiter-task to a IO
              .map {
                case Run(run) => run
                case RunAfter(millis) =>
                  IO
                    .sleep(millis.millis)
                    .flatMap(_ => queue.offer(ScheduledRunQueue))
              }
              // (5) fork each converted IO so that it runs in the background
              .map(_.start)
              // (6) sequence a list of tasks which spawn background fibers
              // into one big task which, when run, will spawn all of them
              .sequence_
              .map(_ => d)
        }
        // (7) recursive call to handle the next message,
        // using the updated data structure
        .flatMap(d => runQueue(d, queue))
    }
  }

  private sealed trait RateLimiterMsg

  private case object ScheduledRunQueue extends RateLimiterMsg

  private case class Schedule(t: IO[Unit]) extends RateLimiterMsg
}
