package com.softwaremill.ratelimiter

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.concurrent.IntegrationPatience

class CatsRateLimiterTest extends RateLimiterTest with IntegrationPatience {

  doTest(
    "cats",
    maxRuns =>
      per =>
        new RateLimiter {
          private val rl = UsingCats.CatsRateLimiter.create(maxRuns, per).unsafeRunSync()
          override def runLimited(f: => Unit): Unit = rl.runLimited(IO { f }).unsafeRunAndForget()
          override def stop(): Unit = rl.stop().unsafeRunSync()
    }
  )
}
