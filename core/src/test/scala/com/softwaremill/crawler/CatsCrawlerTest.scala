package com.softwaremill.crawler

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class CatsCrawlerTest extends FlatSpec with Matchers with CrawlerTestData with ScalaFutures with IntegrationPatience {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(Span(60, Seconds)),
      interval = scaled(Span(150, Millis))
    )

  for (testData: TestDataSet <- testDataSets) {
    it should s"crawl a test data set ${testData.name}" in {
      import testData._

      val t = timed {
        UsingMonix
          .crawl(startingUrl, url => IO(http(url)), parseLinks)
          .unsafeToFuture()
          .futureValue should be(expectedCounts)
      }

      shouldTakeMillisMin.foreach(m => t should be >= (m))
      shouldTakeMillisMax.foreach(m => t should be <= (m))
    }
  }
}
