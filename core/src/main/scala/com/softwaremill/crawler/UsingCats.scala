package com.softwaremill.crawler

import cats.effect.std.Queue
import cats.effect.unsafe.implicits.global
import cats.effect.{FiberIO, IO}
import com.typesafe.scalalogging.StrictLogging
import cats.implicits._

object UsingMonix extends StrictLogging {

  def crawl(crawlUrl: Url, http: Http[IO], parseLinks: String => List[Url]): IO[Map[Host, Int]] = {

    def crawler(crawlerQueue: MQueue[CrawlerMessage], data: CrawlerData): IO[Map[Host, Int]] = {
      def handleMessage(msg: CrawlerMessage, data: CrawlerData): IO[CrawlerData] = msg match {
        case Start(url) =>
          crawlUrl(data, url)

        case CrawlResult(url, links) =>
          val data2 = data.copy(inProgress = data.inProgress - url)

          links.foldM(data2) {
            case (d, link) =>
              val d2 = d.copy(referenceCount = d.referenceCount.updated(link.host, d.referenceCount.getOrElse(link.host, 0) + 1))
              crawlUrl(d2, link)
          }
      }

      def crawlUrl(data: CrawlerData, url: Url): IO[CrawlerData] = {
        if (!data.visitedLinks.contains(url)) {
          workerFor(data, url.host).flatMap {
            case (data2, workerQueue) =>
              workerQueue.offer(url).map { _ =>
                data2.copy(
                  visitedLinks = data.visitedLinks + url,
                  inProgress = data.inProgress + url
                )
              }
          }
        } else IO.pure(data)
      }

      def workerFor(data: CrawlerData, host: Host): IO[(CrawlerData, MQueue[Url])] = {
        data.workers.get(host) match {
          case None =>
            val workerQueue = MQueue.make[Url]
            worker(workerQueue, crawlerQueue).map { workerFiber =>
              (data.copy(workers = data.workers + (host -> WorkerData(workerQueue, workerFiber))), workerQueue)
            }
          case Some(wd) => IO.pure((data, wd.queue))
        }
      }

      crawlerQueue.take.flatMap { msg =>
        handleMessage(msg, data).flatMap { data2 =>
          if (data2.inProgress.isEmpty) {
            data2.workers.values.map(_.fiber.cancel).toList.sequence_.map(_ => data2.referenceCount)
          } else {
            crawler(crawlerQueue, data2)
          }
        }
      }
    }

    def worker(workerQueue: MQueue[Url], crawlerQueue: MQueue[CrawlerMessage]): IO[FiberIO[Unit]] = {
      def handleUrl(url: Url): IO[Unit] = {
        http
          .get(url)
          .attempt
          .map {
            case Left(t) =>
              logger.error(s"Cannot get contents of $url", t)
              List.empty[Url]
            case Right(b) => parseLinks(b)
          }
          .flatMap(r => crawlerQueue.offer(CrawlResult(url, r)))
      }

      workerQueue.take
        .flatMap(handleUrl)
        .iterateUntil(_ => false)
        .start
    }

    val crawlerQueue = MQueue.make[CrawlerMessage]
    for {
      _ <- crawlerQueue.offer(Start(crawlUrl))
      r <- crawler(crawlerQueue, CrawlerData(Map(), Set(), Set(), Map()))
    } yield r
  }

  case class WorkerData(queue: MQueue[Url], fiber: FiberIO[Unit])
  case class CrawlerData(referenceCount: Map[Host, Int], visitedLinks: Set[Url], inProgress: Set[Url], workers: Map[Host, WorkerData])

  sealed trait CrawlerMessage

  /**
    * Start the crawling process for the given URL. Should be sent only once.
    */
  case class Start(url: Url) extends CrawlerMessage
  case class CrawlResult(url: Url, links: List[Url]) extends CrawlerMessage

  //

  class MQueue[T](q: Queue[IO, T]) {
    def take: IO[T] = {
      IO.defer(q.take)
    }
    def offer(t: T): IO[Unit] = {
      IO.defer(q.offer(t))
    }
  }
  object MQueue {
    def make[T]: MQueue[T] = new MQueue(Queue.unbounded[IO, T].unsafeRunSync())
  }
}
