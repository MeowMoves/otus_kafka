import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl._
import akka.stream.{Materializer, SystemMaterializer}
import akka.{Done, NotUsed}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "GraphSystem")
    implicit val executionContext: ExecutionContextExecutor = system.executionContext
    implicit val materializer: Materializer = SystemMaterializer(system).materializer

    // Входной поток
    val source: Source[Int, NotUsed] = Source(1 to 5)

    // Метод умножения
    def multiplyFlow(value: Int, name: String): Flow[Int, Int, NotUsed] =
      Flow[Int].map { i =>
        val result = i * value
        println(s"$name: $result")
        result
      }

    // Создаем три потока с выводом
    val flow1: Source[Int, NotUsed] = source.via(multiplyFlow(10, "flow1"))
    val flow2: Source[Int, NotUsed] = source.via(multiplyFlow(2, "flow2"))
    val flow3: Source[Int, NotUsed] = source.via(multiplyFlow(3, "flow3"))

    // Метод суммирования потоков
    def sumStreams(s1: Source[Int, NotUsed],
                   s2: Source[Int, NotUsed],
                   s3: Source[Int, NotUsed]): Source[Int, NotUsed] = {
      s1.zipWith(s2)(_ + _)
        .zipWith(s3)(_ + _)
    }

    val summed: Source[Int, NotUsed] = sumStreams(flow1, flow2, flow3)

    // Выводим сумму элементов
    val resultFuture: Future[Done] = summed.runForeach(sum => println(s"Sum: $sum"))

    resultFuture.onComplete {
      case Success(_) =>
        println("Summed stream completed.")
        system.terminate()
      case Failure(ex) =>
        println(s"Summed stream failed: $ex")
        system.terminate()
    }
  }
}

