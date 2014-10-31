import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.TraversableOnce.MonadOps

val input = """1: if you prick us do we not bleed
2: if you tickle us do we not laugh
3: if you poison us do we not die and
4: if you wrong us shall we not revenge"""

val sc = new SparkContext(
  new SparkConf()
    .setMaster("local")
    .setAppName("bigram analysis")
)

val parsedInput = input.split("\n")

val lines = sc.parallelize(parsedInput)

//val words = lines.flatMap(a => a.split(" "))

//RDD[T]
//f: T => M[U]
//RDD[U, T]

//TODO f can be T => M[U], in which case we want to return RDD[(U, T)] instead of RDD[(T, T)]
def explode[T, M[_] <: TraversableOnce[_]](rdd: RDD[T], f: T => M[T]): RDD[(T, T)] = {
  val map = rdd.map(element => (f(element), element))
//    map.flatMap(a => (a._1.map(b => (b, a._2))))

//  [M[U] <: TraversableOnce[U], T] => [U, T]
  map.flatMap(a => a._1.map(b => (b.asInstanceOf[T], a._2)))
}


def explode[T, U, M[_] <: TraversableOnce[_]](rdd: RDD[T], f: T => M[U]): RDD[(U, T)] = {
  val map = rdd.map(element => (f(element), element))

//  [M[U] <: TraversableOnce[U], T] => [U, T]
  map.flatMap(a => a._1.map(b => (b.asInstanceOf[U], a._2)))
}

val mapped = lines.map(a => (a.split(" "), a))
//TODO see how we can abstract this into explode function
val wordLine = mapped.flatMap(a1 => a1._1.map(b1 => (b1, a1._2)))