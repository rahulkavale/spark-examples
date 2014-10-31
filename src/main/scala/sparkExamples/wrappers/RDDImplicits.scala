package sparkExamples.wrappers

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.TraversableLike
import scala.reflect.ClassTag

object RDDImplicits {
  implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {
    def countEachElement = {
      rdd.map(bg => (bg, 1)).reduceByKey((c1, c2) => c1 + c2)
    }

    def countWhere(f: T => Boolean): Long = {
      rdd.filter(f).count()
    }

    def sortByDesc[K : Ordering: ClassTag](f: T => K): RDD[T] = {
      val isAscending = false
      rdd.sortBy(f, isAscending)
    }

    def explode[U, M[A] <: TraversableLike](f: T => M[U]): RDD[(U, T)] = {
      val map = rdd.map(element => (f(element), element))
      map.flatMap(a => a._1.map(b => (b, a._2)))
    }
  }

}
