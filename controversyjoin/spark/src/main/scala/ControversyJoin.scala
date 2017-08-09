import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object ControversyJoin
{
    def main(args: Array[String])
    {
        val tasks = args(0).toInt

        def findControversy(ratings: Iterable[Double]) : Double =
        {
            val iterator = ratings.iterator
            var positive = 0.0
            var negative = 0.0
            var ratio = 0.0

            while(iterator.hasNext)
            {
                if (iterator.next>=2.5)
                {
                    positive = positive + 1
                }
                else
                {
                    negative = negative + 1
                }
            }
            if (positive>=negative)
            {
                ratio = positive / negative
            }
            else
            {
                ratio = negative / positive
            }

            ratio = 1 - ratio

            if (ratio<=0)
            {
                return ratio*(-1)
            }
            else
            {
                return ratio
            }
        }

        val sc = new SparkContext(new SparkConf().setAppName("Controversy Join"))

        // Load ratings file from HDFS
        val ratingsFile = sc.textFile("hdfs://moonshot-ha-nameservice/data/movie-ratings/ratings.dat")

        // 1 000 000 ratings
        // val ratingsFile = sc.textFile("hdfs://moonshot-ha-nameservice/user/dae30/input/mil.txt")

        // 100 000 ratings
        // val ratingsFile = sc.textFile("hdfs://moonshot-ha-nameservice/user/dae30/input/100k.txt")

        // Load movies file from HDFS
        val moviesFile = sc.textFile("hdfs://moonshot-ha-nameservice/data/movie-ratings/movies.dat")

        // Create map from movies.dat: key = movieid, value = movietitle
        val IDTitle = moviesFile.map(x => x.split("::")).map(x => (x(0),x(1)))

        // Create map from ratings.dat: key = movieid, value = movierating
        val IDRating = ratingsFile.map(x => x.split("::")).map(x => (x(1),x(2).toDouble))

        // Group together ratings for the same movie, creates map of the structure (String,Iterable[Double])
        val ratingGroup = IDRating.groupByKey(tasks)

        // Create map where key = movieid, value = controversyscore
        val controversyRatings = ratingGroup.map{case(k,v) => (k,findControversy(v))}

        // Join controversy score with movie title
        val join = IDTitle.join(controversyRatings,tasks)

        // Check that it worked
        // val sample = join.takeSample(false,5)
        // sample.foreach(println)

        val inmem = join.persist()
        inmem.saveAsTextFile("hdfs://moonshot-ha-nameservice/user/dae30/spark")
    }
}
