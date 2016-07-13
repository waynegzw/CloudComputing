import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhangwei on 4/17/16.
  */
  object PageRank {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("word count").setMaster("master")
      val sc = new SparkContext(conf)
      val lines = sc.textFile("s3n://AKIAJ6YBUBK2XPA6OOAA:6AMo1KCQOuZHNk2tSq2pT0yoKoGVbGKjHrTvGTMW@cmucc-datasets/TwitterGraph.txt")
      //val lines = sc.textFile("/testinput/test.txt")

      val links = lines.map(line => {
        val ids = line.split("\\s+")
        (ids(0), ids(1))
        }).distinct().groupByKey().cache()

      val v = scala.collection.mutable.ArrayBuffer.empty ++ links.keys.collect()
      val danglingV = scala.collection.mutable.ArrayBuffer[String]()
      for {i <- links.values.collect()
       n <- i if (!v.contains(n))
       } {
        v += n
        danglingV += n
      }
      val linkList = links ++ sc.parallelize(for (i <- danglingV) yield (i, List.empty)).cache()
      val verticesNum = linkList.count()
    // RDD of (URL, rank) pairs
    var ranks = linkList.mapValues(v => 1.0)

    for (i <- 1 to 10) {
      val accum = sc.accumulator(0.0)
      // Build an RDD of (targetURL, float) pairs
      // with the contributions sent by each page
      val contribs = linkList.join(ranks).flatMap {
        case (url, (links, rank)) => {
          var size = links.size
          if (size == 0) {
            accum += rank
            List()
            } else {
              links.map(dest => (dest, rank / size))
            }
          }
        }
        contribs.count()
        val danglingSum = accum.value
      // Sum contributions by URL and get new ranks
      ranks = contribs.reduceByKey((x, y) => x + y).mapValues(sum => 0.15 + 0.85 * (sum + danglingSum / verticesNum))
    }
  }

}
