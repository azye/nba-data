import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import org.apache.spark.api.java.JavaSparkContext
val NUM_YEARS = 3

object App {
  def seasonPer() {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    var per = Map("FGM" -> 85.910, "STL" -> 53.897,
                    "3PM"	-> 51.757, "FTM" -> 46.845,
                    "BLK"	-> 39.190, "ORB" -> 39.190,
                    "AST" -> 34.677, "DRB" -> 14.707,
                    "PF" -> -17.174, "FTF" -> -20.091,
                    "FGF" -> -39.190, "TOV" -> -53.897)

    val csv = sc.textFile("data/test_players.txt")
    val data = csv.map(line => line.split(","))

    // Best average PER, per year
    val per_season = data.map(x => (x(0),
      (x(7).toInt * per.get("FGM").get +
      x(18).toInt * per.get("STL").get +
      x(9).toInt * per.get("3PM").get +
      x(13).toInt * per.get("FTM").get +
      x(19).toInt * per.get("BLK").get +
      x(16).toInt * per.get("ORB").get +
      x(17).toInt * per.get("AST").get +
      x(16).toInt * per.get("DRB").get +
      x(21).toInt * per.get("PF").get +
      (x(14).toInt - x(13).toInt) * per.get("FTF").get +
      (x(8).toInt - x(7).toInt) * per.get("FGF").get +
      x(20).toInt * per.get("TOV").get) * (1/x(6).toDouble)
    )).reduceByKey(_ + _).sortBy(_._2);

    per_season.collect().foreach(x => println(x._1 + " " + x._2/NUM_YEARS))

  }

  def playerPer(player: String) {
    var per = Map("FGM" -> 85.910, "STL" -> 53.897,
                    "3PM"	-> 51.757, "FTM" -> 46.845,
                    "BLK"	-> 39.190, "ORB" -> 39.190,
                    "AST" -> 34.677, "DRB" -> 14.707,
                    "PF" -> -17.174, "FTF" -> -20.091,
                    "FGF" -> -39.190, "TOV" -> -53.897)
    val csv = sc.textFile("data/prod_players.txt")
    val player_data = csv.map(line => line.split(",")).filter(x => x(0) == player)
    // lebron_data.foreach(x => println(x(0) + " " + x(23)));
    val player_per = player_data.map(x => (x(0), x(23),
      (x(7).toInt * per.get("FGM").get +
      x(18).toInt * per.get("STL").get +
      x(9).toInt * per.get("3PM").get +
      x(13).toInt * per.get("FTM").get +
      x(19).toInt * per.get("BLK").get +
      x(16).toInt * per.get("ORB").get +
      x(17).toInt * per.get("AST").get +
      x(16).toInt * per.get("DRB").get +
      x(21).toInt * per.get("PF").get +
      (x(14).toInt - x(13).toInt) * per.get("FTF").get +
      (x(8).toInt - x(7).toInt) * per.get("FGF").get +
      x(20).toInt * per.get("TOV").get) * (1/x(6).toDouble)
    )).sortBy(_._3);

    player_per.collect().foreach(x => println(x._1 + " " + x._2 + " " + x._3))
  }
}

// App.seasonPer
App.playerPer("Tony Parker")
