import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import org.apache.spark.api.java.JavaSparkContext

/**
 * File contains calculations for PER (Player Efficiency Rating)
 *
 */
object per {
  val NUM_YEARS = 9 // Global for number of years total

  def seasonPER(n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val per = Map("FGM" -> 85.910, "STL" -> 53.897,
                    "3PM"	-> 51.757, "FTM" -> 46.845,
                    "BLK"	-> 39.190, "ORB" -> 39.190,
                    "AST" -> 34.677, "DRB" -> 14.707,
                    "PF" -> -17.174, "FTF" -> -20.091,
                    "FGF" -> -39.190, "TOV" -> -53.897)

    val csv = sc.textFile("data/players.txt")
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
    )).reduceByKey(_ + _).filter(x => {
      !x._2.isInfinite
    }).sortBy(_._2);

    per_season.collect().reverse.take(n).foreach(x => println(x._1 + " " + x._2/NUM_YEARS))
  }

  /**
   * Prints PER by players per season
   * @param player String of player name
   */
  def playerPER(player: String) {
    val per = Map("FGM" -> 85.910, "STL" -> 53.897,
                    "3PM"	-> 51.757, "FTM" -> 46.845,
                    "BLK"	-> 39.190, "ORB" -> 39.190,
                    "AST" -> 34.677, "DRB" -> 14.707,
                    "PF" -> -17.174, "FTF" -> -20.091,
                    "FGF" -> -39.190, "TOV" -> -53.897)
    val csv = sc.textFile("data/players.txt")
    val player_data = csv.map(line => line.split(",")).filter(x => x(0) == player)

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
    )).sortBy(_._2);

    player_per.collect().foreach(x => println(x._1 + " " + x._2 + " " + x._3))
  }

  /**
   * Prints PERs of all seasons for two players
   * @param player1 String of player1 name
   * @param player2 String of player2 name
   */
  def playerComparison(player1: String, player2: String) {
    val per = Map("FGM" -> 85.910, "STL" -> 53.897,
                    "3PM"	-> 51.757, "FTM" -> 46.845,
                    "BLK"	-> 39.190, "ORB" -> 39.190,
                    "AST" -> 34.677, "DRB" -> 14.707,
                    "PF" -> -17.174, "FTF" -> -20.091,
                    "FGF" -> -39.190, "TOV" -> -53.897)
    val csv = sc.textFile("data/players.txt");
    val player_data = csv.map(line => line.split(","))
    .filter(x => (x(0) == player1 || x(0) == player2))
    .map(x => (x(0), x(23).toInt,
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
    )).sortBy(r => (r._2, r._3));
    player_data.foreach(println);
  }

  /**
   * Prints PERs of all past MVPs per season in the following format
   */
  def mvpPER() {
    val per = Map("FGM" -> 85.910, "STL" -> 53.897,
                    "3PM"	-> 51.757, "FTM" -> 46.845,
                    "BLK"	-> 39.190, "ORB" -> 39.190,
                    "AST" -> 34.677, "DRB" -> 14.707,
                    "PF" -> -17.174, "FTF" -> -20.091,
                    "FGF" -> -39.190, "TOV" -> -53.897);

    val players = sc.textFile("data/players.txt")
    .map(line => line.split(","))
    .map(x =>
      (
        x(0) + " " + x(23),
        (
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
        )
      )
    )

    val mvp = sc.textFile("data/mvps.txt")
    .map(line => line.split(","))
    .map(x => {
      (x(0) + " " + x(1), "");
    });

    players.join(mvp).map(x => (x._1, x._2._1))
    .collect()
    .foreach(println);
  }
}
