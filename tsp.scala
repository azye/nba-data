import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import org.apache.spark.api.java.JavaSparkContext

/**
 * File contains calculations for TSP (True Shooting Percentage)
 *
 */
object tsp {
  /**
   * Prints top n top TSP by players per season
   * @param n Number top elements to print
   */
  def allTSP(n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x => (x(14).toInt > 0 && x(8).toInt > 0));
    //(player, TS%, FG%, Diff, FT%, year)
    val per_season = data.map(x => (
      x(0),
      (x(22).toInt / (2 * (x(8).toDouble + (0.44 * x(14).toDouble)))),
      (x(7).toDouble / x(8).toDouble),
      (x(7).toDouble / x(8).toDouble) - (x(22).toInt / (2 * (x(8).toDouble + (0.44 * x(14).toDouble)))),
      (x(13).toDouble / x(14).toDouble),
      x(23)
    )).sortBy(_._2);

    per_season.collect().reverse.take(n).foreach(println);
  }

  /**
   * Prints top n top overall TSP by players over all seasons
   * @param n Number top elements to print
   */
  def overallTSP(n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x => (x(14).toInt > 0 && x(8).toInt > 50));

    val per_season = data.map(x => (x(0), x(8).toDouble, x(14).toDouble, x(22).toDouble, x(7).toDouble, x(13).toDouble));
    val player_fga = per_season.map(x => (x._1, x._2)).reduceByKey(_+_);
    val player_fta = per_season.map(x => (x._1, x._3)).reduceByKey(_+_);
    val player_pts = per_season.map(x => (x._1, x._4)).reduceByKey(_+_);
    val player_fgm = per_season.map(x => (x._1, x._5)).reduceByKey(_+_);
    val player_ftm = per_season.map(x => (x._1, x._6)).reduceByKey(_+_);
    // (Zoran Dragic,((((28.0,11.0),30.0),3.0),5.0))
    val player = player_pts.join(player_fgm)
    .join(player_fga).join(player_ftm).join(player_fta)
    .map(x => {
      val player = x._1;
      val pts = x._2._1._1._1._1;
      val fgm = x._2._1._1._1._2;
      val fga = x._2._1._1._2;
      val ftm = x._2._1._2;
      val fta = x._2._2;

      //(player, TS%, FG%, FT%, Diff)
      (
        player,
        pts / (2 * (fga + (0.44 * fta))),
        fgm / fga,
        ftm / fta,
        (fgm / fga) - (pts / (2 * (fga + (0.44 * fta))))
      )
    })
    .collect()
    .sortBy(_._5)
    .reverse
    .take(n)
    .foreach(println);
  }

  /**
   * Prints top n greatest difference in TSP with their FT%
   * @param n Number top elements to print
   */
  def tspByFreeThrow(n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x => (x(14).toInt > 0 && x(8).toInt > 0));

    val per_season = data.map(x => (x(0), x(8).toDouble, x(14).toDouble, x(22).toDouble, x(7).toDouble, x(13).toDouble));
    val player_fga = per_season.map(x => (x._1, x._2)).reduceByKey(_+_);
    val player_fta = per_season.map(x => (x._1, x._3)).reduceByKey(_+_);
    val player_pts = per_season.map(x => (x._1, x._4)).reduceByKey(_+_);
    val player_fgm = per_season.map(x => (x._1, x._5)).reduceByKey(_+_);
    val player_ftm = per_season.map(x => (x._1, x._6)).reduceByKey(_+_);

    val player = player_pts.join(player_fgm)
    .join(player_fga).join(player_ftm).join(player_fta)
    .map(x => {
      val player = x._1;
      val pts = x._2._1._1._1._1;
      val fgm = x._2._1._1._1._2;
      val fga = x._2._1._1._2;
      val ftm = x._2._1._2;
      val fta = x._2._2;

      (
        (fgm / fga) - (pts / (2 * (fga + (0.44 * fta)))),
        ftm / fta
      )
    })
    .collect()
    .sortBy(_._1)
    .reverse
    .take(10)
    .foreach(println);
  }
  /**
   * Prints TSP by season for player
   * @param player String of player name
   */
  def playerTSP(player: String) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x => (x(14).toInt > 0 && x(8).toInt > 0 && x(0) == player));

    //(player, TS%, FG%, Diff, FT%, year)
    val per_season = data.map(x => (x(0),
      (x(22).toInt / (2 * (x(8).toDouble + (0.44 * x(14).toDouble)))),
      (x(7).toDouble / x(8).toDouble),
      (x(7).toDouble / x(8).toDouble) - (x(22).toInt / (2 * (x(8).toDouble + (0.44 * x(14).toDouble)))),
      (x(13).toDouble / x(14).toDouble), x(23)
    )).sortBy(_._4);

    per_season.collect().reverse.take(10).foreach(println);
  }
}
