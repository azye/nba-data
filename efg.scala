import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import org.apache.spark.api.java.JavaSparkContext

/**
 * File contains calculations for EFG (Effective Shooting Percentage)
 *
 */
object efg {
  /**
   * Prints top n top EFG by players per season
   * @param n Number top elements to print
   */
  def allEFG(n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x => (x(14).toInt > 0 && x(8).toInt > 0 && x(8).toInt > 50));

    val per_season = data.map(x => (x(0),
     ((x(7).toDouble + (0.5 * x(9).toDouble)) / x(8).toDouble),
     x(23)
    )).sortBy(_._2);

    per_season.collect().reverse.take(n).foreach(println);
  }

  /**
   * Prints top n top EFG by players per season for a given position
   * @param pos String of position
   * @param n Number top elements to print
   */
  def allEFGByPos(pos: String, n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x =>
      (x(14).toInt > 0 && x(8).toInt > 0 && x(8).toInt > 50 && x(1) == pos)
    );
    // (FG + 0.5 * 3P) / FGA
    val per_season = data.map(x => (x(0),
      ((x(7).toDouble + (0.5 * x(9).toDouble)) / x(8).toDouble),
      x(23)
    )).sortBy(_._2);

    per_season.collect().reverse.take(n).foreach(println);
  }

  /**
   * Prints top n top overall EFG by players over all seasons
   * @param n Number top elements to print
   */
  def overallEFG(n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x =>
      (x(14).toInt > 0 && x(8).toInt > 0 && x(8).toInt > 50)
    );

    val per_season = data.map(x => (x(0), x(8).toDouble, x(7).toDouble, x(9).toDouble));
    val player_fga = per_season.map(x => (x._1, x._2)).reduceByKey(_+_);
    val player_3pm = per_season.map(x => (x._1, x._4)).reduceByKey(_+_);
    val player_fgm = per_season.map(x => (x._1, x._3)).reduceByKey(_+_);

    val player = player_fgm.join(player_3pm).join(player_fga)
    .map(x => {
      val player = x._1;
      val fgm = x._2._1._1;
      val fga = x._2._2;
      val m3p = x._2._1._2;
      (
        player,
        (fgm + (0.5 * m3p)) / fga,
        fgm / fga,
        (fgm / fga) - ((fgm + (0.5 * m3p)) / fga)
      )
    })
    .collect()
    .sortBy(_._2)
    .reverse
    .take(n)
    .foreach(println);
  }

  /**
   * Prints top n top overall EFG by players over all seasons by position
   * @param n Number top elements to print
   * @param pos String of position: "PG", "SG", "SF", "PF", "C"
   */
  def overallEFGByPos(pos: String, n: Int) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x =>
     (x(14).toInt > 0 && x(8).toInt > 0 && x(8).toInt > 50 && x(1) == pos)
    );

    val per_season = data.map(x => (x(0), x(8).toDouble, x(7).toDouble, x(9).toDouble));
    val player_fga = per_season.map(x => (x._1, x._2)).reduceByKey(_+_);
    val player_3pm = per_season.map(x => (x._1, x._4)).reduceByKey(_+_);
    val player_fgm = per_season.map(x => (x._1, x._3)).reduceByKey(_+_);

    val player = player_fgm.join(player_3pm).join(player_fga)
    .map(x => {
      val player = x._1;
      val fgm = x._2._1._1;
      val fga = x._2._2;
      val m3p = x._2._1._2;
      (
        player,
        (fgm + (0.5 * m3p)) / fga,
        fgm / fga,
        (fgm / fga) - ((fgm + (0.5 * m3p)) / fga)
      )
    })
    .collect()
    .sortBy(_._2)
    .reverse
    .take(n)
    .foreach(println);
  }

  /**
   * Prints al EFGs for a plaer for each season
   * @param player String of players name
   */
  def efgByPlayer(player: String) {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val csv = sc.textFile("data/players.txt");
    val data = csv.map(line => line.split(",")).filter(x =>
     (x(14).toInt > 0 && x(8).toInt > 0 && x(8).toInt > 50 && x(0) == player)
    );
    // (FG + 0.5 * 3P) / FGA
    val per_season = data.map(x => (x(0),
     ((x(7).toDouble + (0.5 * x(9).toDouble)) / x(8).toDouble),
     (x(7).toDouble / x(8).toDouble),
     x(23).toInt
   )).sortBy(_._4);

    per_season.collect().reverse.foreach(println);
  }
}
