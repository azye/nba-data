import scala.io._
import org.apache.log4j._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/** Players.txt column names
 * Player,Pos,Age,Team,GP,GS,MIN,FGM,FGA,3PM,3PA,2PM,2PA,FTM,FTA,ORB,DRB,AST,STL,BLK,TOV,PF,PTS,Year
 *
 */
object per48 {
  /**
   * Returns the players per 48 minutes of the given stat over all their years of play
   *
   * statColumn - use the number below one of the []ed stats
   * PlayerName - name of the player
   * sc					- the SparkContext
   * can use any per48 method on any of the stats in []
   * Player,Pos,Age,Team,GP,GS,MIN,[FGM,FGA,3PM,3PA,2PM,2PA,FTM,FTA,ORB,DRB,AST,STL,BLK,TOV,PF,PTS],Year
   *                                [ 7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,21,22]
   *
   */
  def per48(statColumn: Int, PlayerName: String) = {
    var yearColumn = 23
    val minutesColumn = 6

    var players = sc.textFile("data/players.txt")

    var stats = players.map(line => (line.split(",")(0).toUpperCase(),
      (line.split(",")(yearColumn).toInt, line.split(",")(statColumn).toInt, line.split(",")(minutesColumn).toDouble / 48)))

    .filter(_._1 == PlayerName.toUpperCase())
    .map({case (name, (year, stat, minutes)) => (year, (stat, minutes))})
    .mapValues(f => {
     if (f._2 == 0)
       0
     else
       f._1 / f._2
    })

    .collect().sortBy(_._1).foreach(println(_))

  }
  /**
   * Returns the Best Player for a given stat each year
   *
   * statColumn - use the number below one of the []ed stats
   * sc					- the SparkContext
   * can use any per48 method on any of the stats in []
   * Player,Pos,Age,Team,GP,GS,MIN,[FGM,FGA,3PM,3PA,2PM,2PA,FTM,FTA,ORB,DRB,AST,STL,BLK,TOV,PF,PTS],Year
   *                                [ 7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,21,22]
   *
   */
  def bestPer48(statColumn: Int) = {
    var yearColumn = 23
    val minutesColumn = 6

    var players = sc.textFile("data/players.txt")

    var stats = players.map(line => (line.split(",")(0),
      (line.split(",")(yearColumn).toInt, line.split(",")(statColumn).toInt, line.split(",")(minutesColumn).toDouble)))
    .filter(_._2._3 > 60)
    .map({case (name, (year, stat, minutes)) => (year, (name, stat, minutes / 48))})
    .mapValues(f => {
     if (f._3 == 0)
       (f._1, 0.toDouble)
     else
       (f._1, f._2 / f._3)
    })
    .groupByKey()
    .map(f => (f._1, (f._2.maxBy(_._2))))
    .collect().sortBy(_._1).foreach(println(_))

  }

  /**
   * Returns the teams overall per 48 for a given stat
   *
   * statColumn - use the number below one of the []ed stats
   * team			  - abbreviation of the team name
   * sc					- the SparkContext
   * can use any per48 method on any of the stats in []
   * Player,Pos,Age,Team,GP,GS,MIN,[FGM,FGA,3PM,3PA,2PM,2PA,FTM,FTA,ORB,DRB,AST,STL,BLK,TOV,PF,PTS],Year
   *                                [ 7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,21,22]
   *
   */
  def teamPer48(statColumn: Int, team: String) = {
    val yearColumn = 23
    val minutesColumn = 6
    var players = sc.textFile("data/players.txt")
    var stats = players.map(line => (line.split(",")(3),
      (line.split(",")(yearColumn).toInt, line.split(",")(statColumn).toInt, line.split(",")(minutesColumn).toDouble / 48)))
    .filter(_._1 == team)
    .map({case (team, (year, stat, min)) => (team + ": " + year, (stat / min))})
    .combineByKey(x => x,
        (acc: (Double), z) => (acc + z),
        (acc1: (Double), acc2: (Double)) => (acc1 + acc2))
    .mapValues(f => f)
    .map({ case (teamYear, stat) => (teamYear.split(": ")(1), teamYear.split(": ")(0), stat)})
    .collect().sortBy(_._1).foreach(println(_))

  }

  /**
   * Returns the Championship Teams per 48 for a given stat for each year
   * statColumn - use the number below one of the []ed stats
   * sc					- the SparkContext
   * can use any per48 method on any of the stats in []
   * Player,Pos,Age,Team,GP,GS,MIN,[FGM,FGA,3PM,3PA,2PM,2PA,FTM,FTA,ORB,DRB,AST,STL,BLK,TOV,PF,PTS],Year
   *                                [ 7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,21,22]
   *
   */
  def championTeamPer48(statColumn: Int) = {
    var champions = sc.textFile("data/champions.txt")
    var players = sc.textFile("data/players.txt")

    var yearColumn = 23
    val minutesColumn = 6

    var stats = players.map(line => (line.split(",")(3),
      (line.split(",")(yearColumn).toInt, line.split(",")(statColumn).toInt, line.split(",")(minutesColumn).toDouble / 48)))
    .map({case (team, (year, stat, min)) => (team + ": " + year, (stat / min))})
    .combineByKey(x => x,
        (acc: (Double), z) => (acc + z),
        (acc1: (Double), acc2: (Double)) => (acc1 + acc2))
    .mapValues(f => f)

    var nameYear = champions.map(line => (line.split(",")(0), line.split(",")(1).toInt))
    .map({ case (name, year) => (name + ": " + year, 0.0)})

    nameYear.join(stats)
    .mapValues(f => f._2)
    .map({ case (teamYear, stat) => (teamYear.split(": ")(1), teamYear.split(": ")(0), stat)})
    .collect().sortBy(_._1).foreach(println(_))
  }

  /**
   * Returns the MVP Player's per48 stat each year
   * statColumn - use the number below one of the []ed stats
   * sc					- the SparkContext
   * can use any per48 method on any of the stats in []
   * Player,Pos,Age,Team,GP,GS,MIN,[FGM,FGA,3PM,3PA,2PM,2PA,FTM,FTA,ORB,DRB,AST,STL,BLK,TOV,PF,PTS],Year
   *                                [ 7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,21,22]
   *
   */
  def mvpPer48(statColumn: Int) = {
    var mvpPlayers = sc.textFile("data/mvps.txt")
    var players = sc.textFile("data/players.txt")
    var yearColumn = 23
    val minutesColumn = 6

    var stats = players.map(line => (line.split(",")(0).toLowerCase(),
      (line.split(",")(yearColumn).toInt, line.split(",")(statColumn).toInt, line.split(",")(minutesColumn).toDouble / 48)))
    .map({case (name, (year, stat, minutes)) => (name + ": " + year, (stat, minutes))})
    .mapValues(f => {
     if (f._2 == 0)
       0
     else
       f._1 / f._2
    })

    var nameYear = mvpPlayers.map(line => (line.split(",")(0).toLowerCase(), line.split(",")(1).toInt))
    .map({ case (name, year) => (name + ": " + year, 0.0)})

    nameYear.join(stats)
    .mapValues(f => f._2)
    .map({ case (nameYear, stat) => (nameYear.split(": ")(1), nameYear.split(": ")(0), stat)})
    .collect().sortBy(_._1).foreach(println(_))
  }

}
