import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import org.apache.spark.api.java.JavaSparkContext


object App {

	//this function just calculates USG for each player
	def usg() {
		var players = sc.textFile("data/Players.txt")
		var mvps = sc.textFile("data/mvps.txt")
		
		//(  (team,year) (player, FGA, FTA, TOV, MP  ))
		var playerMap = players.map(line => ((line.split(",")(3), line.split(",")(23).toInt), 
			(line.split(",")(0), line.split(",")(8).toInt, line.split(",")(14).toInt, line.split(",")(20).toInt, line.split(",")(6).toInt )  )).filter(_._1._1 != "TOT").filter(_._2._5 > 60)

		//(name, year of mvp)
		var mvpMap = mvps.map(line => (line.split(",")(0),line.split(",")(1)))

		var teamData = playerMap.reduceByKey((x,y) => ("LUL",x._2 + y._2,x._3 + y._3,x._4 + y._4,x._5 + y._5)).map({case (a, (dummy,b,c,d,e)) => (a, (b,c,d,e))}).filter(_._1._1 != "TOT")

		var join = playerMap.join(teamData).map({case ((team,year), (  (player, fga, fta, tov, mp),(tmFGA, tmFTA, tmTOV, tmMP)  ) ) => 
			((team,year), (player,   100 * ((fga + 0.44 * fta + tov) * (tmMP/5))/(mp * (tmFGA + 0.44 * tmFTA + tmTOV))    ))}).sortBy(_._2._2)


	}

	//this function gets teh top 5 avg USG%
	def topFiveAvg {
		var players = sc.textFile("data/Players.txt")
		var mvps = sc.textFile("data/mvps.txt")
		
		//(  (team,year) (player, FGA, FTA, TOV, MP  ))
		var playerMap = players.map(line => ((line.split(",")(3), line.split(",")(23).toInt), 
			(line.split(",")(0), line.split(",")(8).toInt, line.split(",")(14).toInt, line.split(",")(20).toInt, line.split(",")(6).toInt )  )).filter(_._1._1 != "TOT").filter(_._2._5 > 60)

		//(name, year of mvp)
		var mvpMap = mvps.map(line => (line.split(",")(0),line.split(",")(1)))

		var teamData = playerMap.reduceByKey((x,y) => ("LUL",x._2 + y._2,x._3 + y._3,x._4 + y._4,x._5 + y._5)).map({case (a, (dummy,b,c,d,e)) => (a, (b,c,d,e))}).filter(_._1._1 != "TOT")

		var join = playerMap.join(teamData).map({case ((team,year), (  (player, fga, fta, tov, mp),(tmFGA, tmFTA, tmTOV, tmMP)  ) ) => 
			((team,year), (player,   100 * ((fga + 0.44 * fta + tov) * (tmMP/5))/(mp * (tmFGA + 0.44 * tmFTA + tmTOV))    ))}).sortBy(_._2._2)




		//getting top 5 averages here
		
		var top = join.map({case ((team, year), (player, usg)) => (player, (usg,1))}).reduceByKey( (x,y) => (x._1+y._1, x._2 + y._2))
		.map({case (player, (totUSG, tot)) => (player, totUSG / tot)}).sortBy(_._2).collect().foreach(println(_))
		/*
			(Russell Westbrook,31.20551115599283)
			(LeBron James,31.875205033382297)
			(Dwyane Wade,32.357325868679126)
			(Carmelo Anthony,32.42367438948769)
			(Kobe Bryant,32.722217585187714)

		
		*/
		//print out to file
		var collect = join.map({case ((team, year), (player, usg)) => (player, (team,year,usg))}).groupByKey().saveAsTextFile("LUL")
		/*
			(Russell Westbrook,CompactBuffer((OKC,2009,25.306688919055226), (OKC,2010,25.489211000280612), (OKC,2011,30.821541082672557), (OKC,2016,31.159914124896193), (OKC,2012,32.574300353106466), (OKC,2013,32.60210508896673), (OKC,2014,34.284473186750816), (OKC,2015,37.40585549221402)))
			(LeBron James,CompactBuffer((MIA,2013,30.344155415773265), (MIA,2011,30.86466897495315), (MIA,2014,30.976560253362898), (CLE,2007,31.128098167376773), (CLE,2016,31.33022761575263), (CLE,2015,31.741292627541807), (MIA,2012,31.851331984379428), (CLE,2008,32.566163543594726), (CLE,2009,33.958305576071325), (CLE,2010,33.991246175017)))
			

			(Dwyane Wade,CompactBuffer((MIA,2014,27.88948469120383), (MIA,2013,29.64561691175343), (MIA,2011,31.0048626264816), 
			(MIA,2012,31.193410260463324), (MIA,2016,31.544878104676357), (MIA,2008,33.619695367601295), (MIA,2015,33.974235827939694), 
			(MIA,2007,34.415472074612595), (MIA,2010,34.90621796400854), (MIA,2009,35.37938485805059)))

			(Carmelo Anthony,CompactBuffer((NYK,2016,29.900004065807554), (DEN,2008,30.108012107765443), (NYK,2012,31.99903264211251), 
			(DEN,2009,32.05297312103961), (NYK,2014,32.39131647217601), (NYK,2015,32.44607787649335), (DEN,2010,33.45713912171006), 
			(DEN,2007,34.14139657243626), (NYK,2013,35.317117525848374)))


			(Kobe Bryant,CompactBuffer((LAL,2014,28.59950725898449), (LAL,2008,31.130164995286385), (LAL,2013,31.97925627116007), 
			(LAL,2009,31.989214229198627), (LAL,2010,32.377668673003576), (LAL,2016,32.4446817773823), (LAL,2007,33.78338827228036), (
			LAL,2015,34.704256822889775), (LAL,2012,34.973514782805296), (LAL,2011,35.24052276888624)))
		*/
	}

	//this function gets the USG avg for the mvps
	def mvpUsgAvg {
		var players = sc.textFile("data/Players.txt")
		var mvps = sc.textFile("data/mvps.txt")
		
		//(  (team,year) (player, FGA, FTA, TOV, MP  ))
		var playerMap = players.map(line => ((line.split(",")(3), line.split(",")(23).toInt), 
			(line.split(",")(0), line.split(",")(8).toInt, line.split(",")(14).toInt, line.split(",")(20).toInt, line.split(",")(6).toInt )  )).filter(_._1._1 != "TOT").filter(_._2._5 > 60)

		//(name, year of mvp)
		var mvpMap = mvps.map(line => (line.split(",")(0),line.split(",")(1)))

		var teamData = playerMap.reduceByKey((x,y) => ("LUL",x._2 + y._2,x._3 + y._3,x._4 + y._4,x._5 + y._5)).map({case (a, (dummy,b,c,d,e)) => (a, (b,c,d,e))}).filter(_._1._1 != "TOT")

		var join = playerMap.join(teamData).map({case ((team,year), (  (player, fga, fta, tov, mp),(tmFGA, tmFTA, tmTOV, tmMP)  ) ) => 
			((team,year), (player,   100 * ((fga + 0.44 * fta + tov) * (tmMP/5))/(mp * (tmFGA + 0.44 * tmFTA + tmTOV))    ))}).sortBy(_._2._2)







		//getting MVP usgg avgs
		
		var mvpAvg = join.map({case ((team, year), (player, usg)) => (player, (usg,1))}).reduceByKey( (x,y) => (x._1+y._1, x._2 + y._2))
		.map({case (player, (totUSG, tot)) => (player, totUSG / tot)}).join(mvpMap).collect().foreach(println(_))
		
		/*
		(LeBron James,(31.875205033382297,2013))
		(LeBron James,(31.875205033382297,2012))
		(LeBron James,(31.875205033382297,2010))
		(LeBron James,(31.875205033382297,2009))
		(Stephen Curry,(26.662050303085888,2016))
		(Stephen Curry,(26.662050303085888,2015))
		(Dirk Nowitzki,(27.32173671939476,2007))
		(Derrick Rose,(29.192118466221864,2011))
		(Kobe Bryant,(32.722217585187714,2008))
		(Kevin Durant,(29.924840670383176,2014))


		*/
	}

	//this function prints out average usg per each team
	def avgUSGByTeam {
		var players = sc.textFile("data/Players.txt")
		var mvps = sc.textFile("data/mvps.txt")
		
		//(  (team,year) (player, FGA, FTA, TOV, MP  ))
		var playerMap = players.map(line => ((line.split(",")(3), line.split(",")(23).toInt), 
			(line.split(",")(0), line.split(",")(8).toInt, line.split(",")(14).toInt, line.split(",")(20).toInt, line.split(",")(6).toInt )  )).filter(_._1._1 != "TOT").filter(_._2._5 > 60)

		//(name, year of mvp)
		var mvpMap = mvps.map(line => (line.split(",")(0),line.split(",")(1)))

		var teamData = playerMap.reduceByKey((x,y) => ("LUL",x._2 + y._2,x._3 + y._3,x._4 + y._4,x._5 + y._5)).map({case (a, (dummy,b,c,d,e)) => (a, (b,c,d,e))}).filter(_._1._1 != "TOT")

		var join = playerMap.join(teamData).map({case ((team,year), (  (player, fga, fta, tov, mp),(tmFGA, tmFTA, tmTOV, tmMP)  ) ) => 
			((team,year), (player,   100 * ((fga + 0.44 * fta + tov) * (tmMP/5))/(mp * (tmFGA + 0.44 * tmFTA + tmTOV))    ))})
			.map({case ((team,year), (player,usg)) => ((team,year), (usg, 1))}).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2) )
			.map({case ((team,year),(totUSG, tot)) => ((team,year), totUSG/tot)}).sortBy(_._2).collect().foreach(println(_))

	}



}


App.avgUSGByTeam