package edu.calpoly.myprogram
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
object App {
   def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("NameOfApp")
      val sc = new SparkContext(conf)
      val qb = sc.textFile("/user/jkmar/input/project/Quarterback.csv");
      val rb = sc.textFile("/user/jkmar/input/project/Runningback.csv");
      val te = sc.textFile("/user/jkmar/input/project/Tightend.csv");
      val wr = sc.textFile("/user/jkmar/input/project/Widereciever.csv");
      
      val team1 = "BAL"  //Use team acronyms as found on NFL.com 
      val team2 = "WAS"
      
      println("Starting offfensive calculations")
      //0 rk,1 name,2 team,3 pos,4 comp,5 att,6 pct,7 att/g,8 yds, 9 avgyds, 11 td, 12 int, 13 1st, 14 1st%, 15 Lng (has T), 16 20+, 17 40+, 18 Fum, 19 rating
      //qb.collect().foreach(println(_))

      //0 rk,1 name,2 team,3 pos,4 rec, 5 yds, 6 avg per, 7 avg/g, 8 lng (has T), 9 td, 10 20+, 11 40+, 12 1st, 13 1st%, 14 Fum 
      // wr.collect().foreach(println(_))
      
      //0 rk,1 name,2 team,3 pos,4 att,5 att/g,6 yds,7 avg/g,8 td, 9 lng (has T), 10 1st, 11 1st%, 12 20+, 13 40+,14 Fum 
      //rb.collect().foreach(println(_))
      
      //0 rk,1 name,2 team,3 pos,4 rec, 5 yds, 6 avg per, 7 avg/g, 8 lng (has T), 9 td, 10 20+, 11 40+, 12 1st, 13 1st%, 14 Fum       
     // te.collect().foreach(println(_))


      // completed * percentage * avgper * tds - ints + * 1st% + 20s + 40s - sacks
      val qbVal = qb.map(x=>(x.split(",")(2).trim(), (((x.split(",")(4).trim().toDouble * x.split(",")(6).trim().toDouble) * 0.005 * ((x.split(",")(11).toDouble)/7) - x.split(",")(12).toDouble * 25) * (1 + x.split(",")(14).toDouble/100) - x.split(",")(18).toDouble * 7 + x.split(",")(16).trim().toDouble + x.split(",")(17).trim().toDouble *2 ))).filter(x=>x._1==team1).sortBy(x=>x._2, false).take(1)(0)._2 + 100
      

      val wrVal = sc.parallelize(wr.map(x=>(x.split(",")(2).trim(), ((x.split(",")(4).trim().toDouble/10 * x.split(",")(6).trim().toDouble + x.split(",")(9).trim().toDouble/(1 + x.split(",")(14).trim().toDouble *3) +  x.split(",")(10).trim().toDouble + x.split(",")(11).trim().toDouble *2)  *  (x.split(",")(13).trim().toDouble/100 + 1)))).filter(x=>x._1==team1).sortBy(x=>x._2, false).values.take(3)).sum()
      
      val teVal = sc.parallelize(te.map(x=>(x.split(",")(2).trim(), (x.split(",")(4).trim().toDouble/10 * x.split(",")(6).trim().toDouble + x.split(",")(9).trim().toDouble/(1 + x.split(",")(14).trim().toDouble *3) +  x.split(",")(10).trim().toDouble + x.split(",")(11).trim().toDouble *2 ) *  (x.split(",")(13).trim().toDouble/100 + 1))).filter(x=>x._1==team1).sortBy(x=>x._2, false).values.take(2)).sum()
     
     val rbVal = sc.parallelize(rb.map(x=>(x.split(",")(2).trim(), ((x.split(",")(4).trim().toDouble/10 * x.split(",")(7).trim().toDouble + x.split(",")(9).trim().toDouble/(1 + x.split(",")(15).trim().toDouble *3) +  x.split(",")(13).trim().toDouble + x.split(",")(14).trim().toDouble *2 ) *  (x.split(",")(12).trim().toDouble/100 + 1) + 100))).filter(x=>x._1==team1).sortBy(x=>x._2,false).values.take(2)).sum()
   
      val pOff = qbVal + wrVal + teVal
      val rOff = rbVal * 2.2
//rk 0,player 1, team 2, POS 3, COMB 4, Total 5, Assist 6, sack 7, saftey 8, blockedpasses 9, int 10, TD 11, yds 12, LNG 13, Fumbles 14, Rec 15, TD 16
     var linebacker = sc.textFile("/user/jkmar/input/project/linebacker.csv");
     var linemen = sc.textFile("/user/jkmar/input/project/lineman.csv");
     var defensiveback = sc.textFile("/user/jkmar/input/project/defensiveback.csv");
     
     val linebackerData = linebacker.map(line => (line.split(",")(2) + ", " + line.split(",")(3), (line.split(",")(6).toDouble * .05 + line.split(",")(5).toDouble * .07 + line.split(",")(14).toDouble * .7)));
     val MLB = sc.parallelize(linebackerData.filter(x=>(x._1).split(",")(1).trim() == "MLB").filter(x=>(x._1).split(",")(0).trim() == team1).sortBy(x=>x._2, false).values.take(1)).sum();
     val ILB = sc.parallelize(linebackerData.filter(x=>(x._1).split(",")(1).trim() == "ILB").filter(x=>(x._1).split(",")(0).trim() == team1).sortBy(x=>x._2, false).values.take(2)).sum();
     val OLB = sc.parallelize(linebackerData.filter(x=>(x._1).split(",")(1).trim() == "OLB").filter(x=>(x._1).split(",")(0).trim() == team1).sortBy(x=>x._2, false).values.take(2)).sum();
     
     
    val linemenData = linemen.map(line=>(line.split(",")(2) + ", " + line.split(",")(3), (line.split(",")(6).toDouble * .1 + line.split(",")(5).toDouble * .2 + line.split(",")(7).toDouble * .6 + line.split(",")(14).toDouble * .5)));
    val DT = sc.parallelize(linemenData.filter(x=>(x._1).split(",")(1).trim() == "DT").filter(x=>(x._1).split(",")(0).trim() == team1).sortBy(x=>x._2, false).values.take(2)).sum();
    val DE = sc.parallelize(linemenData.filter(x=>(x._1).split(",")(1).trim() == "DE").filter(x=>(x._1).split(",")(0).trim() == team1).sortBy(x=>x._2, false).values.take(2)).sum();
    
     var rDef = (MLB + ILB + OLB + DT + DE) * 15
     
    val defensivebackData = defensiveback.map(line=>(line.split(",")(2) + ", " + line.split(",")(3), (line.split(",")(6).toDouble * .1 + line.split(",")(5).toDouble * .08 + line.split(",")(9).toDouble * .4 + line.split(",")(10).toDouble * .5)));
    val SS = sc.parallelize(defensivebackData.filter(x=>(x._1).split(",")(1).trim() == "SS").filter(x=>(x._1).split(",")(0).trim() == team1).sortBy(x=>x._2, false).values.take(3)).sum();
    val CB = sc.parallelize(defensivebackData.filter(x=>(x._1).split(",")(1).trim() == "CB").filter(x=>(x._1).split(",")(0).trim() == team1).sortBy(x=>x._2, false).values.take(3)).sum();
    
     var pDef = ((SS + CB) * 10) + (rDef * 0.5)

     println(team1 + " pass Offense - " + pOff + " pass Defense - " + pDef)
     println("rush Offense - " + rOff + " rush Defense - " + rDef)

      val qbVal1 = qb.map(x=>(x.split(",")(2).trim(), (((x.split(",")(4).trim().toDouble * x.split(",")(6).trim().toDouble) * 0.005 * ((x.split(",")(11).toDouble)/7) - x.split(",")(12).toDouble * 25) * (1 + x.split(",")(14).toDouble/100) - x.split(",")(18).toDouble * 7 + x.split(",")(16).trim().toDouble + x.split(",")(17).trim().toDouble *2 ))).filter(x=>x._1==team2).sortBy(x=>x._2, false).take(1)(0)._2 + 100
      

      val wrVal1 = sc.parallelize(wr.map(x=>(x.split(",")(2).trim(), ((x.split(",")(4).trim().toDouble/10 * x.split(",")(6).trim().toDouble + x.split(",")(9).trim().toDouble/(1 + x.split(",")(14).trim().toDouble *3) +  x.split(",")(10).trim().toDouble + x.split(",")(11).trim().toDouble *2)  *  (x.split(",")(13).trim().toDouble/100 + 1)))).filter(x=>x._1==team2).sortBy(x=>x._2, false).values.take(3)).sum()
      
      val teVal1 = sc.parallelize(te.map(x=>(x.split(",")(2).trim(), (x.split(",")(4).trim().toDouble/10 * x.split(",")(6).trim().toDouble + x.split(",")(9).trim().toDouble/(1 + x.split(",")(14).trim().toDouble *3) +  x.split(",")(10).trim().toDouble + x.split(",")(11).trim().toDouble *2 ) *  (x.split(",")(13).trim().toDouble/100 + 1))).filter(x=>x._1==team2).sortBy(x=>x._2, false).values.take(2)).sum()
     
     val rbVal1 = sc.parallelize(rb.map(x=>(x.split(",")(2).trim(), ((x.split(",")(4).trim().toDouble/10 * x.split(",")(7).trim().toDouble + x.split(",")(9).trim().toDouble/(1 + x.split(",")(15).trim().toDouble *3) +  x.split(",")(13).trim().toDouble + x.split(",")(14).trim().toDouble *2 ) *  (x.split(",")(12).trim().toDouble/100 + 1) + 100))).filter(x=>x._1==team2).sortBy(x=>x._2,false).values.take(2)).sum()
   
      val pOff1 = qbVal1 + wrVal1 + teVal1
      val rOff1 = rbVal1 * 2.2
//rk 0,player 1, team 2, POS 3, COMB 4, Total 5, Assist 6, sack 7, saftey 8, blockedpasses 9, int 10, TD 11, yds 12, LNG 13, Fumbles 14, Rec 15, TD 16

     
     val MLB1 = sc.parallelize(linebackerData.filter(x=>(x._1).split(",")(1).trim() == "MLB").filter(x=>(x._1).split(",")(0).trim() == team2).sortBy(x=>x._2, false).values.take(1)).sum();
     val ILB1 = sc.parallelize(linebackerData.filter(x=>(x._1).split(",")(1).trim() == "ILB").filter(x=>(x._1).split(",")(0).trim() == team2).sortBy(x=>x._2, false).values.take(2)).sum();
     val OLB1 = sc.parallelize(linebackerData.filter(x=>(x._1).split(",")(1).trim() == "OLB").filter(x=>(x._1).split(",")(0).trim() == team2).sortBy(x=>x._2, false).values.take(2)).sum();
     
     
    val DT1 = sc.parallelize(linemenData.filter(x=>(x._1).split(",")(1).trim() == "DT").filter(x=>(x._1).split(",")(0).trim() == team2).sortBy(x=>x._2, false).values.take(2)).sum();
    val DE1 = sc.parallelize(linemenData.filter(x=>(x._1).split(",")(1).trim() == "DE").filter(x=>(x._1).split(",")(0).trim() == team2).sortBy(x=>x._2, false).values.take(2)).sum();
    
     var rDef1 = (MLB1 + ILB1 + OLB1 + DT1 + DE1) * 15
     
    val SS1 = sc.parallelize(defensivebackData.filter(x=>(x._1).split(",")(1).trim() == "SS").filter(x=>(x._1).split(",")(0).trim() == team2).sortBy(x=>x._2, false).values.take(3)).sum();
    val CB1 = sc.parallelize(defensivebackData.filter(x=>(x._1).split(",")(1).trim() == "CB").filter(x=>(x._1).split(",")(0).trim() == team2).sortBy(x=>x._2, false).values.take(3)).sum();
    
     var pDef1 = ((SS1 + CB1) * 10) + (rDef1 * 0.5)

     println(team2 + " pass Offense - " + pOff1 + " pass Defense - " + pDef1)
     println("run Offense - " + rOff1 + " rush defense - " + rDef1)    

     var ptotal = 0.0
     var rtotal = 0.0
     var ptotal1 = 0.0
     var rtotal1 = 0.0
    if (pOff > rOff){
       ptotal = (pOff - pDef1) * .6
       rtotal = (rOff - rDef1) * .4
    }
    else {
      ptotal = (pOff - pDef1) * .4
      rtotal = (rOff - rDef1) * .6
    }

    if (pOff1 > rOff1){
      ptotal1 = (pOff1 - pDef) * .6
      rtotal1 = (rOff1 - rDef) * .4
    }
    else{
      ptotal1 = (pOff1 - pDef) * .4
      rtotal1 = (rOff1 - pDef) * .6
    }

    var team1total = ptotal + rtotal
    var team2total = ptotal1 + rtotal1
    var diffOff = 0.0;
    if(team1total > team2total) {
      println(team1 + " wins")
      diffOff = team1total - team2total;
    }
    else {
      println(team2 + " wins")
      diffOff = team1total - team2total;
    }

    if(diffOff < 100) {
      println("by around 3 points!")
    }
    else if (diffOff < 150) {
      println("by around 5 points!")
    }
    else if(diffOff < 250) {
      println("by around 7 points!")
    }
    else if(diffOff < 300) {
      println("by around 10 points!")
    }
    else {
      println("by a blowout! (more than 14 points)")
    }

  }
}  

