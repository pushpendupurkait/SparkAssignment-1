import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Assingment{
  def main(args: Array[String]) {
    println("\n\n\n\n"+args(0)+"\n\n\n\n\n")
    val conf = new SparkConf().setAppName("Assignment1").setMaster("local")
    val sc = new SparkContext(conf)
    val pagecounts = sc.textFile(args(0))
    pagecounts.take(10).map(println)

    val totalRecords = pagecounts.count()
    println("Total Records: " + totalRecords + "\n\n\n\n")

    val countEnglish = pagecounts.map(_.split(" ")(0)).filter(_.equals("en"))
    println("\n\n\n\nCount of English: " + countEnglish.count())


    val requests = pagecounts.map(rec => (rec.split(" ")(1), rec.split(" ")(2).toInt))
      .reduceByKey(_ + _)
      .filter(_._2 > 200000)
      .keys.collect.toList
    //pagecounts.map(rec=>(rec.split(" ")(1),rec.split(" ")(2).toInt)).filter(_._1.equals("de")).collect
    println("List of pages with more than 200,000 Requests:")
    requests.map(element => println(element))
  }
}
