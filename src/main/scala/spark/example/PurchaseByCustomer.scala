package spark.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by suman.das on 7/7/18.
  */
object PurchaseByCustomer {

  /** A function that splits a line of input into (id, amountSpent) tuples. */
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val id = fields(0).toInt
    val amount = fields(2).toFloat
    // Create a tuple that is our result.
    (id, amount)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","PurchaseByCustomer");

    val input = sc.textFile("customer-orders.csv");

    val purchaseDS = input.map(x=> parseLine(x));

    val customerSpent = purchaseDS.reduceByKey((x,y)=> x + y);

    val results = customerSpent.collect();
    //Print the results
    results.foreach(println);
    println("########################")
    val sortedByAmount = customerSpent.map(x=> (x._2,x._1)).sortByKey();

    val sortedResults = sortedByAmount.collect();

    // Print the results, customer id sorted by the amount spent
    for (result <- sortedResults) {
      val amount = result._1
      val id = result._2
      println(s"$id: $amount")
    }
  }

}
