import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object CachePersistReports {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Cache and Persist")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    // Raw transaction dataset
    val transactions = sc.parallelize(Seq(
      ("T001", "P101", 100.0, "VALID"),
      ("T002", "P102", -50.0, "INVALID"),
      ("T003", "P103", 250.0, "VALID"),
      ("T004", "P101", 150.0, "VALID"),
      ("T005", "P104", 300.0, "VALID"),
      ("T006", "P102", -20.0, "INVALID")
    ))

    // Clean the transaction dataset
    val cleanedTransactions = transactions
      .filter(transaction => transaction._3 > 0)
      .map(transaction => (
        transaction._1,
        transaction._2,
        transaction._3
      ))
      .cache()

    println("=== Report 1: Transaction Count ===")
    println(s"Valid Transactions: ${cleanedTransactions.count()}")

    println("\n=== Report 2: Total Transaction Amount ===")
    println(s"Total Amount: ${cleanedTransactions.map(_._3).sum}")

    println("\n=== Report 3: Average Transaction Amount ===")
    println(s"Average Amount: ${cleanedTransactions.map(_._3).mean}")

    println("\n=== Storage Level ===")
    println(cleanedTransactions.getStorageLevel)

    // Remove the cached RDD from memory
    cleanedTransactions.unpersist()

    println("\n=== Persist Example ===")

    val persistedTransactions = transactions
      .filter(transaction => transaction._3 > 0)
      .map(transaction => (
        transaction._1,
        transaction._2,
        transaction._3
      ))
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Persisted Transaction Count: ${persistedTransactions.count()}")
    println(s"Persisted Storage Level: ${persistedTransactions.getStorageLevel}")

    persistedTransactions.unpersist()

    sc.stop()
  }
}
