package com.farrellw.github

import com.farrellw.github.Workouts.{getWorkoutList, getWorkoutMap}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
 * Breaks a backblast into individual words and returns the dataframe with the accompanying word count.
 */
object WorkoutCounter extends App {
  case class BbRecord(ao_id: String, bd_date: String, q_user_id: String, timestamp: Option[String], coq_user_id: Option[String], pax_count: Option[Int], backblast: String, fngs: Option[String], fng_count: Option[Int])
  case class WordCount(name: String, count: Int)
  case class BbOutput(ao_id: String, bd_date: String, q_user_id: String, timestamp: Option[String], coq_user_id: Option[String], pax_count: Option[Int], backblast: String, fngs: Option[String], fng_count: Option[Int], year: Option[Int], month: Option[Int], day: Option[Int], wordCount: List[WordCount], unknownWords: Option[String])

  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "WorkoutCounterApp"

  val schema: StructType = new StructType()
    .add("ao_id", StringType, nullable = false)
    .add("bd_date", StringType, nullable = false)
    .add("q_user_id", StringType, nullable = false)
    .add("timestamp", StringType, nullable = true)
    .add("coq_user_id", StringType, nullable = true)
    .add("pax_count", IntegerType, nullable = true)
    .add("backblast", StringType, nullable = true)
    .add("fngs", StringType, nullable = true)
    .add("fng_count", IntegerType, nullable = true)

  logger.info(s"Booting up ${jobName}")

  val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()

  import spark.implicits._

  val df = spark.read.schema(schema).json("/Users/wgfarrell/code/workout-counter/src/main/resources/sept_13_single_line_beatdown.json").as[BbRecord]

  def countOccurrences(src: String, tgt: String): Int =
    src.sliding(tgt.length).count(window => window == tgt)

  val slowDf = df.map(bb => {
    val workoutList = getWorkoutList.get

    val wordCount = Map[String, Int]()

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val dateTimeObject = format.parse(bb.bd_date)


    val foundTuple = workoutList.foldLeft((wordCount, bb.backblast.toLowerCase()))((agg, next) => {
      val remainingBb = agg._2
      val lastLetter = next.word.takeRight(1)
      if (lastLetter == "s") {
        val amountFound = countOccurrences(remainingBb, next.word)
        val amountFoundPlural = countOccurrences(remainingBb, next.word.dropRight(1))
        val newBb = remainingBb.replaceAll(next.word, " ").replaceAll(next.word.dropRight(1), " ")

        (agg._1 + (next.resolve -> (amountFound + amountFoundPlural)), newBb)
      } else {
        val amountFound = countOccurrences(remainingBb, next.word)
        val amountFoundPlural = countOccurrences(remainingBb, next.word + "s")
        val newBb = remainingBb.replaceAll(next.word, " ").replaceAll(next.word + "s", " ")

        (agg._1 + (next.resolve -> (amountFound + amountFoundPlural)), newBb)
      }
    })

    val leftOver = getWorkoutMap.get.toList.foldLeft(foundTuple._2.replaceAll("\n", " "))((agg, next) => {
      agg.replaceAll(next._1, " ")
    })

    BbOutput(bb.ao_id, bb.bd_date, bb.q_user_id, bb.timestamp, bb.coq_user_id, bb.pax_count, bb.backblast, bb.fngs, bb.fng_count, Some(dateTimeObject.getYear), Some(dateTimeObject.getMonth), Some(dateTimeObject.getDay), Some(foundTuple._1.filter(_._2 > 0).map(x => (x._1.replaceAll("[^a-zA-Z0-9]+", "_"), x._2))).getOrElse(List()).toList.map(y => {
      WordCount(name = y._1, count = y._2)}), Some(leftOver))
  })

  slowDf.write.format("json").save("secondEnrichedBd")
//  slowDf.flatMap(_.wordCount.getOrElse(Map()).keys).distinct().coalesce(1).write.format("csv").save("allExcercises")

  spark.close()

  def replaceSpecialCharacters(sentence: String) = {
    sentence.replaceAll("\n", " ").replaceAll("[^a-zA-Z0-9]+", " ")
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase)
  }
}