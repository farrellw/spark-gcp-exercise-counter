package com.farrellw.github

import com.farrellw.github.WorkoutCounter.{BbOutput, BbRecord, WordCount}
import Workouts.{getWorkoutList, getWorkoutMap}
import com.google.cloud.bigquery._
import com.google.cloud.functions.{BackgroundFunction, Context}
import com.google.events.cloud.pubsub.v1.Message

import java.sql.DriverManager
import java.util.UUID
import java.util.logging.Logger
import scala.collection.mutable.ListBuffer

class CloudFunction extends BackgroundFunction[Message] {
 val LOGGER: Logger = Logger.getLogger(this.getClass.getName)

 override def accept(message: Message, context: Context): Unit = {
   everything()
 }

 def everything(): Unit = {
   import collection.JavaConverters._

   LOGGER.info("Initiating Function")

   val conn = DriverManager.getConnection(sys.env("CONNECTION_URL"));

   val today = java.time.LocalDate.now
   val threePrevious = today.minusDays(4)
   val dateFormatted = threePrevious.toString

   LOGGER.info("Reading bb from " + dateFormatted)
   val stmt = conn.createStatement()
   val rs = stmt.executeQuery("SELECT * FROM beatdowns WHERE bd_date > '" + dateFormatted + "'")

   val lb = new ListBuffer[BbRecord]

   while (rs.next()) {
     val aoId = rs.getString(3)
     val bdDate = rs.getString(4)
     val qUserId = rs.getString(5)
     val timestamp = rs.getString(1)
     val coqUserId = rs.getString(6)
     val paxCount = None
     val backblast = rs.getString(8)
     val fngs = None
     val fngCount = None
     lb.append(BbRecord(aoId, bdDate, qUserId, Some(timestamp), None, paxCount, backblast, fngs, fngCount))
   }

   // Retrieve all records
   //https://stackoverflow.com/questions/73683270/named-parameter-type-for-array-of-struct-in-biqquery
   val result = myThing(lb.toList)

   rs.close()
   stmt.close()
   conn.close()

   val bigquery = BigQueryOptions.getDefaultInstance.getService

   val queryTwo =
     s"""
        |SELECT * FROM `f3-workout-counter.workout_with_tagged_words.city-tagged-exercises` EC
        |WHERE EC.bd_date > '${dateFormatted}'
        |""".stripMargin

   val queryConfig = QueryJobConfiguration.newBuilder(queryTwo).build

   // Create a job ID so that we can safely retry.// Create a job ID so that we can safely retry.

   val jobId = JobId.of(UUID.randomUUID.toString)
   var queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)

   // Wait for the query to complete.
   queryJob = queryJob.waitFor()
   val results = queryJob.getQueryResults()
   val myPrimaryKeys = new ListBuffer[String]
   results.iterateAll().forEach(row => {
     val aoId = row.get("ao_id").getStringValue
     val date = row.get("bd_date").getStringValue
     val qUserId = row.get("q_user_id").getStringValue
     myPrimaryKeys.append(aoId + date + qUserId)
   })

   


   val filteredToEnter = result.filter(x => !myPrimaryKeys.contains(x.ao_id + x.bd_date + x.q_user_id))

   if (filteredToEnter.nonEmpty) {
    LOGGER.info("Going to enter this many " + filteredToEnter.length.toString)
     val insertAllRequest = filteredToEnter.map(fte => {
       val nestedWordCount = fte.wordCount.map(wc => Map[String, Any]("name" -> wc.name, "count" -> wc.count).asJava).asJava

       Map[String, Any]("ao_id" -> fte.ao_id, "bd_date" -> fte.bd_date, "q_user_id" -> fte.q_user_id, "backblast" -> fte.backblast, "unknownWords" -> fte.unknownWords.getOrElse(""), "wordCount" -> nestedWordCount)
     }).foldLeft(InsertAllRequest.newBuilder("workout_with_tagged_words", "city-tagged-exercises"))((agg, next) => {
       agg.addRow(next.asJava)
     }).build()

     val response = bigquery.insertAll(insertAllRequest)

     if (response.hasErrors) {
       response.getInsertErrors.values().forEach(e => {
         LOGGER.info(e.toString)
       })
     } else {
       LOGGER.info("Success")
     }
   } else {
     LOGGER.info("No new BB to add")
   }
 }

 def countOccurrences(src: String, tgt: String): Int =
   src.sliding(tgt.length).count(window => window == tgt)

 def myThing(df: List[BbRecord]): List[BbOutput] = {
   df.map(bb => {
    try {
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
 
      Some(BbOutput(bb.ao_id, bb.bd_date, bb.q_user_id, bb.timestamp, bb.coq_user_id, bb.pax_count, bb.backblast, bb.fngs, bb.fng_count, Some(dateTimeObject.getYear), Some(dateTimeObject.getMonth), Some(dateTimeObject.getDay), Some(foundTuple._1.filter(_._2 > 0).map(x => (x._1.replaceAll("[^a-zA-Z0-9]+", "_"), x._2))).getOrElse(List()).toList.map(y => {
        WordCount(name = y._1, count = y._2)
      }), Some(leftOver)))
    } catch {
      case err: Throwable => {
        LOGGER.info("Encountered an error")
        LOGGER.info(err.toString)
        LOGGER.info(bb.toString)
        None
      }
      case _: Throwable => {
        LOGGER.info("Encountered an error")
        LOGGER.info(bb.toString)
        None
      }
    }
   }).flatten
 }
}