package com.farrellw.github

import java.text.SimpleDateFormat
import scala.io.Source

object Workouts {
  type WorkoutMap = Map[String, Word]
  type WorkoutList = List[Word]

  case class Word(word: String, valid: Boolean, resolve: String)
  case class AO(id: String, name: String)

  lazy val format = new SimpleDateFormat("yyyy-mm-dd")

  var workoutMap: Option[WorkoutMap] = None
  var workoutList: Option[List[Word]] = None
  var aoList: Option[Map[String, String]] = None

  def initAoList: Option[Map[String, String]] = {
    if(aoList.isEmpty) {
      val filesource = Source.fromFile("/Users/wgfarrell/code/workout-counter/src/main/resources/city-ao.txt")
      val lines = filesource.getLines().map(_.split(",")).toList.map(w => (w(0), w(1))).toMap
      filesource.close()

      aoList = Some(lines)
      aoList
    } else {
      aoList
    }
  }

  def initAnotherWorkoutDatastore: Option[WorkoutList] = {
    if (workoutList.isEmpty) {
      val filesource = Source.fromFile("/Users/wgfarrell/code/workout-counter/src/main/resources/all_single_line_excercises.txt")
      val lines = filesource.getLines().map(_.split(",")).toList.sortBy(_.length).map(w => Word(w(0).toLowerCase, valid = true, w(0).toLowerCase()))
      filesource.close()

      workoutList = Some(lines)
      workoutList
    } else {
      workoutList
    }
  }

  def getAOList: Option[Map[String, String]] = {
    if (aoList.isEmpty) {
      initAoList
    } else {
      aoList
    }
  }

  def initWorkoutDatastore: Option[WorkoutMap] = {
    if (workoutMap.isEmpty) {
      val filesource = Source.fromFile("/Users/wgfarrell/code/workout-counter/src/main/resources/invalid-words.txt")
      val lines = filesource.getLines().toList.map(_.split(","))
      filesource.close()

      val tempMap = scala.collection.mutable.Map[String, Word]()

      lines.foreach(vw => {
        val word = vw(0)
        val valid = false
        if (vw.length > 1) {
          val resolve = vw(1)
          tempMap += (word -> Word(word, valid, resolve))
        } else {
          tempMap += (word -> Word(word, valid, vw(0)))
        }
      })

      workoutMap = Some(tempMap.toMap)
      workoutMap
    } else {
      workoutMap
    }
  }

  def getWorkoutList: Option[WorkoutList] = {
    if (workoutList.isEmpty) {
      initAnotherWorkoutDatastore
    } else {
      workoutList
    }
  }

  def getWorkoutMap: Option[WorkoutMap] = {
    if (workoutMap.isEmpty) {
      initWorkoutDatastore
    } else {
      workoutMap
    }
  }
}