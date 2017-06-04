package com.spark.fjuted

import com.spark.fjuted.mqtt._
import com.spark.fjuted.mongo._

import com.redis._
import java.text.SimpleDateFormat
import java.util.Calendar
/**
  * Created by rong on 5/10/17.
  */
object basic_statistic {
  def main(args: Array[String]): Unit ={
    /**
      * <arg parameters :>
      * <UserEmail> <SensorID> <Redis> <MQTTBroker> <BeginDate> <EndDate> <Function>
      */

    println("---------- Basic Statistic Method Main Function ----------")
    // If the number of args parameter is not correct, print out Require hint.
    if (args.length < 2) {
      System.err.println("Require args: <UserEmail> <SensorID> <Redis> <MQTTBroker> <MongoDB> <BeginDate> <EndDate> <Function>")
      System.exit(1)
    }

    // Get args parameters from caller
    val UserEmail   = args(0).toString
    val SensorID    = args(1).toString
    val Redis       = args(2).toString
    val MQTTBroker  = args(3).toString
    val MongoDB     = args(4).toString
    val Begin       = args(5).toString
    val End         = args(6).toString
    val Method      = args(7).toString

    println("UserEmail: "+UserEmail+"\n" + "SensorID: "+SensorID+"\n" + "MQTTBroker: "+MQTTBroker+"\n" + "Redis: "+Redis+"\n" +
      "MongoDB: "+MongoDB+"\n" + "BeginDate: "+Begin+"\n" + "EndDate: "+End+"\n" + "Function: "+Method+"\n"
    )

    var result: List[Int] = Nil
    val redis = new RedisClient(Redis, 6379)


    val format = new SimpleDateFormat("yyyy-MM-dd")
    val startDate = format.parse(Begin)
    val endDate = format.parse(End)

    val start = Calendar.getInstance
    start.setTime(startDate)

    val end = Calendar.getInstance
    end.setTime(endDate)

    var seed = start.getTime()
    var date = format.format(seed)

    //start.add(Calendar.DATE, -1)
    while ( {!start.after(end)}) {
      //println(date)

      val temp = redis.hvals(SensorID + "/" + date)
      val list = temp.get
      val arr = list.map(info => info.toInt)
      result = List.concat(result, arr)

      println(result)

      start.add(Calendar.DATE, 1)
      seed = start.getTime()
      date = format.format(seed)
    }//while

    if(Method.contains("Average")){
      val num = result.length
      val sum = result.sum
      val average = sum / num
      println(average)
      mqtt_push(UserEmail, SensorID, MQTTBroker, "Average", Begin+"~"+End+"/"+average.toString)
      mongo_insert(UserEmail, SensorID, MongoDB, "Average", Begin, End, average.toString)
    }//if_Average
    if(Method.contains("Maximum")) {
      val maximum = result.max
      println(maximum)
      mqtt_push(UserEmail, SensorID, MQTTBroker, "Maximum", Begin+"~"+End+"/"+maximum.toString)
      mongo_insert(UserEmail, SensorID, MongoDB, "Maximum", Begin, End, maximum.toString)
    }//if_Maximum
    if(Method.contains("Minimum")){
      val minimum = result.min
      println(minimum)
      mqtt_push(UserEmail, SensorID, MQTTBroker, "Minimum", Begin+"~"+End+"/"+minimum.toString)
      mongo_insert(UserEmail, SensorID, MongoDB, "Minimum", Begin, End, minimum.toString)
    }//if_Minimum
  }//main
}//basic_statistic
