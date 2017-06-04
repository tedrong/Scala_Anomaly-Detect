package com.spark.fjuted

import com.spark.fjuted.mqtt._
import com.spark.fjuted.mongo._
import com.spark.fjuted.facebook._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.mqtt.MQTTUtils

/**
  * Created by rong on May/7/2017.
  */
object threshold {
  /** Set the console information level.
    * Level.ALL     (All levels including custom levels)
    * Level.DEBUG   (Designates fine-grained informational events that are most useful to debug an application)
    * Level.ERROR   (Designates error events that might still allow the application to continue running)
    * Level.FATAL   (Designates very severe error events that will presumably lead the application to abort)
    * Level.INFO    (Designates informational messages that highlight the progress of the application at coarse-grained level)
    * Level.OFF     (The highest possible rank and is intended to trun off logging)
    * Level.TRACE   (Designates finer-grained informational events than the DEBUG)
    * Level.WARN    (Designates potentially harmful situations)
    */
  Logger.getLogger("org.apache.spark").setLevel(Level.FATAL)

  def main(args: Array[String]): Unit ={
    /**
      * <arg parameters :>
      * <UserEmail> <SensorID> <StreamInterval> <MQTTBroker> <MongoDB> <FacebookToken> <DeviceToken> <AlertFunction> <UpperBound> <LowerBound>
      */

    println("---------- Threshold Method Main Function ----------")
    // If the number of args parameter is not correct, print out Require hint.
    if (args.length < 2) {
      System.err.println("Require args: <UserEmail> <SensorID> <StreamInterval> <MQTTBroker> <MongoDB> <FacebookToken> <DeviceToken> <AlertFunction> <UpperBound> <LowerBound>")
      System.exit(1)
    }

    // Get args parameters from caller
    val UserEmail        = args(0).toString
    val SensorID         = args(1).toString
    val StreamInterval   = args(2).toString
    val MQTTBroker       = args(3).toString
    val MongoDB          = args(4).toString
    val FacebookToken    = args(5).toString
    val DeviceToken      = args(6).toString
    val AlertFunction    = args(7).toString
    val UpperBound       = args(8).toString
    val LowerBound       = args(9).toString


    println("---------- Job received ----------")
    println("UserEmail: "+UserEmail+"\n" + "SensorID: "+SensorID+"\n" + "StreamInterval: "+StreamInterval+"\n" + "MQTTBroker: "+MQTTBroker+"\n" + "MongoDB: "+MongoDB+"\n"  +
      "FacebookToken: "+FacebookToken+"\n" + "DeviceToken: "+DeviceToken+"\n" + "AlertFunction: "+AlertFunction+"\n" +
      "UpperBound: "+UpperBound+"\n" + "LowerBound: "+LowerBound)

    val conf = new SparkConf().setMaster("local[2]").setAppName("Threshold - " + UserEmail)
    val ssc = new StreamingContext(conf, Seconds(StreamInterval.toInt))

    val lines = MQTTUtils.createStream(ssc, "tcp://" + MQTTBroker + ":1883", SensorID, StorageLevel.MEMORY_ONLY_SER)
    ssc.checkpoint("/home/teddy/Desktop/Master_Programs/IdeaProjects/checkpoint")

    // Mapping input string to (SensorID, string) pair, prepare for mapWithState
    val pair = lines.map(info => (SensorID, info))

    val UpdateFunction =
      (sensorId: String, row: Option[String], state: State[String]) => {
        // Trying to get String from mapWithState caller, if Option[] is empty return null
        val source:String = row.getOrElse(null)

        // Spliting out time and value from String
        val time = source.split(',').take(1)
        val value = source.split(',').take(2).drop(1).head.toDouble

        // Comparing with Bounds
        if(value < LowerBound.toDouble){
          println(time.head.toString + " " + value.toString + " " + "Down_Event")
          // MQTT: sensorid, broker, message
          mqtt_push(SensorID, MQTTBroker, SensorID + " " + time.head.toString + " " + value.toString + " " + "Down_Event")
          // Insert to MongoDB: useremail, sensorid, mongodb, time, value, status
          mongo_insert(UserEmail, SensorID, MongoDB, time.head.toString, value.toString, "Down_Event")

          if(AlertFunction.contains("Facebook")){
            println("Alert Function: Facebook")
            facebook.facebook_post(FacebookToken, time.head.toString + " " + value.toString + " " + "Down_Event")
          }
          if(AlertFunction.contains("Gmail")){
            println("Alert Function: Gmail")
            val mail = new gmail
            mail.setSender("fjutedrong@gmail.com", "virus9513")
            mail.setReceiver(UserEmail)
            mail.Send_Email("Abnormal", time.head.toString + " " + value.toString + " " + "Down_Event")
          }
          if(AlertFunction.contains("App")){
            println("Alert Function: App")
            val fcm = new fcm
            fcm.CallFCMPush(DeviceToken, time.head.toString + " " + value.toString + " " + "Down_Event")
          }
        }//if
        else if(value > UpperBound.toDouble){
          println(time.head.toString + " " + value.toString + " " + "Up_Event")
          // MQTT: sensorid, broker, message
          mqtt_push(SensorID, MQTTBroker, SensorID + " " + time.head.toString + " " + value.toString + " " + "Up_Event")
          // Insert to MongoDB: useremail, sensorid, mongodb, time, value, status
          mongo_insert(UserEmail, SensorID, MongoDB, time.head.toString, value.toString, "Up_Event")

          if(AlertFunction.contains("Facebook")){
            println("Alert Function: Facebook")
            facebook.facebook_post(FacebookToken, time.head.toString + " " + value.toString + " " + "Up_Event")
          }
          if(AlertFunction.contains("Gmail")){
            println("Alert Function: Gmail")
            val mail = new gmail
            mail.setSender("fjutedrong@gmail.com", "virus9513")
            mail.setReceiver(UserEmail)
            mail.Send_Email("Abnormal", time.head.toString + " " + value.toString + " " + "Up_Event")
          }
          if(AlertFunction.contains("App")){
            println("Alert Function: App")
            val fcm = new fcm
            fcm.CallFCMPush(DeviceToken, time.head.toString + " " + value.toString + " " + "Up_Event")
          }
        }//elseif
        else{
          println(time.head.toString + " " + value.toString + " " + "Normal")
        }//else
      }//UpdateFunction

    // Using mapWithState api to monitor stream data
    val result = pair.mapWithState(StateSpec.function(UpdateFunction)).stateSnapshots()
    // The Output Operation
    result.print

    // Start streaming
    ssc.start()
    ssc.awaitTermination()
  }//main
}//threshold
