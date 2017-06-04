package com.spark.fjuted

import com.spark.fjuted.mongo._
import com.spark.fjuted.mqtt._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.mqtt.MQTTUtils

/**
  * Created by teddy on 5/9/17.
  */
object normal_distribution {
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

  // Main function
  def main(args: Array[String]): Unit = {
    /**
      * <arg parameters :>
      * <UserEmail> <SensorID> <StreamInterval> <MQTTBroker> <MongoDB> <FacebookToken> <DeviceToken> <AlertFunction>
      */

    println("---------- Normal_Distribution Method Main Function ----------")
    // If the number of args parameter is not correct, print out Require hint.
    if (args.length < 2) {
      System.err.println("Require args: <UserEmail> <SensorID> <StreamInterval> <MQTTBroker> <MongoDB> <FacebookToken> <DeviceToken> <AlertFunction>")
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

    println("UserEmail: "+UserEmail+"\n" + "SensorID: "+SensorID+"\n" + "StreamInterval: "+StreamInterval+"\n" + "MQTTBroker: "+MQTTBroker+"\n" + "MongoDB: "+MongoDB+"\n"  +
      "FacebookToken: "+FacebookToken+"\n" + "DeviceToken: "+DeviceToken+"\n" + "AlertFunction: "+AlertFunction+"\n"
      )

    val conf = new SparkConf().setMaster("local[*]").setAppName("Normal_Distribution - " + UserEmail)
    val ssc = new StreamingContext(conf, Seconds(StreamInterval.toInt))

    val lines = MQTTUtils.createStream(ssc, "tcp://" + MQTTBroker + ":1883", SensorID, StorageLevel.MEMORY_ONLY_SER)
    ssc.checkpoint("/home/teddy/Desktop/Master_Programs/IdeaProjects/checkpoint")

    // Mapping input string to (SensorID, string) pair, prepare for mapWithState
    val pair = lines.map(info => (SensorID, info))

    val learn = 20
    val UpdateFunction =
      (sensorId: String, row: Option[String], state: State[(Double, Double, Long, String, String)]) => {
        // State : 1.Mean, 2.Variance, 3.Counter, 4.Status, 5.Timestamp
        val source: String = row.getOrElse(null)
        val time = source.split(',').take(1)
        val value = source.split(',').take(2).drop(1).head.toDouble

        if (state.exists()) {
          val old_mean = state.get._1
          val old_variance = state.get._2
          val counter = state.get._3

          val new_mean = old_mean + (value - old_mean) / (counter + 1)
          val new_variance = old_variance + (value - old_mean) * (value - new_mean)

          val upper_bound = old_mean + 3 * Math.sqrt(old_variance / counter)
          val lower_bound = old_mean - 3 * Math.sqrt(old_variance / counter)
          println("Data : " + value)
          println("UpB  : " + upper_bound)
          println("LowB : " + lower_bound)

          val Anomaly_Up: Boolean = value > upper_bound
          val Anomaly_Down: Boolean = value < lower_bound

          if(counter >= learn) {
            if (Anomaly_Up) {
              println(time.head.toString + " " + value.toString + "Up_Event")
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
              state.update(old_mean, old_variance, counter, "Warning: Up_Event", time.head)
            }//if Anomaly_Up
            else if (Anomaly_Down) {
              println(time.head.toString + " " + value.toString + "Down_Event")
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
              state.update(old_mean, old_variance, counter, "Warning: Down_Event", time.head)
            }//else if Anomaly_Down
            else {
              state.update(new_mean, new_variance, counter + 1, "Normal", time.head)
            }//else
          }//if counter > learning number
          else {
            state.update(new_mean, new_variance, counter + 1, "Learning", time.head)
          }//else
        } //if_state.exist
        else {
          state.update(value, 0.0, 1, null, null)
        } //Initial state

        //if (state.get._3 > 5) {
          //state.remove()
        //} //test
      }
    //UpdateFunction
    val result = pair.mapWithState(StateSpec.function(UpdateFunction)).stateSnapshots()
    result.print

    ssc.start()
    ssc.awaitTermination()
  }//main
}//normal_distribution
