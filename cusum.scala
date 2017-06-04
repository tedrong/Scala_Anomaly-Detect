package com.spark.fjuted

import com.spark.fjuted.mongo.mongo_insert
import com.spark.fjuted.mqtt.mqtt_push
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.mqtt.MQTTUtils

/**
  * Created by teddy on 5/9/17.
  */
object cusum {
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
      * <UserEmail> <SensorID> <StreamInterval> <MQTTBroker> <MongoDB> <FacebookToken> <DeviceToken> <AlertFunction>
      */

    println("---------- CUSUM Method Main Function ----------")
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

    val conf = new SparkConf().setMaster("local[*]").setAppName("CUSUM - " + UserEmail)
    val ssc = new StreamingContext(conf, Seconds(StreamInterval.toInt))

    val lines = MQTTUtils.createStream(ssc, "tcp://" + MQTTBroker + ":1883", SensorID, StorageLevel.MEMORY_ONLY_SER)
    ssc.checkpoint("/home/teddy/Desktop/Master_Programs/IdeaProjects/checkpoint")

    // Mapping input string to (SensorID, string) pair, prepare for mapWithState
    val pair = lines.map(info => (SensorID, info))

    val learn = 20
    val UpdateFunction =
      (sensorId: String, row: Option[String],
       state: State[(Double, Double, Double, Double, Long, String, String)]) => {
        // State : 1.Mean, 2.Variance, 3.S_high, 4.S_low, 5.Counter, 6.Status, 7.Timestamp
        val source:String = row.getOrElse(null)
        val time = source.split(',').take(1)
        val value = source.split(',').take(2).drop(1).head.toDouble

        if(state.exists()) {
          val old_mean = state.get._1
          val old_variance = state.get._2
          val counter = state.get._5

          val new_mean = old_mean + (value - old_mean) / (counter + 1)
          val new_variance = old_variance + (value - old_mean) * (value - new_mean)

          val old_Sh = state.get._3
          val old_Sl = state.get._4

          val std = Math.sqrt(new_variance / counter)
          val k = std * 0.5
          val new_Sh = Math.max(0.0, old_Sh + value - old_mean - k)
          val new_Sl = Math.min(0.0, old_Sl + value - old_mean + k)


          //println(value)
          //state.update(new_mean, new_variance, new_Sh, new_Sl, counter+1, "normal", time.head)


          val up_event_flag: Boolean = Math.abs(new_Sh) >= Math.abs(4 * std)
          val down_event_flag: Boolean = Math.abs(new_Sl) >= Math.abs(4 * std)

          if (counter >= learn) {
            if (up_event_flag) {
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
              state.update(old_mean, old_variance, 0.0, 0.0, counter, "Warning_UpEvent", time.head)
            }
            else if (down_event_flag) {
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
              state.update(old_mean, old_variance, 0.0, 0.0, counter, "Warning_DownEvent", time.head)
            }
            else {
              state.update(new_mean, new_variance, new_Sh, new_Sl, counter + 1, "Normal", time.head)
            }

          } //if counter > learning number
          else {
            state.update(new_mean, new_variance, 0.0, 0.0, counter + 1, "Learning", time.head)
          }
        }
        else{
          state.update(value, 0.0, value, 0.0, 1, null, null)
        }//Initial state

        //if(state.get._5 > 100){
          //state.remove()
        //}//test
      }//UpdateFunction
    val result = pair.mapWithState(StateSpec.function(UpdateFunction)).stateSnapshots()
    result.print

    ssc.start()
    ssc.awaitTermination()
  }//main
}
