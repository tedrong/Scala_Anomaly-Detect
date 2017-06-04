package com.spark.fjuted

import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

/**
  * Created by rong on 5/8/17.
  */
object mqtt {
  def mqtt_push(sensorid:String, broker:String, message:String): Unit ={
    var client: MqttClient = null
    val persistence = new MemoryPersistence()

    client = new MqttClient("tcp://" + broker + ":1883", MqttClient.generateClientId(), persistence)
    client.connect()

    val msgtopic = client.getTopic("A" + sensorid)
    val Msg = new MqttMessage(message.getBytes("utf-8"))

    msgtopic.publish(Msg)
    println(s"Published data. topic: ${msgtopic.getName()}; Message: $Msg")

    client.disconnect()
  }
}
