name := "Normal_Distribution"

version := "1.0"

scalaVersion := "2.11.8"

// Spark Library
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.0"

// MQTT stream Library
libraryDependencies += "org.apache.bahir" %% "spark-streaming-mqtt" % "2.1.0"
libraryDependencies += "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "latest.integration"

// MongoDB Library
libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

// Facebook POST
libraryDependencies += "org.facebook4j" % "facebook4j-core" % "2.4.9"

// Gmail SMTP Library
libraryDependencies += "javax.mail" % "javax.mail-api" % "1.5.6"
libraryDependencies += "javax" % "javaee-api" % "7.0"

// FCM JSON
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1"