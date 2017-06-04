package com.spark.fjuted

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject

/**
  * Created by rong on 5/8/17.
  */
object mongo {
  def mongo_insert(useremail:String, sensorid:String, mongo:String, time:String, value:String, status:String): Unit ={
    val mongoClient = MongoClient(mongo, 27017)
    val db = mongoClient("Anomalies")
    val coll = db(useremail)
    db.collectionNames

    val obj = MongoDBObject(
      sensorid -> MongoDBObject(
        "time" -> time,
        "value" -> value,
        "status" -> status
      )
    )
    coll.insert(obj)
  }//mongo_insert
}//mongo
