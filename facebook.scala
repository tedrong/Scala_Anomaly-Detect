package com.spark.fjuted

import java.text.SimpleDateFormat
import java.util.Calendar

import facebook4j.FacebookFactory
import facebook4j.auth.AccessToken

/**
  * Created by rong on 5/9/17.
  */
object facebook {
  def facebook_post(fbtoken:String, message:String): Unit ={
    val facebook = new FacebookFactory().getInstance()
    facebook.setOAuthAppId("", "")
    val accesstoken = fbtoken
    val at = new AccessToken(accesstoken)
    facebook.setOAuthAccessToken(at)
    facebook.postStatusMessage(Get_timestamp + "\n" + message)
  }

  // TimeStamp Create Function
  def Get_timestamp:String = {
    val seed = Calendar.getInstance().getTime()
    val stamp_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = stamp_format.format(seed)
    return date
  }
}
