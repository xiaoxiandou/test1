package com.doumi.bi.utils

import java.text.{ParseException, SimpleDateFormat}

object DateUtil {

  /**
    * 以秒显示timestamp为了和mysql的兼容
    * @param time yyyy-MM-dd HH:mm:ss
    * @return
    */
  def getTimeStampStandardS(time:String):String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      val date = sdf.parse(time).getTime
      String.valueOf(date/1000)
    }catch {
      case e: ParseException =>
        e.printStackTrace()
        ConstantUtil.EMPTY_PARARM_FROM_URL
    }
  }

}
