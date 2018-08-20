package com.doumi.bi.utils

import com.alibaba.fastjson.JSONObject

object URLParamUtil {

  /**
    * 针对request_body进行json化
    * @param requestBody "request_body"字段
    * @return
    */
  def requestBodyParam2Json(requestBody: String): JSONObject = {

    val jsonObject = new JSONObject()
    val _params = requestBody.split("&")
    for (temp: String <- _params) {
      val _temp = temp.split("=")
      if (_temp.length == 2) {
        jsonObject.put(_temp(0), _temp(1))
      } else if (_temp.length < 2) {
        if(_temp.length!=0) {
          jsonObject.put(_temp(0), ConstantUtil.EMPTY_PARARM_FROM_URL)
        }
      } else {
        jsonObject.put(_temp(0), new JSONObject())
        val tmp_temp = temp.split("@")
        for (_tmp_temp: String <- tmp_temp) {
          val index = _tmp_temp.indexOf("=")
          var key: String = null
          var value: String = null

          // @1@
          if (-1 == index) {
            key = _tmp_temp
            value = ConstantUtil.EMPTY_PARARM_FROM_URL
          } else{
          // @1=@
            key = _tmp_temp.substring(0, index)
            value = _tmp_temp.substring(index + 1)
          }

          if ("".equals(value)) {
            value = ConstantUtil.EMPTY_PARARM_FROM_URL
          }
          jsonObject.getJSONObject(_temp(0)).put(key, value)
        }
      }
    }
    jsonObject
  }

  /**
    * 针对request_body进行一级json化
    * @param url "request_body"字段
    * @return
    */
  def RequestBody2Json1Level(url: String): JSONObject = {
    val jsonObject = new JSONObject()
    val _params = url.split("&")
    for (temp: String <- _params){
      val location = temp.indexOf("=")
      var key: String = null
      var value: String = null
      // &1&
      if (-1 == location) {
        key = temp
        value = ConstantUtil.EMPTY_PARARM_FROM_URL
      }
      else{
      // &1=&
        key = temp.substring(0, location)
        value = temp.substring(location + 1)
      }
      if ("".equals(value)) {
        value = ConstantUtil.EMPTY_PARARM_FROM_URL
      }
      jsonObject.put(key, value)
    }
    jsonObject
  }

}
