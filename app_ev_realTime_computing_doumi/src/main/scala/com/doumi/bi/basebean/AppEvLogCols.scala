package com.doumi.bi.basebean

import com.alibaba.fastjson.{JSON, JSONObject}
import com.doumi.bi.utils.{ConstantUtil, DateUtil, URLParamUtil}

object AppEvLogCols {

  private var jsonObject = new JSONObject()

  //完全json化
  private var requestBody = new JSONObject()

  //只一级json化
  private var requestBody1Level = new JSONObject()

  def apply(message: String): AppEvLogCols.type = {

    //解析Json串为一个JSONObject对象
    jsonObject = JSON.parseObject(message)

    //获取"request_body"字段
    val request_body = jsonObject.getString("request_body")

    //针对request_body进行json化
    requestBody = URLParamUtil.requestBodyParam2Json(request_body)

    //针对request_body进行一节json化
    requestBody1Level = URLParamUtil.RequestBody2Json1Level(request_body)

    init(jsonObject)

    //返回当前对象
    this
  }

  /**
    * 初始化json对象
    *
    * @param jsonObject 是一整条日志的JSONObject对象
    */
  def init(jsonObject: JSONObject): Unit = {
    //uuid
    requestUriUuid = getRequestURLValue(null, "device_token")
    //user_id
    requestUriDmuser = getRequestURLValue(null, "dmuser")
    //record_time（需要转成转成unix时间戳）
    val time = getRequestURLValue(null, "time")
    //转成unix时间戳
    timestamp = DateUtil.getTimeStampStandardS(time)
    //dmch_page
    requestUriDmchPage = getRequestURLValue("dmch", "dmch")
    //dmalog_post_id
    requestUriDmalogPostId = getRequestURLValue("dmalog", "post_id")
    //dmalog_atype
    requestUriDmalogAtype = getRequestURLValue("dmalog", "atype")
  }

  /**
    *
    * @param location
    * @param key
    * @return
    */
  def getRequestURLValue(location: String, key: String): String = {

    if (null == location) {
      requestBody.getString(key) match {
        case null => return ConstantUtil.EMPTY_PARARM_FROM_URL //return必须加
        case _ => return requestBody.getString(key) //return必须加
      }
    }

    try {
      val jsonObject = requestBody.getJSONObject(location)

      if (null == jsonObject) {
        return ConstantUtil.EMPTY_PARARM_FROM_URL
      }

      jsonObject.getString(key) match {
        case null => return ConstantUtil.EMPTY_PARARM_FROM_URL
        case _ => return jsonObject.getString(key)
      }
    }
    //防止runtime异常,因为有可能有些字段有多个或者是1个
    //例如："dmch":"-" 也可能为"dmch":{"dmch":"/jianzhi/sz/-/0/0_0_0_0/list","pn":"10gc","city":"sz"}
    catch {
      case e: java.lang.ClassCastException =>
        requestBody.getString(key) match {
          case null => return ConstantUtil.EMPTY_PARARM_FROM_URL
          case _ => return requestBody.getString(key)
        }
    }
  }

  var requestUriUuid: String = _
  var requestUriDmuser: String = _
  var timestamp: String = _
  var requestUriDmchPage: String = _
  var requestUriDmalogPostId: String = _
  var requestUriDmalogAtype: String = _

  override def toString: String = {
    requestUriUuid + ConstantUtil.APP_EV_FIELD_DELIMETER +
      requestUriDmuser + ConstantUtil.APP_EV_FIELD_DELIMETER +
      timestamp + ConstantUtil.APP_EV_FIELD_DELIMETER +
      requestUriDmchPage + ConstantUtil.APP_EV_FIELD_DELIMETER +
      requestUriDmalogPostId + ConstantUtil.APP_EV_FIELD_DELIMETER +
      requestUriDmalogAtype + ConstantUtil.APP_EV_FIELD_DELIMETER
  }
}
