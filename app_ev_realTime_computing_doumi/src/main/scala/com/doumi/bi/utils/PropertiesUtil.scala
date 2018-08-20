package com.doumi.bi.utils

import java.io._
import java.util.Properties


/**
  * 加载配置文件
  */
class PropertiesUtil private() {

}

object PropertiesUtil {

  def getProperties(path: String): Properties = {

    var props: Properties = null
    var in: InputStream = null

    try {
//      in = new BufferedInputStream(new FileInputStream(new File(path)))
      in = PropertiesUtil.getClass.getClassLoader.getResourceAsStream(path)
      props = new Properties()
      props.load(in)
      props
    } catch {
      case e: IOException => throw e
    } finally {
      if (in != null)
        in.close()
    }
  }
}
