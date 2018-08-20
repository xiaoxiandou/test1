package com.doumi.bi.consumer

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.doumi.bi.basebean.AppEvLogCols
import com.doumi.bi.producer.KafkaProducerPool
import com.doumi.bi.utils.{ConstantUtil, PropertiesUtil}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

object AppevlogAndBinlogConsumer {

  def main(args: Array[String]): Unit = {

    //加载配置文件
    val properties: Properties = PropertiesUtil.getProperties("configuration.properties")

    //获取checkpoint路径
    val checkPointPath: String = properties.getProperty("streaming.checkpoint.path")

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(checkPointPath,
      () => createStreamingContext(properties, checkPointPath))

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 创建一个新的StreamingContext，
    *
    * @param properties     配置文件
    * @param checkPointPath 检查点路径
    * @return StreamingContext
    */
  def createStreamingContext(properties: Properties, checkPointPath: String): StreamingContext = {

    //创建sparkConf
    val conf: SparkConf = new SparkConf().setAppName("Consume AppEvLog and binlog").setMaster("local[*]")
    //设置SparkStreaming优雅的停止
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    //获取时间间隔
    val streamingInterval: Long = properties.getProperty("streaming.interval").toLong
    //创建一个StreamingContext
    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(streamingInterval))
    //设置检查点路径
    streamingContext.checkpoint(checkPointPath)

    //获取kafka集群信息
    val brokerList: String = properties.getProperty("kafka.broker.list")
    //获取当前的消费者组
    val groupId: String = properties.getProperty("kafka.consumer.groupId")
    //appev日志的topic
    val kafkaTopic_appevlog: String = properties.getProperty("kafka.source.topic.appevlog")
    //binlog日志的topic
    val kafkaTopic_binlog: String = properties.getProperty("kafka.source.topic.binlog")
    val offsetReset: String = properties.getProperty("auto.offset.reset.config")

    //将获取的多个topic放进Set集合中
    val topicsSet: Set[String] = Set(kafkaTopic_appevlog, kafkaTopic_binlog)

    //封装kafka的参数信息
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].toString,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].toString,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset
    )

    //获取之前保存在zk的对应消费者组以及对应主题+分区的offset值
    val kafkaCluster = new KafkaCluster(kafkaParams)
    val topicAndPartitionSet: Set[TopicAndPartition] = kafkaCluster.getPartitions(topicsSet).right.get
    var consumerOffsetsLongMap = new HashMap[TopicAndPartition, Long]
    //之前无保存
    if (kafkaCluster.getConsumerOffsetMetadata(groupId, topicAndPartitionSet).isLeft) {
      for (topicAndPartition <- topicAndPartitionSet) {
        consumerOffsetsLongMap += ((topicAndPartition, 0L))
      }
    } else { //之前有保存
      val consumerOffsetsTemp = kafkaCluster.getConsumerOffsetMetadata(groupId, topicAndPartitionSet).right.get
      for (topicAndPartition <- topicAndPartitionSet) {
        val offset = consumerOffsetsTemp(topicAndPartition).offset
        consumerOffsetsLongMap += ((topicAndPartition, offset))
      }
    }

    //创建DirectDStream
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
      String]( //指定最后一个方法的返回值
      streamingContext,
      kafkaParams,
      consumerOffsetsLongMap,
      (mmd: MessageAndMetadata[String, String]) => mmd.message())

    //获取RDD中维护的offset数据
    var offsetRanges = Array[OffsetRange]()
    val kafkaOriDStream: DStream[String] = kafkaDStream.transform { rdd: RDD[String] =>
      //获取新读入数据的offsets，添加到上面的数组中
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //把kafkaDStream中的每个rdd原封不动的返回
      rdd
    }

    //过滤，先获取appEVLog
    val appEVLogDStream: DStream[String] = kafkaOriDStream.filter {
      message: String =>
        var flag: Boolean = true
        //过滤掉binlog
        if (!message.contains("request_body")) {
          flag = false
        }
        flag
    }
    //过滤，再获取binLog
    val binLogDStream: DStream[String] = kafkaOriDStream.filter {
      message: String =>
        var flag: Boolean = true
        //过滤掉binlog
        if (!message.contains("__action")) {
          flag = false
        }
        flag
    }

    //先解析AppEVLog
    val clickLogDStream: DStream[JSONObject] = appEVLogDStream.map { log: String =>
      val appEvLogCols = AppEvLogCols(log)
      appEvLogCols.toString
    }
      //      .filter { clickLog: String =>
      //      val fields = clickLog.split(ConstantUtil.APP_EV_FIELD_DELIMETER)
      //      val dmch_page = fields(3)
      //      val dmalog_post_id = fields(4)
      //      val dmalog_atype = fields(5)
      //
      //      if ("/jianzhi/index/".equals(dmch_page) && !dmalog_post_id.equals("-") && dmalog_atype.equals("click"))
      //        true
      //      else
      //        false
      //    }
      .map { filterClickLog: String =>
        val fields = filterClickLog.split(ConstantUtil.APP_EV_FIELD_DELIMETER)
        val uuid = fields(0)
        val user_id = fields(1)
        val record_time = fields(2)
        val dmalog_post_id = fields(4)

        val clickLogJson = new JSONObject()
        clickLogJson.put("uuid", uuid)
        clickLogJson.put("user_id", user_id)
        clickLogJson.put("post_id", dmalog_post_id)
        clickLogJson.put("click_or_apply", "click")
        clickLogJson.put("time", record_time)

        clickLogJson
    }

    //再解析binlog
    val applyLogDStream: DStream[JSONObject] = binLogDStream.filter { binlog: String =>
      if (binlog.toUpperCase.contains("INSERT"))
        true
      else
        false
    }.map { filterBinLog: String =>
      val binLogJson = JSON.parseObject(filterBinLog)
      val user_id = binLogJson.getString("user_id")
      val post_id = binLogJson.getString("post_id")
      val time = binLogJson.getString("create_at")

      val applyLogJson = new JSONObject()
      applyLogJson.put("uuid", ConstantUtil.EMPTY_PARARM_FROM_URL)
      applyLogJson.put("user_id", user_id)
      applyLogJson.put("post_id", post_id)
      applyLogJson.put("click_or_apply", "apply")
      applyLogJson.put("time", time)

      applyLogJson
    }

    //合并解析后的两种日志
    val resultDStream: DStream[JSONObject] = clickLogDStream.union(applyLogDStream)

    //写出
    resultDStream.foreachRDD { resultRDD: RDD[JSONObject] =>
      resultRDD.foreachPartition { result: Iterator[JSONObject] =>
        //写到另外一个topic中
        val targetTopic: String = properties.getProperty("kafka.target.topic")

        //获取连接池
        val pool = KafkaProducerPool(brokerList)
        //获取代理对象
        val kafkaProxy = pool.borrowObject()
        //写数据
        for (item <- result)
          kafkaProxy.send(targetTopic, item.toString)

        //归还对象到连接池中
        pool.returnObject(kafkaProxy)
      }

      //消费完之后修改每个分区的offset
      for (offsetRange <- offsetRanges) {
        val topicAndPartition = TopicAndPartition(offsetRange.topic, offsetRange.partition)
        var topicAndPartitionToLongMap = new HashMap[TopicAndPartition, Long]
        topicAndPartitionToLongMap += ((topicAndPartition, offsetRange.untilOffset))
        kafkaCluster.setConsumerOffsets(groupId, topicAndPartitionToLongMap)
      }
    }

    //打印到控制台看看
    resultDStream.print()

    streamingContext
  }
}
