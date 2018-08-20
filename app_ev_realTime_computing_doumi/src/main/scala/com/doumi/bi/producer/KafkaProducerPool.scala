package com.doumi.bi.producer

import java.util.Properties
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

//代理类
class KafkaProxy(brokers: String) {

  //连接kafka的参数列表
  private val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

  //kafka生产者
  private val kafkaConn = new KafkaProducer[String,String](props)

  def send(topic: String,value: String): Unit ={
    //生产数据
    kafkaConn.send(new ProducerRecord[String,String](topic,value))
  }

  def close(): Unit ={
    //关闭生产者的客户端连接
    kafkaConn.close()
  }
}

//生产对象的工厂
class KafkaProxyFactory(brokers: String) extends BasePooledObjectFactory[KafkaProxy]{

  override def create() = new KafkaProxy(brokers)

  //把实例包装成池中的一个对象
  override def wrap(t: KafkaProxy) = new DefaultPooledObject[KafkaProxy](t)
}

//连接池
object KafkaProducerPool {

  //连接池对象
  private var kafkaProxyPool: GenericObjectPool[KafkaProxy] = null

  def apply(brokers: String): GenericObjectPool[KafkaProxy] = {

    if (kafkaProxyPool == null){
      KafkaProducerPool.synchronized{
        if (kafkaProxyPool == null){
          //创建连接池
          kafkaProxyPool = new GenericObjectPool[KafkaProxy](new KafkaProxyFactory(brokers))
        }
      }
    }
    kafkaProxyPool
  }
}