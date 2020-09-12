package xyz.weizj.gmall.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.Date

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import constans.GmallConstans
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
import xyz.weizj.gmall.realtime.StartUpLog
import xyz.weizj.gmall.realtime.util.{MyKafkaUtils, RedisUtil}

import scala.util.parsing.json.JSONObject

object DAUApp {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("REALTIME")

        val ssc = new StreamingContext(conf, Seconds(5))

        val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(GmallConstans.KAFKA_TOPIC_STARTUP, ssc)

        val startUpLogDstream: DStream[StartUpLog] = inputStream.map {
            record => {
                val jsonString: String = record.value()
                val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
                val dateHour: String = new SimpleDateFormat("yy-MM-dd HH")
                        .format(new Date(startUpLog.ts))
                val dateHourArr: Array[String] = dateHour.split(" ")
                startUpLog.logDate = dateHourArr(0)
                startUpLog.logHour = dateHourArr(1)
                startUpLog
            }
        }

        val filteredStream: DStream[StartUpLog] = startUpLogDstream.transform(
            rdd => {
                val jedis = RedisUtil.getJedisClient
                val dateString: String = DateTimeFormatter.ofPattern("yy-MM-dd")
                        .format(LocalDate.now())
                val dauMidSet: util.Set[String] = jedis.smembers("dau:" + dateString)
                val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet);
                val filterRdd: RDD[StartUpLog] = rdd.filter {
                    startuplog => {
                        val dauMidSet: util.Set[String] = dauMidBC.value
                        !dauMidSet.contains(startuplog.mid)
                    }
                }
                filterRdd
            }
        )

        val finalFilterStream: DStream[StartUpLog] = filteredStream.map(startuplog => (startuplog.mid, startuplog))
                .groupByKey()
                .flatMap(_._2.toList.sortWith((startup1, startup2) => startup1.ts < startup2.ts).take(1))

        finalFilterStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iterator => {
                        val jedis = RedisUtil.getJedisClient
                        iterator.foreach(
                            startuplog => {
                                //保存redis //所有今天访问过的mid清单
                                //redis 1.type:set 2key:[dau:-10-]
                                val dauKey: String = "dau:" + startuplog.logDate
                                jedis.sadd(dauKey, startuplog.mid)
                            }
                        )
                        jedis.close()
                    }
                )
            })




        ssc.start()
        ssc.awaitTermination()
        ssc.stop(false)

    }

}
