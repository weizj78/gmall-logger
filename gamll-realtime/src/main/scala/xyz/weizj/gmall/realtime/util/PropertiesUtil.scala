package xyz.weizj.gmall.realtime.util

import java.util.Properties

object PropertiesUtil {
    def main(args: Array[String]): Unit = {
        val properties: Properties = load("config.properties")
        println(properties.getProperty("kafka.broker.list"))
    }
    def load(str: String): Properties = {
        val prop = new Properties()
        prop.load(this.getClass.getClassLoader.getResourceAsStream(str))
        prop
    }


}
