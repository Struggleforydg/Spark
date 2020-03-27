package com.my.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_wordCount {

  def main(args: Array[String]): Unit = {

    //创建第一个spark应用程序：WordCount

    //1创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]");

    //2创建spark环境连接对象(创建spark上下文连接对象的类称之为Driver类）
    val sc = new SparkContext(conf);

    //3读取文件：1.从classpath中获取 2.从项目的环境中获取
    //从classpath中获取：Thread.currentThread().getContextClassLoader().getResourceAsStream()
    //从项目的环境中获取
    val lineRDD: RDD[String] = sc.textFile("data/input")

    //4将每一行字符串拆分成一个一个的单词（扁平化)
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    //5将单次转换结构，用于后续的统计
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    //6将数据进行分组聚合
    //RDD类中不存在reduceByKey方法，但是如果想要使用，需要使用隐式转换
    val wordToCountRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey((x, y) => {
      x + y
    })

    //7将数据结果收集到Driver端进行展示
    val result: Array[(String, Int)] = wordToCountRDD.collect()

    //8循环遍历展示结果
    result.foreach(println);
  }
}
