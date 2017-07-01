@file:Suppress("HasPlatformType")

package com.holi.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

val <T> JavaRDDLike<T, *>.size get() = count()

operator fun <T, R> Tuple2<T, R>.component1() = _1()
operator fun <T, R> Tuple2<T, R>.component2() = _2()


fun JavaSparkContext.disableLog() = setLogLevel("OFF")