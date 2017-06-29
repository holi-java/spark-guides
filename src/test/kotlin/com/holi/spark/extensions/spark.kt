package com.holi.spark.extensions

import org.apache.spark.api.java.JavaRDDLike

val <T> JavaRDDLike<T, *>.size get() = count()