package com.holi.spark.hamkrest

import com.holi.spark.size
import com.natpryce.hamkrest.Matcher
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import org.apache.spark.api.java.JavaRDDLike
import java.util.concurrent.atomic.AtomicInteger

fun <T> hasSize(expected: Number): Matcher<JavaRDDLike<T, *>> {
    return has(JavaRDDLike<T, *>::size, equalTo(expected.toLong()))
}

fun asInt(expected: Int): Matcher<AtomicInteger> {
    return has(AtomicInteger::get, equalTo(expected))
}