package com.holi.spark.hamkrest

import com.holi.spark.extensions.size
import com.natpryce.hamkrest.Matcher
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import org.apache.spark.api.java.JavaRDDLike

fun <T> hasSize(expected: Long): Matcher<JavaRDDLike<T, *>> {
    return has(JavaRDDLike<T, *>::size, equalTo(expected))
}