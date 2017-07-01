@file:Suppress("MemberVisibilityCanPrivate", "HasPlatformType")

package com.holi.spark.variables

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.has
import com.natpryce.hamkrest.isA
import com.natpryce.hamkrest.throws
import org.apache.spark.SparkException
import org.junit.Test
import java.io.NotSerializableException
import java.io.Serializable
import java.util.*

class BroadcastVariableTest : SharedJavaSparkContext(), Serializable {

    @Test
    fun `read the read-only broadcast variable during operations`() {
        val times = jsc().broadcast(3)

        assert.that(jsc().parallelize(listOf(2)).map { it * times.value }.reduce(Integer::sum), equalTo(6))
    }

    @Test
    fun `can't broadcast un-serializable value in parallel operations`() {
        assert.that({ jsc().broadcast(Any()) }, throws(isA<NotSerializableException>()))
    }

}