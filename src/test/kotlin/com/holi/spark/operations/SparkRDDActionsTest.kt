package com.holi.spark.operations

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.natpryce.hamkrest.*
import com.natpryce.hamkrest.assertion.assert
import org.junit.Test
import java.util.*

@Suppress("MemberVisibilityCanPrivate")
class SparkRDDActionsTest : SharedJavaSparkContext() {
    val lines = listOf("#Spark", "Spark Resilient Distributed Dataset test")
    val RDD by lazy { jsc().parallelize(Collections.synchronizedList(lines))!! }


    @Test
    fun `reducing elements in resilient distributed dataset to the list in parallel`() {
        val it = RDD.reduce(String::plus)

        assert.that(it, startsWith("#Spark") or endsWith("#Spark"))
        assert.that(it, contains(Regex("#Spark")) and contains(Regex("test")))
    }


}