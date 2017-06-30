package com.holi.spark.operations

import com.holdenkarau.spark.testing.JavaRDDComparisons.assertRDDEquals
import com.holdenkarau.spark.testing.SharedJavaSparkContext
import org.junit.Test
import java.util.Collections.synchronizedList

@Suppress("MemberVisibilityCanPrivate")
class SparkRDDTransformationsTest : SharedJavaSparkContext() {
    val lines = listOf("#Spark", "Spark Resilient Distributed Dataset test")
    val RDD by lazy { jsc().parallelize(synchronizedList(lines))!! }


    @Test
    fun `map resilient distributed dataset and then reduce the mapped collection`() {
        val expected = lines.map { it.split(" ").size }

        assertRDDEquals(RDD.map { it.split(" ").size }, jsc().parallelize(expected))
    }


    @Test
    fun `filter items from resilient distributed dataset`() {
        val it = RDD.filter { it.contains("test") }

        assertRDDEquals(it, jsc().parallelize(lines.drop(1)))
    }

}