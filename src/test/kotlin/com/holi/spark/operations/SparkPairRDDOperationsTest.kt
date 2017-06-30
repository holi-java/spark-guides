package com.holi.spark.operations

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.junit.Test
import scala.Tuple2

@Suppress("MemberVisibilityCanPrivate")
class SparkPairRDDOperationsTest : SharedJavaSparkContext() {
    val items = listOf("foo", "bar", "foo")
    val RDD by lazy { jsc().parallelize(items)!! }
    
    @Test
    fun `transforms a RDD to a PairRDD`() {
        val pair = RDD.mapToPair { Tuple2(it, 1) }

        val expected: Map<String, Int> = items.asSequence().groupingBy { it }.eachCount()

        assert.that(pair.reduceByKey(Int::plus).collectAsMap(), equalTo(expected));
    }
}