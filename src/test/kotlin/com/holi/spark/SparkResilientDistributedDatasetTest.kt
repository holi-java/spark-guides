package com.holi.spark

import com.holdenkarau.spark.testing.SharedJavaSparkContext
import com.holi.io.plusAssign
import com.holi.spark.hamkrest.hasSize
import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.natpryce.hamkrest.sameInstance
import org.apache.spark.api.java.JavaRDD
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

@Suppress("MemberVisibilityCanPrivate")
class SparkResilientDistributedDatasetTest : SharedJavaSparkContext() {

    @get:Rule val folder: TemporaryFolder = TemporaryFolder()

    val readme by lazy {
        folder.newFile("test")!!.apply {
            writer().use { it += "#Spark\nSpark Resilient Distributed Dataset test" }
        }
    }

    val RDD: JavaRDD<String> get() = jsc().textFile(readme.path)

    @Test
    fun `create resilient distributed dataset from file`() {
        assert.that(RDD, hasSize(2L))
    }

    @Test
    fun `fetch the first 'item' from resilient distributed dataset`() {
        assert.that(RDD.first(), equalTo("#Spark"))
    }

    @Test
    fun `filter items from resilient distributed dataset`() {
        val it = RDD.filter { it.contains("test") }

        assert.that(it, hasSize(1L))
        assert.that(it.first(), equalTo("Spark Resilient Distributed Dataset test"))
    }

    @Test
    fun `caching repeatedly`() {
        val it = RDD.cache()

        assert.that(it, !sameInstance(RDD))
        assert.that(it.cache(), !sameInstance(it))
    }

    @Test
    fun `create resilient distributed dataset from parallelized list`() {
        val it = jsc().parallelize(listOf("spark", "resilient distributed dataset", "test"))

        assert.that(it, hasSize(3L))
    }
}

